package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.conductor.ConductorSystemDatabase;
import dev.dbos.transact.conductor.WorkflowFork;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.dao.ApplicationRenameDAO;
import dev.dbos.transact.database.dao.ApplicationVersionDAO;
import dev.dbos.transact.database.dao.ExternalStateDAO;
import dev.dbos.transact.database.dao.NotificationsDAO;
import dev.dbos.transact.database.dao.QueuesDAO;
import dev.dbos.transact.database.dao.ScheduleRecord;
import dev.dbos.transact.database.dao.SchedulesDAO;
import dev.dbos.transact.database.dao.StepsDAO;
import dev.dbos.transact.database.dao.StreamsDAO;
import dev.dbos.transact.database.dao.WorkflowDAO;
import dev.dbos.transact.database.signal.SignalKey;
import dev.dbos.transact.database.signal.SignalMap;
import dev.dbos.transact.database.signal.Subscription;
import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.internal.Validation;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.ApplicationRowCounts;
import dev.dbos.transact.workflow.Debouncer.DebounceIds;
import dev.dbos.transact.workflow.DeduplicationHolder;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.RewindOptions;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.SendMessage;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.VersionInfo;
import dev.dbos.transact.workflow.WorkflowDelay;
import dev.dbos.transact.workflow.WorkflowIdReusePolicy;
import dev.dbos.transact.workflow.WorkflowSchedule;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.jspecify.annotations.Nullable;
import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SystemDatabase implements AutoCloseable {

  public static final Object END_OF_STREAM = new Object();

  /**
   * Carries wake-ups between the processes sharing this system database, in both directions: a
   * notification seen on the wire becomes a local signal, and a local write becomes a notification
   * for everyone else. Absent where the database has no LISTEN/NOTIFY, in which case waiters are
   * still woken directly by a writer in their own process and otherwise re-poll.
   */
  public interface NotificationSource {
    void start();

    void close();

    /** Push a wake-up on {@code channel} to the other processes. Only after the write commits. */
    void push(String channel, String payload);

    /**
     * Whether a listener has been started for this source, which selects the re-check interval the
     * waits use.
     *
     * <p>This is "does this process have push delivery at all", not "could a notification arrive
     * this instant": it stays true across a reconnect, when nothing is being delivered. Narrowing
     * it to the live connection would not help much -- the interval is chosen once per wait, so a
     * wait already sleeping when the connection drops sits out its interval either way, and the gap
     * is the second or two the listener takes to notice and reconnect. Python's _listener_running
     * is set once and left set for the same reason.
     */
    default boolean isRunning() {
      return false;
    }
  }

  class NullNotificationSource implements NotificationSource {

    @Override
    public void start() {}

    @Override
    public void close() {}

    @Override
    public void push(String channel, String payload) {}
  }

  private static final Logger logger = LoggerFactory.getLogger(SystemDatabase.class);

  public static String sanitizeSchema(String schema) {
    return Objects.requireNonNullElse(schema, Constants.DB_SCHEMA).replace("\0", "");
  }

  private final DbContext ctx;
  private final boolean created;

  private final AtomicBoolean closed = new AtomicBoolean(false);
  // See reachesThisDatabase: this database's identity, read at start and refreshed periodically
  // (null when it could not be read), and the answer for each physical connection already probed.
  // Weak, so connections the pool retires can be collected.
  private volatile @Nullable DatabaseIdentity databaseIdentity;
  private final Map<Object, SameDatabaseAnswer> reachesThisDatabaseByConnection =
      Collections.synchronizedMap(new WeakHashMap<>());
  private final AtomicInteger sameDatabaseProbes = new AtomicInteger();
  private final SignalMap signalMap = new SignalMap();
  private final Function<SignalKey, Subscription> createSubscription =
      key -> signalMap.subscribe(key.toString());
  private final NotificationSource notificationSource;

  /**
   * How long a wait re-queries at when nothing is pushing it: awaiting a result, every stream read,
   * and recv and getEvent with no listener running.
   */
  private static final Duration DB_POLLING_INTERVAL = Duration.ofSeconds(1);

  /**
   * How long recv and getEvent wait before re-querying, when a listener is running.
   *
   * <p>The listener signals them promptly, so the re-check is only a safety net: against a
   * notification dropped on the wire or missed across a reconnect, and against a writer that died
   * between committing and its notifier flushing. Without a listener the re-check is the only
   * delivery mechanism, so {@link #DB_POLLING_INTERVAL} is used instead.
   *
   * <p>A stream read does not use this, and polls at {@link #DB_POLLING_INTERVAL} whatever the
   * listener is doing: besides a value arriving, which is pushed, it is also watching for the
   * producer to terminate, and nothing pushes that. Python and Go draw the line in the same place.
   */
  private static final Duration NOTIFICATION_FALLBACK_INTERVAL = Duration.ofSeconds(60);

  /** Maximum pool size of a data source DBOS creates, and the assumed size of one it is handed. */
  static final int DEFAULT_POOL_SIZE = 10;

  /**
   * Half the pool by default (at least one), leaving the rest reachable by the control plane. A
   * configured value is taken as given, including a non-positive one, which turns the limiter off.
   */
  static int resolvePollingConcurrency(DataSource dataSource, @Nullable Integer configured) {
    if (configured != null) {
      return configured;
    }
    var poolSize =
        dataSource instanceof HikariDataSource hds ? hds.getMaximumPoolSize() : DEFAULT_POOL_SIZE;
    return Math.max(1, poolSize / 2);
  }

  private static void validatePostgresDataSource(DataSource dataSource) {
    try (Connection conn = dataSource.getConnection()) {
      String productName = conn.getMetaData().getDatabaseProductName();
      if (!productName.toLowerCase().contains("postgresql")) {
        throw new IllegalStateException(
            "DBOS requires a PostgreSQL datasource, but the provided datasource reports: "
                + productName);
      }
    } catch (SQLException e) {
      throw new IllegalStateException("Failed to validate DBOS datasource", e);
    }
  }

  /**
   * @param created whether this handle owns {@code dataSource} and so closes it with itself. Not a
   *     caller's choice: it follows from who built the pool, which is why the public constructors
   *     set it themselves rather than taking it.
   * @param appName the application the rows written through this handle belong to. Pass the
   *     executor's resolved name rather than {@code config.appName()}: on DBOS Cloud the executor
   *     takes its name from {@code DBOS_APP_NAME}, and row ownership must be the same identity that
   *     the application version hashes and that the peer-ownership checks compare against.
   */
  private SystemDatabase(
      DataSource dataSource,
      String schema,
      boolean created,
      DBOSSerializer serializer,
      boolean useListenNotify,
      String executorId,
      @Nullable String appName,
      Duration notificationCoalesceInterval,
      Integer pollingConcurrency) {
    validatePostgresDataSource(dataSource);
    schema = sanitizeSchema(schema);
    if (schema.contains("\"")) {
      throw new IllegalArgumentException("Schema name must not contain double quotes");
    }

    var pollingLimiter =
        new PollingLimiter(resolvePollingConcurrency(dataSource, pollingConcurrency));
    this.ctx =
        new DbContext(
            dataSource, schema, serializer, this.closed::get, executorId, appName, pollingLimiter);
    this.created = created;
    try {
      useListenNotify = isCockroach(dataSource) ? false : useListenNotify;
    } catch (SQLException e) {
      logger.error("Failed to determine if dataSource is CockroachDB", e);
      useListenNotify = false;
    }

    notificationSource =
        useListenNotify
            ? new ListenNotifySource(ctx, notificationCoalesceInterval, signalMap)
            : new NullNotificationSource();
  }

  /** Builds its own connection pool from {@code url}, and closes it when this handle closes. */
  public SystemDatabase(
      String url,
      String user,
      String password,
      String schema,
      DBOSSerializer serializer,
      boolean useListenNotify,
      @Nullable String appName) {
    this(
        createDataSource(url, user, password, null),
        schema,
        true,
        serializer,
        useListenNotify,
        null,
        appName,
        null,
        null);
  }

  /** Borrows {@code dataSource}, which the caller owns and which outlives this handle. */
  public SystemDatabase(
      DataSource dataSource,
      String schema,
      DBOSSerializer serializer,
      boolean useListenNotify,
      @Nullable String appName) {
    this(dataSource, schema, false, serializer, useListenNotify, null, appName, null, null);
  }

  /**
   * @param appName the application the rows written through this handle belong to. Pass the
   *     executor's resolved name rather than {@code config.appName()}: on DBOS Cloud the executor
   *     takes its name from {@code DBOS_APP_NAME}, and row ownership must be the same identity that
   *     the application version hashes and that the peer-ownership checks compare against.
   */
  public static SystemDatabase create(
      DBOSConfig config, @Nullable String executorId, @Nullable String appName) {
    var dataSource = config.dataSource();
    return new SystemDatabase(
        dataSource != null ? dataSource : createDataSource(config),
        config.databaseSchema(),
        dataSource == null,
        config.serializer(),
        config.useListenNotify(),
        executorId,
        appName,
        config.notificationCoalesceInterval(),
        config.databasePollingConcurrency());
  }

  /**
   * The application this handle acts for. Rows it writes are owned by that application, and reads
   * it does not scope explicitly are scoped to it. Null for a handle given no application, which
   * writes unclaimed rows and reads every application's.
   */
  public @Nullable String applicationName() {
    return ctx.appName();
  }

  // For tests in this package.
  DbContext context() {
    return ctx;
  }

  Optional<HikariConfig> getConfig() {
    if (ctx.dataSource() instanceof HikariDataSource hds) {
      return Optional.of(hds);
    }
    return Optional.empty();
  }

  /** The pool for {@code config}'s database, with its idle-in-transaction timeout. */
  public static HikariDataSource createDataSource(DBOSConfig config) {
    return createDataSource(
        config.databaseUrl(),
        config.dbUser(),
        config.dbPassword(),
        config.systemDatabaseIdleTransactionTimeout());
  }

  /**
   * A pool on a DBOS database. Every connection it opens gets {@code idleTransactionTimeout} as its
   * {@code idle_in_transaction_session_timeout}: the owner check on every checkpoint write locks
   * the workflow's row until the write commits, and a session that stalls inside that transaction
   * must not hold the lock forever.
   *
   * @param idleTransactionTimeout null for {@link
   *     DBOSConfig#DEFAULT_SYSTEM_DATABASE_IDLE_TRANSACTION_TIMEOUT}; zero or negative, or a URL
   *     that sets {@code idle_in_transaction_session_timeout} itself, leaves the setting alone
   */
  public static HikariDataSource createDataSource(
      String url, String user, String password, @Nullable Duration idleTransactionTimeout) {
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl(url);
    config.setUsername(user);
    config.setPassword(password);

    config.setMaxLifetime(60_000);
    config.setKeepaliveTime(30000);
    config.setConnectionTimeout(10000);
    config.setValidationTimeout(2000);
    config.setInitializationFailTimeout(-1);
    config.setMaximumPoolSize(DEFAULT_POOL_SIZE);
    config.setMinimumIdle(DEFAULT_POOL_SIZE);

    config.addDataSourceProperty("tcpKeepAlive", "true");
    config.addDataSourceProperty("connectTimeout", "10");
    config.addDataSourceProperty("socketTimeout", "60");
    config.addDataSourceProperty("reWriteBatchedInserts", "true");

    var timeout =
        Objects.requireNonNullElse(
            idleTransactionTimeout, DBOSConfig.DEFAULT_SYSTEM_DATABASE_IDLE_TRANSACTION_TIMEOUT);
    if (!timeout.isNegative() && !timeout.isZero() && !setsIdleTransactionTimeout(url)) {
      // Rounded up, so a sub-millisecond timeout does not become 0, which Postgres reads as none.
      long ms = Math.max(1, (timeout.toNanos() + 999_999) / 1_000_000);
      config.setConnectionInitSql("SET idle_in_transaction_session_timeout = " + ms);
    }
    return new HikariDataSource(config);
  }

  /** Whether the connection URL already sets the timeout, in its options or as a parameter. */
  static boolean setsIdleTransactionTimeout(@Nullable String url) {
    return url != null
        && url.toLowerCase(Locale.ROOT).contains("idle_in_transaction_session_timeout");
  }

  public static boolean isCockroach(DataSource dataSource) throws SQLException {
    try (var conn = dataSource.getConnection()) {
      return isCockroach(conn);
    }
  }

  public static boolean isCockroach(Connection conn) throws SQLException {
    try (var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT version()")) {
      if (rs.next()) {
        return rs.getString(1).toLowerCase().contains("cockroachdb");
      }
    }
    return false;
  }

  @Override
  public void close() {
    closed.set(true);
    notificationSource.close();
    if (created && ctx.dataSource() instanceof HikariDataSource hikariDataSource) {
      hikariDataSource.close();
    }
  }

  /** For tests: whether this handle listens for notifications, or only polls. */
  boolean hasNotificationListener() {
    return !(notificationSource instanceof NullNotificationSource);
  }

  /** For tests: whether {@link #start} has actually brought the listener up. */
  boolean isNotificationListenerRunning() {
    return notificationSource.isRunning();
  }

  /** For recv and getEvent only; see {@link #NOTIFICATION_FALLBACK_INTERVAL}. */
  private Duration notificationRecheckInterval() {
    return notificationSource.isRunning() ? NOTIFICATION_FALLBACK_INTERVAL : DB_POLLING_INTERVAL;
  }

  public void start() {
    // Read now, while no step holds a connection: reading it from a step would take a second
    // connection from a pool the step may already hold one of.
    databaseIdentity = readDatabaseIdentity();
    notificationSource.start();
  }

  /** How far {@link #anyLinked} walks before giving up; see its note on cycles. */
  private static final int MAX_LINKED_EXCEPTIONS = 1000;

  /**
   * Whether anything reachable from {@code t} along either of JDBC's chains satisfies {@code
   * match}.
   *
   * <p>Two chains, not one. Causes carry DBOS's own wrapping; {@link
   * SQLException#getNextException()} carries what JDBC links rather than wraps -- HikariCP's
   * connection failure behind a pool timeout, and a failed batch's second and later errors.
   *
   * <p>{@link SQLException}'s iterator covers an exception, its causes, its next exceptions and
   * their causes, but not a nested cause's next exceptions, which is why the outer loop re-enters
   * at every level.
   *
   * <p>Bounded because neither chain is guaranteed acyclic: {@link SQLException#setNextException}
   * has no self-link guard, and the iterator over a self-linked exception never ends. Giving up can
   * only lose a SQLSTATE, which hands the failure up rather than retrying it.
   */
  private static boolean anyLinked(Throwable t, Predicate<Throwable> match) {
    int budget = MAX_LINKED_EXCEPTIONS;
    for (Throwable cause = t; cause != null && budget-- > 0; cause = cause.getCause()) {
      if (match.test(cause)) {
        return true;
      }
      if (cause instanceof SQLException sqlException) {
        for (Throwable linked : sqlException) {
          if (budget-- <= 0) {
            return false;
          }
          if (match.test(linked)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /** Whether any SQLSTATE reachable from {@code t} satisfies {@code match}. */
  private static boolean anySqlState(Throwable t, Predicate<String> match) {
    return anyLinked(
        t,
        linked ->
            linked instanceof SQLException sqlException
                && sqlException.getSQLState() != null
                && match.test(sqlException.getSQLState()));
  }

  /** Whether anything in {@code t}'s chains carries a SQLSTATE at all. */
  private static boolean hasSqlState(Throwable t) {
    return anySqlState(t, state -> true);
  }

  /** What {@link #dbRetry} does about a failure. */
  enum Failure {
    /** Reset the pool and try again. */
    CONNECTION,
    /** Try again. */
    TRANSIENT,
    /** Not ours to retry: hand it up. */
    CALLERS
  }

  /**
   * Whether a failure is evidence that the pooled connections themselves are gone, so recycling
   * them is what lets a retry succeed.
   *
   * <p>Separate from {@link #classify} because retrying and recycling are different questions whose
   * answers do not coincide. Class 08 and the {@code 57P0x} shutdown codes mean the connection or
   * the server went away, and a retry on the same socket cannot succeed. {@code 57014
   * query_canceled} is class 57 too, but it means a {@code statement_timeout} fired or someone
   * cancelled the backend: the statement is in trouble and the connection is fine, so recycling
   * every pooled connection over it is collateral damage. A borrow that timed out is likewise
   * contention for healthy connections, and recycling them mid-shortage makes the shortage worse.
   *
   * <p>HikariCP copies the last connection failure's SQLSTATE onto its timeout exception, so a
   * timeout from genuinely broken connections carries class 08 and does evict. That copy is not
   * always about the timeout: {@code PoolBase} records the last failure on a failed keepalive too
   * and clears it only when a new connection is established, so a demand-spike timeout can carry a
   * state from something else a {@code maxLifetime} earlier and be handed to the caller on its
   * account. Left alone: the states realistically sitting there are from connections that could not
   * be made, and handing those up beats retrying them forever.
   */
  static boolean evictsPool(SQLException e) {
    if (hasSqlState(e)) {
      return anySqlState(
          e, state -> state.startsWith("08") || (state.startsWith("57") && !"57014".equals(state)));
    }
    return e instanceof SQLRecoverableException || anyMessage(e, DEAD_CONNECTION_MESSAGES);
  }

  /**
   * What a failed statement means, deciding on the SQLSTATE when there is one and on the exception
   * type only when there is not.
   *
   * <p>These are two tiers, not two alternatives. The dispatch used to OR them -- {@code e
   * instanceof SQLTransientException || isTransientState(e)} -- which let the coarse signal
   * override the precise one. {@link SQLTransientException} has three standard subclasses and one
   * of them, {@link java.sql.SQLTransactionRollbackException}, is class 40: a serialization failure
   * or deadlock arriving as JDBC's standard type went back into this unbounded loop, which is what
   * the conflict-retry change set out to stop. PgJDBC's {@code PSQLException} extends {@link
   * SQLException} directly, so it does not fire on the current driver, which is why no test caught
   * it. The narrow guard that patched it is gone: a class 40 state now falls through to {@link
   * Failure#CALLERS} because it is not a state this retries, without having to be named.
   *
   * <p>All three reference SDKs read the code first and fall back to type or message only for
   * errors carrying no code: Go reads {@code pgErrCode} then {@code net.Error}, Python reads {@code
   * pgcode} then driver message text, TypeScript reads {@code code} then its errno set.
   *
   * <p>This answers only whether to retry; whether to recycle the pool is {@link #evictsPool},
   * decided separately. A {@link Failure#CONNECTION} verdict does not by itself evict: {@code
   * 57014} and a state-less pool timeout are both retried on the pool they arrived from.
   */
  static Failure classify(SQLException e) {
    if (hasSqlState(e)) {
      if (isConnectionState(e)) {
        return Failure.CONNECTION;
      }
      return isTransientState(e) ? Failure.TRANSIENT : Failure.CALLERS;
    }
    if (e instanceof SQLRecoverableException || hasConnectionMessage(e)) {
      return Failure.CONNECTION;
    }
    return e instanceof SQLTransientException ? Failure.TRANSIENT : Failure.CALLERS;
  }

  /**
   * Class 08 connection_exception, class 57 operator_intervention, or 25P03
   * idle_in_transaction_session_timeout, anywhere in the chains. The last is the server ending a
   * session that sat idle inside a transaction; the transaction is gone, and a retry runs on
   * another connection.
   */
  static boolean isConnectionState(Throwable t) {
    return anySqlState(
        t, state -> state.startsWith("08") || state.startsWith("57") || "25P03".equals(state));
  }

  /**
   * Messages naming a connection that is gone. HikariCP and JDBC raise these below the protocol
   * level, with no SQLSTATE to prefer over them.
   */
  private static final List<String> DEAD_CONNECTION_MESSAGES =
      List.of("connection is closed", "connection reset", "broken pipe", "socket closed");

  /**
   * HikariCP's message for a borrow that timed out. Unlike the above this is contention for live
   * connections, not a dead one: worth retrying, not worth recycling the pool over.
   */
  private static final List<String> POOL_EXHAUSTED_MESSAGES =
      List.of("connection is not available");

  /** Whether any message along either chain contains one of {@code needles}. */
  private static boolean anyMessage(Throwable t, List<String> needles) {
    return anyLinked(t, linked -> messageContains(linked, needles));
  }

  private static boolean messageContains(Throwable t, List<String> needles) {
    String msg = t.getMessage();
    if (msg == null) {
      return false;
    }
    String lower = msg.toLowerCase();
    return needles.stream().anyMatch(lower::contains);
  }

  /**
   * Whether any message in the chains names a connection failure.
   *
   * <p>Only for exceptions carrying no SQLSTATE at all: HikariCP and JDBC raise connection errors
   * as bare messages ("Connection is closed"). A message is a guess where a SQLSTATE is a fact, so
   * this is the fallback tier and never overrides one.
   */
  static boolean hasConnectionMessage(Throwable t) {
    return anyMessage(t, DEAD_CONNECTION_MESSAGES) || anyMessage(t, POOL_EXHAUSTED_MESSAGES);
  }

  /** Class 53 insufficient_resources, anywhere in the chains. */
  static boolean isTransientState(Throwable t) {
    return anySqlState(t, state -> state.startsWith("53"));
  }

  /**
   * Whether a failure is a transaction conflict the database has already rolled back: SQLSTATE
   * 40001 serialization_failure or 40P01 deadlock_detected.
   *
   * <p>Named for Python's {@code _is_serialization_error} and TypeScript's {@code
   * isSerializationError}, which match the same two codes; Go's {@code IsRetryableTransaction} is
   * the same predicate under another name.
   *
   * <p>Retrying one means replaying the whole transaction, so only a caller that knows its work is
   * safe to re-run may do it -- see {@link #retryOnSerializationError} for what that requires, and
   * {@link #dbRetryIncludingSerializationError} for the callers that opt in. {@link #dbRetry} does
   * not, so any caller that has not opted in lets the conflict reach its own caller; the dequeue,
   * which does not retry at all ({@link #dbOnce}), relies on that.
   */
  public static boolean isSerializationError(Throwable t) {
    return anySqlState(t, state -> "40001".equals(state) || "40P01".equals(state));
  }

  /**
   * Whether a failure means a peer was mid-dequeue rather than something being wrong: SQLSTATE
   * 55P03 lock_not_available, raised by the {@code FOR UPDATE NOWAIT} that rate-limited queues take
   * so every executor sees a consistent count.
   *
   * <p>40001 serialization_failure counts too: a dequeue that escalates to REPEATABLE READ to keep
   * a shared budget consistent loses the race the same way, and the queue listener reacts to both
   * identically. TypeScript classifies exactly these two codes here, and Go the same plus 40P01.
   * Java follows TypeScript and leaves deadlocks out: nothing in the dequeue expects one, so a
   * deadlock is a real error and should be logged as one.
   *
   * <p>{@link #dbRetry} no longer absorbs class 40, so a serialization failure now reaches this
   * caller rather than being slept off on a pooled thread.
   *
   * <p>The exception arrives wrapped by dbRetry, so this walks the cause chain.
   */
  public static boolean isContentionError(Throwable t) {
    return anySqlState(t, state -> "55P03".equals(state) || "40001".equals(state));
  }

  /**
   * Whether {@code t} is 55P03 lock_not_available: a {@code FOR UPDATE NOWAIT} that lost the race
   * for a row lock.
   *
   * <p>Separate from {@link #isContentionError} because the two codes deserve different reactions
   * at the queue poll loop. This one means a peer holds the rows inside its dequeue transaction --
   * a select, an update and a commit, with dispatch deliberately outside it -- so the obstruction
   * lasts milliseconds and is gone by the next tick. A serialization failure means a peer already
   * committed, which under a shared budget can keep happening, so that one is worth damping.
   *
   * <p>The exception arrives wrapped by dbRetry, so this walks the cause chain.
   */
  public static boolean isLockNotAvailable(Throwable t) {
    return anySqlState(t, "55P03"::equals);
  }

  /**
   * Sleeps for {@code baseMs} scaled by a random factor in [0.5, 1.5), so peers that collided do
   * not collide again.
   *
   * <p>Propagates {@link InterruptedException} rather than restoring the flag and returning. Both
   * callers are retry loops that must stop when cancelled, and a swallowed interrupt reaches them
   * as a normal return with a flag quietly set -- invisible unless the loop remembers to check it.
   * Declaring it makes the compiler ask the question instead. A caller that cannot propagate it
   * restores the flag with {@code Thread.currentThread().interrupt()} and stops.
   */
  public static void sleepWithJitter(double baseMs) throws InterruptedException {
    double jitter = 0.5 + ThreadLocalRandom.current().nextDouble(); // [0.5, 1.5)
    Thread.sleep((long) (baseMs * jitter));
  }

  /** A database call returning nothing, which may fail with {@link SQLException}. */
  @FunctionalInterface
  public interface SqlRunnable {
    void run() throws SQLException;
  }

  private void dbRetry(SqlRunnable runnable) {
    dbRetry(
        () -> {
          runnable.run();
          return null;
        });
  }

  /** A database call returning a value, which may fail with {@link SQLException}. */
  @FunctionalInterface
  public interface SqlSupplier<T> {
    T get() throws SQLException;
  }

  /**
   * Replays work that a transaction conflict rolled back, up to ten times.
   *
   * <p>For callers whose work is safe to re-run -- which is stronger than it sounds. A
   * serialization error means a <em>peer committed</em>, so the replay never sees the state the
   * first attempt saw; it sees a later one. The work must be safe against a database that changed
   * underneath it, not merely safe because a rollback undid the first attempt. See {@link
   * #dbRetryIncludingSerializationError} for what qualifies.
   *
   * <p>Only a caller that knows this may use it. {@link #dbRetry} deliberately does not, and the
   * dequeue does not retry at all ({@link #dbOnce}): a conflict on the dequeue is a signal the
   * queue poll loop needs rather than one to sleep off.
   *
   * <p>Bounded, unlike {@link #dbRetry}: a conflict means a peer won, which is progress, so
   * spinning forever would only mean this caller never does. The schedule is Python's and
   * TypeScript's exactly -- ten attempts, 50 ms doubling to a 2 s cap, jittered so peers that
   * collided do not collide again.
   *
   * <p>Stops on interruption, leaving the flag set for the caller. Both exhaustion and cancellation
   * give back the same {@link SQLException}, and the interrupt flag is the only thing that
   * separates them.
   *
   * @param operation what is being replayed, for the log
   * @param work the database call, which must be safe to run more than once
   */
  public static <T> T retryOnSerializationError(String operation, SqlSupplier<T> work)
      throws SQLException {
    final int maxAttempts = 10;
    final double maxBackoffMs = 2000.0;
    double backoffMs = 50.0;
    for (int attempt = 1; ; attempt++) {
      try {
        return work.get();
      } catch (SQLException e) {
        if (!isSerializationError(e)) {
          throw e;
        }
        if (attempt == maxAttempts) {
          logger.warn("{} failed after {} attempts", operation, maxAttempts, e);
          throw e;
        }
        logger.warn(
            "Contention or deadlock detected in {} (attempt {}); retrying", operation, attempt, e);
        try {
          sleepWithJitter(backoffMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          logger.warn("{} interrupted after {} attempts; giving up", operation, attempt, e);
          throw e;
        }
        backoffMs = Math.min(backoffMs * 2, maxBackoffMs);
      }
    }
  }

  /** As {@link #retryOnSerializationError(String, SqlSupplier)}, for work returning nothing. */
  public static void retryOnSerializationError(String operation, SqlRunnable work)
      throws SQLException {
    retryOnSerializationError(
        operation,
        () -> {
          work.run();
          return null;
        });
  }

  /**
   * As {@link #dbRetry(SqlSupplier)}, but a serialization error is replayed rather than thrown at
   * the caller -- so this retries what {@link #retryOnSerializationError} does as well as what
   * {@link #dbRetry} does.
   *
   * <p>The caller asserts the precondition by choosing this method, and it is stronger than it
   * looks. A serialization error means a <em>peer committed</em>, so the replay never sees the
   * state the first attempt saw -- it sees a later one. The work must therefore be safe to re-run
   * against a database that has changed underneath it, not merely safe because a rollback undid the
   * first attempt.
   *
   * <p>That holds for an upsert keyed on identity, an insert guarded by {@code ON CONFLICT}, a
   * delete, a read, or a step whose recorded-result check makes a peer's win the right answer. It
   * does <em>not</em> hold when part of the work has already committed -- a transaction followed by
   * batched sweeps replays the committed part, and anything it counts or returns will be wrong. See
   * {@code renameApplication}, which is excluded for exactly that reason.
   *
   * <p>The replay goes <em>inside</em> the connection retry, and that order matters: {@link
   * #dbRetry} turns a failure it will not retry into a {@link RuntimeException}, which {@link
   * #retryOnSerializationError} does not catch, so the other nesting would silently replay nothing.
   * Composing it here means no call site can get that wrong.
   *
   * @param operation what is being run, for the log
   * @param supplier the work, which must be safe to run more than once
   */
  private <T> T dbRetryIncludingSerializationError(String operation, SqlSupplier<T> supplier) {
    return dbRetry(() -> retryOnSerializationError(operation, supplier));
  }

  /**
   * As {@link #dbRetryIncludingSerializationError(String, SqlSupplier)}, for work returning
   * nothing.
   */
  private void dbRetryIncludingSerializationError(String operation, SqlRunnable runnable) {
    dbRetry(() -> retryOnSerializationError(operation, runnable));
  }

  /**
   * Runs {@code supplier}, retrying while the failure looks like it will pass, and adapting the
   * checked {@link SQLException} the caller cannot declare into an unchecked one.
   *
   * <p>Every exit that carries a database failure wraps it in {@link DBOSSystemDatabaseException}
   * and does so <b>exactly once</b>: this never throws from a path it would retry, so nothing
   * re-enters and wraps a second time. That is what lets the SQLSTATE classifiers find the original
   * one level down a chain of known depth.
   *
   * @throws DBOSSystemDatabaseException if the failure is not one worth retrying, or an interrupt
   *     stops the loop
   * @throws IllegalStateException if the system database has been closed, which is a lifecycle
   *     error rather than a database one and so is not wrapped
   */
  private <T> T dbRetry(SqlSupplier<T> supplier) {
    double backoffMs = 1000.0;
    final double maxBackoffMs = 60_000.0;
    int attempt = 0;
    while (true) {
      if (closed.get()) {
        throw new IllegalStateException("SystemDatabase is closed");
      }
      try {
        return supplier.get();
      } catch (SQLException e) {
        attempt++;
        switch (classify(e)) {
          case CONNECTION -> {
            if (evictsPool(e)) {
              logger.warn(
                  "Recoverable connection error (attempt {}), resetting client pool", attempt, e);
              softEvictPool();
            } else {
              logger.warn(
                  "Recoverable connection error (attempt {}), retrying on the same pool",
                  attempt,
                  e);
            }
          }
          case TRANSIENT -> logger.warn("Transient DB error (attempt {}), retrying", attempt, e);
          case CALLERS -> throw new DBOSSystemDatabaseException(e);
        }
        try {
          sleepWithJitter(backoffMs);
        } catch (InterruptedException ie) {
          // This loop is otherwise unbounded, so an ignored interrupt means nothing can stop it.
          // Restore the flag for the caller and give back the failure that was being retried.
          Thread.currentThread().interrupt();
          logger.warn("Interrupted while retrying a database operation (attempt {})", attempt, e);
          throw new DBOSSystemDatabaseException(e);
        }
        backoffMs = Math.min(backoffMs * 2, maxBackoffMs);
      }
    }
  }

  /**
   * Runs {@code supplier} once, adapting its {@link SQLException} as {@link #dbRetry} does but
   * never retrying it.
   *
   * <p>For the queue poll. Its loop is already the retry: a failed poll is logged and the next one
   * comes a polling interval later. Retrying inside a poll instead would hold the listener, and the
   * poll pass {@code pause()} waits on, for as long as the database stays down. A connection
   * failure that calls for a fresh pool still gets one, so the next poll does not draw the same
   * dead connections.
   *
   * @throws DBOSSystemDatabaseException if the call fails
   * @throws IllegalStateException if the system database has been closed
   */
  private <T> T dbOnce(SqlSupplier<T> supplier) {
    if (closed.get()) {
      throw new IllegalStateException("SystemDatabase is closed");
    }
    try {
      return supplier.get();
    } catch (SQLException e) {
      if (classify(e) == Failure.CONNECTION && evictsPool(e)) {
        logger.warn("Connection error on a queue poll, resetting client pool", e);
        softEvictPool();
      }
      throw new DBOSSystemDatabaseException(e);
    }
  }

  private void softEvictPool() {
    if (ctx.dataSource() instanceof HikariDataSource hikariDataSource) {
      hikariDataSource.getHikariPoolMXBean().softEvictConnections();
    }
  }

  /**
   * The database's clock in epoch milliseconds, for inlining into SQL. The workflow_status and
   * queues times that executors compare with each other are stamped and compared on this clock:
   * created_at, updated_at, completed_at, started_at_epoch_ms, the rate-limit window, a workflow's
   * deadline, and the release of a delayed workflow. Executors sharing a system database would
   * otherwise write those rows on as many clocks as there are hosts, and FIFO order, rate limits,
   * timeouts, delays and retention would all inherit the skew between them. Where an executor acts
   * on a deadline itself, it measures the time left from a {@link DatabaseTime} reading with its
   * monotonic clock.
   *
   * <p>Times that only the writing executor reads stay on the JVM's clock: step timings and the
   * durable sleep, recv and getEvent timeouts. So, for now, do the end of a relative delay, which
   * the enqueuing JVM resolves, and debounce deadlines.
   *
   * <p>now() is the transaction's start time on Postgres and CockroachDB alike, so every statement
   * in one transaction reads the same value. It matches the column defaults, which is what keeps a
   * payload row's retention_timestamp from landing before its status row's created_at.
   */
  public static final String NOW_EPOCH_MS = "(EXTRACT(epoch FROM now()) * 1000.0)::bigint";

  /**
   * SQL for the deadline {@code timeoutMs} from {@link #NOW_EPOCH_MS}. A timeout within the current
   * epoch milliseconds of the bigint maximum would overflow the sum, so the deadline saturates at
   * that maximum instead, which never elapses. Null if {@code timeoutMs} is null.
   *
   * @param timeoutMs a SQL expression for the timeout, evaluated twice
   */
  public static String deadlineEpochMs(String timeoutMs) {
    return "CASE WHEN %1$s > 9223372036854775807 - %2$s THEN 9223372036854775807 ELSE %2$s + %1$s END"
        .formatted(timeoutMs, NOW_EPOCH_MS);
  }

  /**
   * The database's clock in epoch milliseconds as the statement evaluates it, for a {@link
   * DatabaseTime} reading. Unlike {@link #NOW_EPOCH_MS}, which is the transaction's start, it
   * includes the time the statement spent before returning, such as a wait on a lock, so a deadline
   * measured from the reading is not pushed out by that wait. Never stamped into a row: rows take
   * {@link #NOW_EPOCH_MS}, so one transaction's stamps agree.
   */
  public static final String CLOCK_TIMESTAMP_EPOCH_MS =
      "(EXTRACT(epoch FROM clock_timestamp()) * 1000.0)::bigint";

  public static Instant toInstant(Long epochMs) {
    return epochMs != null ? Instant.ofEpochMilli(epochMs) : null;
  }

  public static Duration toDuration(Long ms) {
    return ms != null ? Duration.ofMillis(ms) : null;
  }

  /**
   * The error column of a workflow or a step, or null when it records no error.
   *
   * <p>The Go SDK stores the error as a non-nullable string, so a workflow that succeeded leaves ""
   * behind where this one writes NULL. Empty only: no SDK writes a blank non-empty value.
   */
  public static String errorOrNull(String error) {
    return error != null && error.isEmpty() ? null : error;
  }

  /**
   * Initializes the status of a workflow.
   *
   * <p>Only the call that created a row owns its execution, with the token returned in {@link
   * WorkflowInitResult#ownerXid()}: a caller that finds a row someone else inserted gets {@code
   * shouldExecuteOnThisExecutor() == false} and polls for the outcome instead. A workflow the queue
   * has already claimed never comes through here -- the claim wrote everything this would, so the
   * dispatch path runs from the claimed row directly.
   *
   * @param initStatus The initial workflow status details.
   * @param maxRetries The workflow's configured attempt budget, reported if it is already
   *     dead-lettered.
   * @param workflowIdReusePolicy What to do when a workflow with this ID already exists; null is
   *     {@link WorkflowIdReusePolicy#RETURN_EXISTING}.
   * @return An object containing the current status and optionally the deadline epoch milliseconds.
   * @throws DBOSConflictingWorkflowException If a conflicting workflow already exists.
   * @throws DBOSWorkflowIdInUseException If the policy is {@link WorkflowIdReusePolicy#REJECT} and
   *     a workflow with this ID already exists.
   * @throws DBOSMaxRecoveryAttemptsExceededException If the workflow has already been
   *     dead-lettered.
   */
  public WorkflowInitResult initWorkflowStatus(
      WorkflowStatusInternal initStatus,
      @Nullable Integer maxRetries,
      @Nullable WorkflowIdReusePolicy workflowIdReusePolicy) {
    return initWorkflowStatus(initStatus, maxRetries, workflowIdReusePolicy, null);
  }

  /**
   * As {@link #initWorkflowStatus(WorkflowStatusInternal, Integer, WorkflowIdReusePolicy)}, for a
   * child workflow: the child's row and its parent's step recording it are written in one
   * transaction, and only while the parent's execution still owns the parent, so a parent that has
   * lost its workflow can neither start a child nor record one.
   */
  public WorkflowInitResult initChildWorkflowStatus(
      WorkflowStatusInternal initStatus,
      @Nullable Integer maxRetries,
      @Nullable WorkflowIdReusePolicy workflowIdReusePolicy,
      ChildOf parent) {
    return initWorkflowStatus(initStatus, maxRetries, workflowIdReusePolicy, parent);
  }

  private WorkflowInitResult initWorkflowStatus(
      WorkflowStatusInternal initStatus,
      @Nullable Integer maxRetries,
      @Nullable WorkflowIdReusePolicy workflowIdReusePolicy,
      @Nullable ChildOf parent) {
    // The token tells this call whether it created the row or found one already there. Generated
    // outside the retry loop, so an attempt that committed but lost its acknowledgement recognizes
    // its own row on the retry. A row created PENDING is also owned by it.
    String creatorXid = UUID.randomUUID().toString();
    Long delayUntilEpochMs = resolveDelayUntil(initStatus);
    // The insert sets a directly started workflow's deadline on the database's clock, so a retry
    // measures it from the attempt that commits. One that committed but lost its ack finds its own
    // row, and keeps the deadline that row was written with. A child's start locks two status rows,
    // the child's and then its parent's, so it can deadlock with a multi-row write; replaying it is
    // safe, as the insert keeps a row already there and the parent's step re-checks its owner.
    return dbRetryIncludingSerializationError(
        "initWorkflowStatus",
        () ->
            WorkflowDAO.initWorkflowStatus(
                ctx,
                initStatus,
                delayUntilEpochMs,
                maxRetries,
                creatorXid,
                workflowIdReusePolicy,
                parent));
  }

  /**
   * The absolute end of a relative delay, counted from this JVM's clock, as Python and TypeScript
   * count it from their process's. Resolved once, outside the write's retry loop: resolved inside
   * it, every retry would push the delay later by that attempt's backoff.
   */
  private static @Nullable Long resolveDelayUntil(@Nullable Duration delay) {
    if (delay == null) {
      return null;
    }
    try {
      return Math.addExact(System.currentTimeMillis(), delay.toMillis());
    } catch (ArithmeticException e) {
      throw new DBOSDelayPastLatestInstantException();
    }
  }

  /**
   * The absolute end of a status's delay. A debounced workflow's is capped at its debounce
   * deadline, as every later bounce caps it.
   */
  private static @Nullable Long resolveDelayUntil(WorkflowStatusInternal status) {
    Long delayUntil = resolveDelayUntil(status.delay());
    Long cap = status.debounceDeadlineEpochMs();
    return delayUntil != null && cap != null && cap < delayUntil ? cap : delayUntil;
  }

  /**
   * Store the result to workflow_output, marking the workflow SUCCESS
   *
   * @param workflowId id of the workflow
   * @param result output serialized as json
   * @param ownerXid the token of the execution recording it
   * @return true if the outcome was recorded, false if the row is no longer PENDING and this
   *     execution no longer owns the workflow's outcome
   */
  public boolean recordWorkflowOutput(String workflowId, String result, String ownerXid) {
    return dbRetryIncludingSerializationError(
        "recordWorkflowOutput",
        () -> WorkflowDAO.recordWorkflowOutput(ctx, workflowId, result, ownerXid));
  }

  /**
   * Store the error to workflow_output, marking the workflow ERROR
   *
   * @param workflowId id of the workflow
   * @param error output serialized as json
   * @param ownerXid the token of the execution recording it
   * @return true if the outcome was recorded, false if the row is no longer PENDING and this
   *     execution no longer owns the workflow's outcome
   */
  public boolean recordWorkflowError(String workflowId, String error, String ownerXid) {
    return dbRetryIncludingSerializationError(
        "recordWorkflowError",
        () -> WorkflowDAO.recordWorkflowError(ctx, workflowId, error, ownerXid));
  }

  /**
   * Insert a workflow_status row already in the ERROR state, for a workflow that was never started.
   * See {@link WorkflowDAO#recordErrorForUnstartedWorkflow}.
   */
  public void recordErrorForUnstartedWorkflow(WorkflowStatusInternal initStatus, String error) {
    Long delayUntilEpochMs = resolveDelayUntil(initStatus);
    dbRetry(
        () ->
            WorkflowDAO.recordErrorForUnstartedWorkflow(ctx, initStatus, delayUntilEpochMs, error));
  }

  public WorkflowStatus getWorkflowStatus(String workflowId) {
    return dbRetry(() -> WorkflowDAO.getWorkflowStatus(ctx, workflowId));
  }

  /** A workflow's status, with the database's clock as it was read; null if there is none. */
  public @Nullable TimedWorkflowStatus getTimedWorkflowStatus(String workflowId) {
    return dbRetry(() -> WorkflowDAO.getTimedWorkflowStatus(ctx, workflowId));
  }

  /** The token of the execution that owns the workflow, read without a lock; null if none. */
  /**
   * Whether {@code dataSource} draws from the pool this system database uses, seen through wrappers
   * such as a transaction-aware proxy. A caller holding a connection from that pool must not take a
   * second one: concurrent callers doing so can exhaust the pool waiting on each other.
   */
  public boolean sharesPool(DataSource dataSource) {
    return poolOf(dataSource) == poolOf(ctx.dataSource());
  }

  private static DataSource poolOf(DataSource dataSource) {
    try {
      if (dataSource.isWrapperFor(HikariDataSource.class)) {
        return dataSource.unwrap(HikariDataSource.class);
      }
    } catch (SQLException e) {
      // Not unwrappable: compare the object itself.
    }
    return dataSource;
  }

  // The database a connection reaches: its server's system_identifier and when the server started,
  // then the database's name and OID. system_identifier is set when the server's data directory
  // was created, so every copy of it -- a server restored from another's backup, a cloud clone, a
  // container from a prebuilt image -- has the same one; the start time tells copies running at
  // the same time apart. Name and OID are unique only within one server. The start time is in
  // epoch seconds so the session's time zone cannot change the text. A proxy such as PgBouncer
  // reports the server behind it.
  static final String DATABASE_IDENTITY_SQL =
      """
      SELECT (SELECT system_identifier FROM pg_control_system()) || '/'
             || extract(epoch FROM pg_postmaster_start_time()) || '/' || current_database()
             || '/' || d.oid
      FROM pg_database d WHERE d.datname = current_database()
      """;

  // Where the user may not call pg_control_system(), the rest of the identity alone. Two servers
  // agreeing on their start time, to the microsecond, and on the database's name and OID is not a
  // realistic coincidence.
  static final String DATABASE_IDENTITY_BY_START_TIME_SQL =
      """
      SELECT extract(epoch FROM pg_postmaster_start_time()) || '/' || current_database()
             || '/' || d.oid
      FROM pg_database d WHERE d.datname = current_database()
      """;

  /** The identity of a database, and the query that read it, for reading it again to compare. */
  record DatabaseIdentity(String sql, String value) {}

  // A probe's answer, and the system database's identity it was compared with: after a restart
  // of the database server changes that identity, the answer is probed again.
  private record SameDatabaseAnswer(String identity, boolean reaches) {}

  /**
   * Whether {@code conn}, a transactional step's connection, reaches the database this system
   * database lives in, as a database user that may lock its {@code workflow_status} rows. Then a
   * step can check that its execution owns the workflow on {@code conn} itself ({@link
   * #checkOwner}) instead of on a second connection.
   *
   * <p>It matters when the step's factory cannot say which pool {@code conn} came from. Two
   * connections from the same pool reach the same database, so a step on this system database's
   * pool is always recognized, and never asks that pool for a second connection while holding one:
   * steps doing so can hold every connection between them and each wait forever for one more. A
   * connection that reaches another database cannot come from this pool, so taking a second
   * connection is safe for it.
   *
   * <p>Each physical connection is probed once, and again after the system database's identity
   * changes (see {@link #refreshDatabaseIdentity}). The probe runs under a savepoint when {@code
   * conn} is in a transaction, so a server that cannot answer it leaves that transaction intact;
   * such a failure answers no and is not remembered. The privilege is checked for the user the
   * connection had when it was probed: a step that switches to another role ({@code SET ROLE}) that
   * may not lock the row fails its ownership check.
   */
  public boolean reachesThisDatabase(Connection conn) {
    var identity = databaseIdentity;
    if (identity == null) {
      return false;
    }
    var key = physicalConnection(conn);
    var known = reachesThisDatabaseByConnection.get(key);
    if (known != null && known.identity().equals(identity.value())) {
      return known.reaches();
    }
    var reaches = probeReachesThisDatabase(conn, identity);
    if (reaches == null) {
      return false;
    }
    reachesThisDatabaseByConnection.put(key, new SameDatabaseAnswer(identity.value(), reaches));
    return reaches;
  }

  /** For tests: stands in for the identity changing, as a restart of the database server does. */
  void replaceDatabaseIdentity(DatabaseIdentity identity) {
    databaseIdentity = identity;
  }

  /** For tests: the identity steps' connections are compared with. */
  @Nullable DatabaseIdentity databaseIdentity() {
    return databaseIdentity;
  }

  /** For tests: how many times {@link #reachesThisDatabase} has probed a connection. */
  int sameDatabaseProbes() {
    return sameDatabaseProbes.get();
  }

  // Pools and proxies hand out a fresh wrapper for each borrow; the driver's connection beneath is
  // what stays the same, and what the probe's answer belongs to. Some wrappers return themselves
  // when asked for a java.sql.Connection, so ask for the driver's own type.
  private static Object physicalConnection(Connection conn) {
    try {
      if (conn.isWrapperFor(PGConnection.class)) {
        return conn.unwrap(PGConnection.class);
      }
    } catch (SQLException e) {
      // Not unwrappable: key on the connection itself.
    }
    return conn;
  }

  // The probe's answer, or null when it could not be had.
  private @Nullable Boolean probeReachesThisDatabase(Connection conn, DatabaseIdentity identity) {
    sameDatabaseProbes.incrementAndGet();
    // A user who cannot run the identity query fails here, under the savepoint.
    var sql =
        """
        SELECT (%s) = ?,
               COALESCE(has_table_privilege(t.c, 'SELECT') AND has_table_privilege(t.c, 'UPDATE'),
                        false)
        FROM (SELECT to_regclass(?) AS c) t
        """
            .formatted(identity.sql());
    Savepoint savepoint = null;
    try {
      if (!conn.getAutoCommit()) {
        savepoint = conn.setSavepoint();
      }
      boolean reaches;
      try (var stmt = conn.prepareStatement(sql)) {
        stmt.setString(1, identity.value());
        stmt.setString(2, "\"%s\".workflow_status".formatted(ctx.schema()));
        try (var rs = stmt.executeQuery()) {
          reaches = rs.next() && rs.getBoolean(1) && rs.getBoolean(2);
        }
      }
      if (savepoint != null) {
        conn.releaseSavepoint(savepoint);
      }
      return reaches;
    } catch (SQLException e) {
      if (savepoint != null) {
        try {
          conn.rollback(savepoint);
        } catch (SQLException rollbackFailure) {
          e.addSuppressed(rollbackFailure);
        }
      }
      logger.debug("Could not tell whether a step's connection reaches the system database", e);
      return null;
    }
  }

  // Null when this server cannot say; steps then check ownership as before.
  private @Nullable DatabaseIdentity readDatabaseIdentity() {
    try {
      return dbRetry(
          () -> {
            try (var conn = ctx.getConnection()) {
              return readDatabaseIdentity(conn);
            }
          });
    } catch (DBOSSystemDatabaseException e) {
      logger.debug("Could not read the system database's identity", e);
      return null;
    }
  }

  /**
   * Reads the system database's identity again, so a restart of its server, which changes the
   * identity, stops making steps' connections look like they reach another database. Called
   * periodically from a background thread, and by a step that could not check its ownership and has
   * released its connection; never while holding one, as it takes a connection from the pool. One
   * attempt; a failure keeps the identity it had.
   */
  public void refreshDatabaseIdentity() {
    if (databaseIdentity == null) {
      return;
    }
    try (var conn = ctx.getConnection()) {
      var identity = readDatabaseIdentity(conn);
      if (identity != null) {
        databaseIdentity = identity;
      }
    } catch (SQLException | RuntimeException e) {
      logger.debug("Could not refresh the system database's identity", e);
    }
  }

  /**
   * The identity of the database {@code conn} reaches: with its server's system_identifier, or
   * without it where the user may not read it; null when neither can be read. A failure that may
   * pass, such as a lost connection, is thrown, so a caller can retry rather than settle for less.
   */
  static @Nullable DatabaseIdentity readDatabaseIdentity(Connection conn) throws SQLException {
    for (var sql : List.of(DATABASE_IDENTITY_SQL, DATABASE_IDENTITY_BY_START_TIME_SQL)) {
      try (var stmt = conn.createStatement();
          var rs = stmt.executeQuery(sql)) {
        if (rs.next()) {
          return new DatabaseIdentity(sql, rs.getString(1));
        }
      } catch (SQLException e) {
        // Only a query this server or user will never answer, such as a missing function or a
        // missing privilege, moves on to the next one.
        if (classify(e) != Failure.CALLERS) {
          throw e;
        }
        logger.debug("Could not read the database's identity", e);
        // A failed statement aborts a transaction the pool's connection may have open.
        if (!conn.getAutoCommit()) {
          conn.rollback();
        }
      }
    }
    return null;
  }

  /**
   * {@link WorkflowDAO#checkOwner} on {@code conn}, inside its caller's transaction, for a caller
   * whose connection comes from this system database's pool. Not retried: a failure rolls back the
   * caller's transaction, and retrying it is the caller's.
   */
  public void checkOwner(Connection conn, String workflowId, String ownerXid) {
    try {
      WorkflowDAO.checkOwner(conn, ctx.schema(), workflowId, ownerXid);
    } catch (SQLException e) {
      throw new DBOSSystemDatabaseException(e);
    }
  }

  public @Nullable String getWorkflowOwner(String workflowId) {
    return dbRetry(() -> WorkflowDAO.getWorkflowOwner(ctx, workflowId));
  }

  /**
   * Reads the workflow's owner in one attempt, for a caller that holds a connection of this
   * database's pool and so must not wait on that pool indefinitely: steps that hold every
   * connection between them, each waiting for one more, would never get one.
   *
   * @throws ConnectionUnavailableException when the attempt fails in a way {@link #dbRetry} would
   *     retry, such as the pool giving no connection within its timeout. The caller releases its
   *     connection and tries again.
   */
  public @Nullable String getWorkflowOwnerOnce(String workflowId) {
    try {
      return WorkflowDAO.getWorkflowOwner(ctx, workflowId);
    } catch (SQLException e) {
      if (classify(e) == Failure.CALLERS) {
        throw new DBOSSystemDatabaseException(e);
      }
      throw new ConnectionUnavailableException(e);
    }
  }

  /**
   * Thrown by {@link #getWorkflowOwnerOnce} when it got no answer for a reason that may pass, such
   * as the pool having no connection to give. A transactional step that sees it rolls back,
   * releasing its connection, and runs again.
   */
  public static final class ConnectionUnavailableException extends RuntimeException {
    ConnectionUnavailableException(SQLException cause) {
      super(cause);
    }
  }

  public String getWorkflowSerialization(String workflowId) {
    return dbRetry(() -> WorkflowDAO.getWorkflowSerialization(ctx, workflowId));
  }

  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
    return dbRetry(() -> WorkflowDAO.listWorkflows(ctx, input));
  }

  public @Nullable String findWorkflowIdByDeduplicationId(
      String queueName, String deduplicationId) {
    return dbRetry(
        () -> WorkflowDAO.findWorkflowIdByDeduplicationId(ctx, queueName, deduplicationId));
  }

  public @Nullable DeduplicationHolder findDeduplicationHolder(
      String queueName, String deduplicationId) {
    return dbRetry(() -> WorkflowDAO.findDeduplicationHolder(ctx, queueName, deduplicationId));
  }

  /**
   * Cancels a debouncer workflow that stopped answering and creates the user workflow it promised,
   * in one transaction; as the caller's step when one is given, checkpointed in the same
   * transaction. Returns the promised workflow's id if this call created it, otherwise null, as
   * {@code Object}. See {@link WorkflowDAO#takeOverDebouncerWorkflow}.
   */
  public Object takeOverDebouncerWorkflow(
      String debouncerWorkflowId,
      @Nullable WorkflowStatusInternal promised,
      @Nullable DebounceCaller caller) {
    // The delay is resolved outside the retry, as in initWorkflowStatus, so a retry cannot push it
    // later. An attempt that committed but lost its ack finds the holder cancelled on the retry and
    // returns null; the caller then goes on to extend the promised workflow like any other call.
    // Safe to replay after a peer committed, as a serialization error means one did: the holder is
    // re-checked under its lock, the step is re-checked, and the insert keeps a row already there.
    String ownerXid = UUID.randomUUID().toString();
    Long delayUntilEpochMs = promised == null ? null : resolveDelayUntil(promised);
    return dbRetryIncludingSerializationError(
        "takeOverDebouncerWorkflow",
        () ->
            WorkflowDAO.takeOverDebouncerWorkflow(
                ctx, debouncerWorkflowId, promised, delayUntilEpochMs, ownerXid, caller));
  }

  /**
   * Extends a debounced DELAYED workflow's delay and replaces its inputs, or reports who holds the
   * pair instead; as the caller's step when one is given, checkpointed in the same transaction. See
   * {@link WorkflowDAO#debounceDelayedWorkflow}.
   */
  public Object debounceDelayedWorkflow(
      String workflowName,
      String className,
      @Nullable String instanceName,
      String queueName,
      String deduplicationId,
      long delayUntilEpochMs,
      Object[] args,
      @Nullable String serializationFormat,
      @Nullable DebounceIds ids,
      @Nullable DebounceCaller caller) {
    // The bounce locks the delayed workflow's row and the caller's step checkpoint then locks the
    // caller's, so it can deadlock with a bulk cancel, resume or recovery taking both the other
    // way. Safe to replay: the step is re-checked first, and the delay and inputs it writes are
    // fixed before the first attempt.
    return dbRetryIncludingSerializationError(
        "debounceDelayedWorkflow",
        () ->
            WorkflowDAO.debounceDelayedWorkflow(
                ctx,
                workflowName,
                className,
                instanceName,
                queueName,
                deduplicationId,
                delayUntilEpochMs,
                args,
                serializationFormat,
                ids,
                caller));
  }

  /** Returns the given executors' PENDING workflows to their queues; reports which rows moved. */
  public List<String> reenqueueForRecovery(
      List<String> executorIds, String appVersion, String recoveryQueueName) {
    return dbRetry(
        () -> QueuesDAO.reenqueueForRecovery(ctx, executorIds, appVersion, recoveryQueueName));
  }

  /**
   * Moves claimed workflows that have exhausted their attempts off the queue.
   *
   * <p>Guarded on PENDING like every other claim-owning write, and on the attempt count the
   * decision was read from, so a row another executor has already moved on -- or one given a fresh
   * budget by resume -- is left alone.
   */
  public void deadLetterWorkflows(List<String> workflowIds, int minRecoveryAttempts) {
    if (workflowIds.isEmpty()) {
      return;
    }
    dbRetry(() -> WorkflowDAO.deadLetterWorkflows(ctx, workflowIds, minRecoveryAttempts));
  }

  public List<String> getQueuePartitions(String queueName) {
    return dbOnce(() -> QueuesDAO.getQueuePartitions(ctx, queueName));
  }

  public boolean upsertQueue(
      String name, QueueOptions options, boolean updateExisting, @Nullable String applicationName) {
    if (Constants.DBOS_INTERNAL_QUEUE.equals(name)) {
      throw new IllegalArgumentException(
          String.format("%s is a reserved queue name", Constants.DBOS_INTERNAL_QUEUE));
    }
    return dbRetryIncludingSerializationError(
        "upsertQueue",
        () -> QueuesDAO.upsertQueue(ctx, name, options, updateExisting, applicationName));
  }

  public void updateQueue(String name, QueueOptions update) {
    dbRetry(() -> QueuesDAO.updateQueue(ctx, name, update));
  }

  public Optional<Queue> findQueue(String name) {
    return dbRetry(() -> QueuesDAO.findQueue(ctx, name));
  }

  public List<Queue> listQueues() {
    return listQueues(null);
  }

  public List<Queue> listQueues(@Nullable List<String> applicationName) {
    return dbRetry(() -> QueuesDAO.listQueues(ctx, applicationName));
  }

  public boolean deleteQueue(String name) {
    return dbRetry(() -> QueuesDAO.deleteQueue(ctx, name));
  }

  public StepResult checkStepResult(
      String workflowId, int functionId, String functionName, @Nullable String ownerXid) {

    return dbRetry(
        () -> {
          try (Connection connection = ctx.getConnection()) {
            return StepsDAO.checkStepResult(
                connection, ctx.schema(), workflowId, functionId, functionName, ownerXid);
          }
        });
  }

  /**
   * Records a step of the execution holding {@code ownerXid}, if it still owns the workflow. See
   * {@link StepsDAO#recordStepResult(DbContext, Connection, StepResult, Long, Long, String)}.
   */
  public void recordStepResult(StepResult result, long startTime, String ownerXid) {
    var et = System.currentTimeMillis();
    // The owner check locks the workflow's row, so the record can deadlock with a multi-row write.
    // Replaying it is safe: the check runs again, and the same row written twice is accepted.
    dbRetryIncludingSerializationError(
        "recordStepResult", () -> StepsDAO.recordStepResult(ctx, result, startTime, et, ownerXid));
  }

  public List<StepInfo> listWorkflowSteps(
      String workflowId, Boolean loadOutput, Integer limit, Integer offset) {
    return dbRetry(() -> StepsDAO.listWorkflowSteps(ctx, workflowId, loadOutput, limit, offset));
  }

  /**
   * Awaits a workflow's recorded outcome. A missing row normally means the workflow just hasn't
   * been inserted yet (an unchecked retrieve, or a debounced workflow whose row appears only after
   * the debounce period), so by default it is polled for. Callers that know the row must already
   * exist pass {@code failIfMissing} to fail fast instead.
   */
  public <T> Result<T> awaitWorkflowResult(String workflowId, boolean failIfMissing) {
    // Not a notification wait: no channel carries workflow completion, so this poll is the only
    // delivery mechanism and stays short whether or not a listener is running.
    return dbRetry(
        () ->
            WorkflowDAO.<T>awaitWorkflowResult(
                ctx, DB_POLLING_INTERVAL, workflowId, failIfMissing));
  }

  public List<String> startQueuedWorkflows(
      Queue queue,
      String executorId,
      String appVersion,
      String partitionKey,
      long localRunningCount,
      long partitionLocalRunningCount,
      String ownerXid) {
    return dbOnce(
        () ->
            QueuesDAO.startQueuedWorkflows(
                ctx,
                queue,
                executorId,
                appVersion,
                partitionKey,
                localRunningCount,
                partitionLocalRunningCount,
                ownerXid));
  }

  /** Claims every idle partition's head workflow in one transaction; see {@link QueuesDAO}. */
  public List<String> startQueuedPartitionedWorkflows(
      Queue queue, String executorId, String appVersion, long maxTasks, String ownerXid) {
    return dbOnce(
        () ->
            QueuesDAO.startQueuedPartitionedWorkflows(
                ctx,
                queue,
                executorId,
                appVersion,
                maxTasks,
                QueuesDAO.PARTITIONED_DEQUEUE_SWEEP_CAP,
                ownerXid));
  }

  /** See {@link WorkflowDAO#checkChildWorkflow}. */
  public Optional<StepResult> checkChildWorkflow(String workflowUuid, int functionId) {
    return dbRetry(() -> WorkflowDAO.checkChildWorkflow(ctx, workflowUuid, functionId));
  }

  /**
   * Sends messages from {@code caller}, or from outside any workflow when it is null. A send from
   * inside a step records nothing of its own, but its caller is still checked, after the messages
   * are written and before they commit: a step whose execution has lost its workflow sends nothing.
   */
  public void sendBulk(
      List<SendMessage> messages,
      @Nullable SendCaller caller,
      String functionName,
      boolean sendToForks,
      String serialization) {
    dbRetryIncludingSerializationError(
        "sendBulk",
        () ->
            NotificationsDAO.sendBulk(
                ctx, messages, caller, functionName, sendToForks, serialization));
  }

  public Object recv(
      String workflowId,
      int stepId,
      int timeoutStepId,
      String topic,
      Duration timeout,
      String ownerXid) {
    return dbRetry(
        () ->
            NotificationsDAO.recv(
                ctx,
                workflowId,
                stepId,
                timeout,
                timeoutStepId,
                topic,
                notificationRecheckInterval(),
                createSubscription,
                ownerXid));
  }

  public void setEvent(
      String workflowId,
      int functionId,
      String key,
      Object message,
      boolean asStep,
      String serialization,
      String ownerXid) {
    dbRetryIncludingSerializationError(
        "setEvent",
        () ->
            NotificationsDAO.setEvent(
                ctx, workflowId, functionId, key, message, asStep, serialization, ownerXid));
    signal(new SignalKey.Event(workflowId, key));
  }

  public Object getEvent(String targetId, String key, Duration timeout, GetEventCaller caller) {
    return dbRetry(
        () ->
            NotificationsDAO.getEvent(
                ctx,
                targetId,
                key,
                timeout,
                caller,
                notificationRecheckInterval(),
                createSubscription));
  }

  public void sleep(String workflowId, int functionId, Duration duration, String ownerXid) {
    dbRetryIncludingSerializationError(
        "sleep", () -> StepsDAO.sleep(ctx, workflowId, functionId, duration, ownerXid));
  }

  public void cancelWorkflows(List<String> workflowIds, boolean cancelChildren) {
    dbRetry(() -> WorkflowDAO.cancelWorkflows(ctx, workflowIds, cancelChildren));
  }

  /**
   * Cancels a workflow on behalf of the execution holding {@code ownerXid}, as its own deadline
   * does, and only while that execution still owns it: once the workflow has changed hands, the
   * stale execution's timer no longer reaches it. Returns whether it cancelled the workflow.
   */
  public boolean cancelOwnedWorkflow(String workflowId, String ownerXid) {
    return dbRetry(() -> WorkflowDAO.cancelOwnedWorkflow(ctx, workflowId, ownerXid));
  }

  public void resumeWorkflows(List<String> workflowIds, String queueName) {
    dbRetry(() -> WorkflowDAO.resumeWorkflows(ctx, workflowIds, queueName));
  }

  public void updateWorkflowAttributes(String workflowId, Map<String, Object> attributes) {
    var validated = Validation.validateAttributes(attributes);
    dbRetry(() -> WorkflowDAO.updateWorkflowAttributes(ctx, workflowId, validated));
  }

  public void deleteWorkflows(List<String> workflowIds, boolean deleteChildren) {
    dbRetryIncludingSerializationError(
        "deleteWorkflows", () -> WorkflowDAO.deleteWorkflows(ctx, workflowIds, deleteChildren));
  }

  public String forkWorkflow(String originalWorkflowId, int startStep, ForkOptions options) {
    return dbRetryIncludingSerializationError(
        "forkWorkflow",
        () -> WorkflowDAO.forkWorkflow(ctx, originalWorkflowId, startStep, options));
  }

  public void forkWorkflows(
      List<WorkflowFork> forks,
      @Nullable String applicationVersion,
      @Nullable String queueName,
      @Nullable String queuePartitionKey) {
    dbRetryIncludingSerializationError(
        "forkWorkflows",
        () -> {
          WorkflowDAO.forkWorkflows(
              ctx, forks, applicationVersion, queueName, queuePartitionKey, null);
          return null;
        });
  }

  public void rewindWorkflow(String workflowId, int startStep, RewindOptions options) {
    dbRetryIncludingSerializationError(
        "rewindWorkflow", () -> WorkflowDAO.rewindWorkflow(ctx, workflowId, startStep, options));
  }

  public void checkRewindable(String workflowId, int startStep) {
    dbRetry(() -> WorkflowDAO.checkRewindable(ctx, workflowId, startStep));
  }

  public void createApplicationVersion(String versionName, @Nullable String applicationName) {
    dbRetryIncludingSerializationError(
        "createApplicationVersion",
        () -> ApplicationVersionDAO.createApplicationVersion(ctx, versionName, applicationName));
  }

  public void updateApplicationVersionTimestamp(
      String versionName, Instant newTimestamp, @Nullable String applicationName) {
    dbRetry(
        () ->
            ApplicationVersionDAO.updateApplicationVersionTimestamp(
                ctx, versionName, newTimestamp, applicationName));
  }

  /**
   * Give {@code newName} ownership of the rows {@code oldName} holds, of unclaimed rows, or of
   * both. The renamed application must be stopped, or its dequeues race this.
   */
  public ApplicationRowCounts renameApplication(
      @Nullable String oldName,
      String newName,
      @Nullable Integer batchSize,
      boolean adoptUnclaimedRows) {
    // Deliberately not dbRetryIncludingSerializationError: this is a transaction followed by two
    // batched sweeps in their own transactions, so a replay would re-run an already-committed
    // move -- finding no rows left under the old name, and undercounting what it reports.
    return dbRetry(
        () ->
            ApplicationRenameDAO.renameApplication(
                ctx, oldName, newName, batchSize, adoptUnclaimedRows));
  }

  public List<VersionInfo> listApplicationVersions() {
    return dbRetry(() -> ApplicationVersionDAO.listApplicationVersions(ctx));
  }

  public VersionInfo getLatestApplicationVersion() {
    return dbRetry(() -> ApplicationVersionDAO.getLatestApplicationVersion(ctx));
  }

  /** Rows deleted per committed transaction. Matches Python's DEFAULT_GC_BATCH_SIZE. */
  public static final int DEFAULT_GC_BATCH_SIZE = 50_000;

  /** Enforces retention across the entire system database. */
  public void garbageCollect(Instant cutoff, Long rowsThreshold, int batchSize) {
    if (cutoff == null && rowsThreshold == null) {
      return;
    }
    dbRetry(() -> WorkflowDAO.runRetentionRound(ctx, cutoff, rowsThreshold, batchSize));
  }

  public void setWorkflowDelay(String workflowId, WorkflowDelay delay) {
    Objects.requireNonNull(delay, "delay must not be null");
    long delayUntilEpochMs;
    if (delay instanceof WorkflowDelay.Delay d) {
      delayUntilEpochMs = resolveDelayUntil(d.delay());
    } else if (delay instanceof WorkflowDelay.DelayUntil du) {
      // An absolute delay is the caller's instant, taken as given.
      delayUntilEpochMs = du.delayUntil().toEpochMilli();
    } else {
      throw new IllegalArgumentException("Unexpected WorkflowDelay value");
    }
    dbRetry(() -> WorkflowDAO.setWorkflowDelay(ctx, workflowId, delayUntilEpochMs));
  }

  /** See {@link WorkflowDAO#cancelTimedOutWorkflows}. */
  public List<String> cancelTimedOutWorkflows(int limit) {
    return dbRetry(() -> WorkflowDAO.cancelTimedOutWorkflows(ctx, limit));
  }

  public void transitionDelayedWorkflows() {
    dbRetry(() -> WorkflowDAO.transitionDelayedWorkflows(ctx));
  }

  public void createSchedule(WorkflowSchedule schedule) {
    dbRetry(() -> SchedulesDAO.createSchedule(ctx, schedule));
  }

  public Optional<WorkflowSchedule> getSchedule(String name) {
    return dbRetry(() -> SchedulesDAO.getSchedule(ctx, name));
  }

  public List<WorkflowSchedule> listSchedules(
      List<ScheduleStatus> statuses,
      List<String> workflowNames,
      List<String> scheduleNamePrefixes) {
    return listSchedules(statuses, workflowNames, scheduleNamePrefixes, null);
  }

  public List<WorkflowSchedule> listSchedules(
      List<ScheduleStatus> statuses,
      List<String> workflowNames,
      List<String> scheduleNamePrefixes,
      @Nullable List<String> applicationName) {
    return dbRetry(
        () ->
            SchedulesDAO.listSchedules(
                ctx, statuses, workflowNames, scheduleNamePrefixes, applicationName));
  }

  // Raw form of listSchedules used by the scheduler's poller; see ScheduleRecord for why.
  public List<ScheduleRecord> listScheduleRecords(
      List<ScheduleStatus> statuses,
      List<String> workflowNames,
      List<String> scheduleNamePrefixes) {
    return dbRetry(
        () -> SchedulesDAO.listScheduleRecords(ctx, statuses, workflowNames, scheduleNamePrefixes));
  }

  public @Nullable DBOSSerializer serializer() {
    return ctx.serializer();
  }

  public void pauseSchedule(String name) {
    dbRetry(() -> SchedulesDAO.pauseSchedule(ctx, name));
  }

  public void resumeSchedule(String name) {
    dbRetry(() -> SchedulesDAO.resumeSchedule(ctx, name));
  }

  public void updateScheduleLastFiredAt(String name, Instant lastFiredAt) {
    dbRetry(() -> SchedulesDAO.updateScheduleLastFiredAt(ctx, name, lastFiredAt));
  }

  public void deleteSchedule(String name) {
    dbRetry(() -> SchedulesDAO.deleteSchedule(ctx, name));
  }

  public void applySchedules(List<WorkflowSchedule> schedules) {
    dbRetryIncludingSerializationError(
        "applySchedules", () -> SchedulesDAO.applySchedules(ctx, schedules));
  }

  @SuppressWarnings("removal") // implements the deprecated ExternalState API
  public Optional<ExternalState> getExternalState(String service, String workflowName, String key) {
    return dbRetry(() -> ExternalStateDAO.getExternalState(ctx, service, workflowName, key));
  }

  @SuppressWarnings("removal") // implements the deprecated ExternalState API
  public ExternalState upsertExternalState(ExternalState state) {
    return dbRetry(() -> ExternalStateDAO.upsertExternalState(ctx, state));
  }

  public boolean patch(String workflowId, int functionId, String patchName, String ownerXid) {
    return dbRetryIncludingSerializationError(
        "patch", () -> StepsDAO.patch(ctx, workflowId, functionId, patchName, ownerXid));
  }

  public boolean deprecatePatch(String workflowId, int functionId, String patchName) {
    return dbRetry(() -> StepsDAO.deprecatePatch(ctx, workflowId, functionId, patchName));
  }

  public Set<String> getWorkflowChildren(String workflowId) {
    return dbRetry(() -> WorkflowDAO.getWorkflowChildren(ctx, workflowId));
  }

  /**
   * This database as the Conductor client in {@code dev.dbos:transact-enterprise} sees it: its
   * connections and schema, and the retries this class's own calls use.
   */
  public ConductorSystemDatabase forConductor() {
    return new ConductorSystemDatabase() {
      @Override
      public DataSource dataSource() {
        return ctx.dataSource();
      }

      @Override
      public String schema() {
        return ctx.schema();
      }

      @Override
      public Set<String> workflowChildren(String workflowId) throws SQLException {
        return WorkflowDAO.getWorkflowChildren(ctx, workflowId);
      }

      @Override
      public @Nullable Object deserialize(@Nullable String value, @Nullable String serialization) {
        return SerializationUtil.deserializeValue(value, serialization, ctx.serializer());
      }

      @Override
      public <T> T retry(SqlWork<T> work) {
        return dbRetry(work::run);
      }

      @Override
      public <T> T retryIncludingSerializationError(String operation, SqlWork<T> work) {
        return dbRetryIncludingSerializationError(operation, work::run);
      }
    };
  }

  public Map<String, Object> getAllEvents(String workflowId) {
    return dbRetry(() -> WorkflowDAO.getAllEvents(ctx, workflowId));
  }

  public void writeStreamFromStep(
      String workflowId,
      int functionId,
      String key,
      Object value,
      String serializationFormat,
      String ownerXid) {
    dbRetryIncludingSerializationError(
        "writeStreamFromStep",
        () ->
            StreamsDAO.writeStreamFromStep(
                ctx, workflowId, functionId, key, value, serializationFormat, ownerXid));
    signal(new SignalKey.Stream(workflowId, key));
  }

  public void writeStreamFromWorkflow(
      String workflowId,
      int functionId,
      String key,
      Object value,
      String serializationFormat,
      String ownerXid) {
    dbRetryIncludingSerializationError(
        "writeStreamFromWorkflow",
        () ->
            StreamsDAO.writeStreamFromWorkflow(
                ctx, workflowId, functionId, key, value, serializationFormat, ownerXid));
    signal(new SignalKey.Stream(workflowId, key));
  }

  public void closeStream(String workflowId, int functionId, String key, String ownerXid) {
    dbRetryIncludingSerializationError(
        "closeStream", () -> StreamsDAO.closeStream(ctx, workflowId, functionId, key, ownerXid));
    // Closing writes the sentinel entry, so readers need the same wake-up as any other write.
    signal(new SignalKey.Stream(workflowId, key));
  }

  /**
   * Wake everyone waiting for a value on {@code key}: the waiters in this process directly, with no
   * round trip, and those in other processes through the notifier's next batch.
   *
   * <p>Called only after the write has committed. Signalling before would let a woken waiter
   * re-read ahead of the row becoming visible and go back to sleep until its next poll.
   */
  private void signal(SignalKey key) {
    signalMap.raiseSignal(key.toString());
    notificationSource.push(key.channel(), key.payload());
  }

  public Object readStream(String workflowId, String key, int offset) {
    return dbRetry(
        () ->
            StreamsDAO.readStream(
                ctx, workflowId, key, offset, DB_POLLING_INTERVAL, createSubscription));
  }
}
