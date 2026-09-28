package dev.dbos.transact.utils;

import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.migrations.MigrationManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.cockroachdb.CockroachContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

public class PgContainer implements AutoCloseable {

  public static final boolean USE_COCKROACH_DB =
      Boolean.parseBoolean(System.getenv("DBOS_TEST_USE_COCKROACH_DB"));

  /** The database a prebaked image ships, already migrated. */
  private static final String POOLED_DB_NAME = "dbos_test_0";

  /** What a from-empty container uses, since it has to create its own database anyway. */
  private static final String FRESH_DB_NAME = "dbos_test_db";

  /** Where a prebaked CockroachDB image keeps its store, which is not the default location. */
  private static final String CRDB_STORE = "/cockroach/prebuilt";

  private static final Queue<JdbcDatabaseContainer<?>> POOL = new ConcurrentLinkedQueue<>();

  /**
   * How long {@link #acquire()} waits for a pooled container's previous user to disconnect. Closing
   * a test's remaining resources takes about a second at most, so running out means something
   * leaked.
   */
  private static final Duration IDLE_TIMEOUT = Duration.ofSeconds(30);

  /** SQLSTATE 53300, "sorry, too many clients already". */
  private static final String TOO_MANY_CONNECTIONS = "53300";

  private static final Logger logger = LoggerFactory.getLogger(PgContainer.class);

  private static String image(String variable, String fallback) {
    var value = System.getenv(variable);
    return value == null || value.isBlank() ? fallback : value;
  }

  // Pinned to a migration version rather than floating, so a migration landing in dbos-ctl cannot
  // change what this suite runs against without a commit here saying so.
  private static final String PG_IMAGE = image("DBOS_TEST_POSTGRES_IMAGE", "postgres:18");
  private static final String CRDB_IMAGE =
      image("DBOS_TEST_COCKROACH_IMAGE", "cockroachdb/cockroach:latest-v26.2");
  private static final String PG_PREBAKED_IMAGE =
      image("DBOS_TEST_POSTGRES_PREBAKED_IMAGE", "ghcr.io/dbos-inc/dbos-test-postgres:18-m108");
  private static final String CRDB_PREBAKED_IMAGE =
      image("DBOS_TEST_COCKROACH_PREBAKED_IMAGE", "ghcr.io/dbos-inc/dbos-test-cockroach:26.2-m108");

  /** A stock PostgreSQL server with no DBOS schema. */
  public static PostgreSQLContainer getPG() {
    return new PostgreSQLContainer(PG_IMAGE);
  }

  /** A stock CockroachDB server with no DBOS schema. */
  public static CockroachContainer getCRDB() {
    return new CockroachContainer(CRDB_IMAGE);
  }

  /**
   * A PostgreSQL server whose DBOS schema is already migrated.
   *
   * <p>The wait strategy has to be replaced rather than inherited. The stock one waits for
   * "database system is ready to accept connections" <i>twice</i>, because a container that
   * initialises itself starts the server once to run initdb and again to serve. This image arrives
   * initialised, so the entrypoint skips that first pass and the message appears once -- against
   * the stock strategy every container would sit until the startup timeout and then fail, having
   * been ready the whole time.
   *
   * <p>The user has to be {@code postgres} for the same reason: {@code POSTGRES_USER} only takes
   * effect during initdb, so an image that skips it has whatever roles it was built with.
   */
  // The container is returned to be owned and closed by a PgContainer; the IDE cannot follow a
  // closeable through the builder chain and reports it leaked.
  @SuppressWarnings("resource")
  private static PostgreSQLContainer prebakedPG() {
    return new PostgreSQLContainer(
            DockerImageName.parse(PG_PREBAKED_IMAGE).asCompatibleSubstituteFor("postgres"))
        .withDatabaseName(POOLED_DB_NAME)
        .withUsername("postgres")
        .withPassword("dbos")
        .waitingFor(
            Wait.forLogMessage(".*database system is ready to accept connections.*\\s", 1)
                .withStartupTimeout(Duration.ofMinutes(2)));
  }

  /**
   * A CockroachDB server whose DBOS schema is already migrated.
   *
   * <p>The command has to name the store: a prebaked image keeps its baked data outside the default
   * location precisely so a container that forgets to ask starts an empty node rather than silently
   * appearing to work, and the stock command does not ask.
   *
   * <p>No password is set, and that is load-bearing. {@code CockroachContainer.configure()}
   * replaces the command with a bare {@code start-single-node} when a password is present, which
   * would drop both the store path and {@code --insecure}.
   */
  // The container is returned to be owned and closed by a PgContainer; the IDE cannot follow a
  // closeable through the builder chain and reports it leaked.
  @SuppressWarnings("resource")
  private static CockroachContainer prebakedCRDB() {
    return new CockroachContainer(
            DockerImageName.parse(CRDB_PREBAKED_IMAGE)
                .asCompatibleSubstituteFor("cockroachdb/cockroach"))
        .withDatabaseName(POOLED_DB_NAME)
        .withCommand("start-single-node", "--insecure", "--store=path=" + CRDB_STORE);
  }

  private static JdbcDatabaseContainer<?> containerSupplier(boolean prebaked) {
    JdbcDatabaseContainer<?> container;
    if (prebaked) {
      container = USE_COCKROACH_DB ? prebakedCRDB() : prebakedPG();
    } else {
      container = USE_COCKROACH_DB ? getCRDB() : getPG();
    }
    container.start();
    return container;
  }

  static JdbcDatabaseContainer<?> acquire() {
    JdbcDatabaseContainer<?> container;
    while ((container = POOL.poll()) != null) {
      var jdbcUrl = container.getJdbcUrl().replaceFirst("/[^/]+$", "/" + POOLED_DB_NAME);
      try (var conn = connectOnceIdle(container, jdbcUrl)) {
        if (conn == null) {
          // Whatever is still connected is not going to let go, and handing the container on
          // would put two tests on one database. It is off the pool and nothing else will close
          // it, so stop it rather than leave it running, and try the next one.
          logger.warn(
              "Discarding pooled container {}: its previous user was still connected after {}",
              container.getContainerId(),
              IDLE_TIMEOUT);
          closeQuietly(container);
          continue;
        }
        resetDbosTables(conn);
      } catch (SQLException e) {
        // The container came out of the pool, so nothing else is holding it: dropping it here
        // without closing it would leave it running and unreachable for the rest of the run.
        closeQuietly(container);
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        // Nothing is wrong with the container, and the next acquire() waits and resets it anyway.
        release(container);
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
      return container;
    }
    var fresh = containerSupplier(true);
    var jdbcUrl = fresh.getJdbcUrl().replaceFirst("/[^/]+$", "/" + POOLED_DB_NAME);

    try {
      // The image arrives migrated, so this is normally a no-op -- every SDK gates on
      // `current < latest`. It is kept rather than deleted because it is what makes the pinned
      // image tag a performance decision rather than a correctness one: if this build's migrations
      // run ahead of the image, this applies the tail instead of failing.
      MigrationManager.runMigrations(
          jdbcUrl, fresh.getUsername(), fresh.getPassword(), "dbos", true);
    } catch (RuntimeException e) {
      // A container that fails to prepare has been started but never handed to anyone, and the
      // caller is about to see an exception rather than an AutoCloseable. Left alone it stays up
      // for the rest of the run, and since the next test finds the pool empty and starts another,
      // one broken image turns into as many orphaned containers as there are tests. That is how a
      // wrong pg_hba.conf in a prebaked image produced 181 of them.
      closeQuietly(fresh);
      throw e;
    }
    return fresh;
  }

  private static void closeQuietly(JdbcDatabaseContainer<?> container) {
    try {
      container.close();
    } catch (RuntimeException suppressed) {
      // Reporting why the database could not be prepared matters more than reporting that its
      // container also would not stop.
    }
  }

  /**
   * Connects to a pooled container once nothing else is connected to it, or returns null if that
   * has not happened within {@link #IDLE_TIMEOUT}.
   *
   * <p>A container goes back to the pool when its {@code PgContainer} closes, and nothing orders
   * that after the DBOS instances, clients and data sources the test built against it. JUnit closes
   * a class's {@code @AutoClose} fields in declaration order, and the suite declares the {@code
   * PgContainer} first, so the container is normally back in the pool while its previous test still
   * holds a few dozen connections. Handed on in that state, it can hit {@code max_connections}
   * before the new test has opened anything (#547). And since every test on a container shares one
   * database, the old instance would also still be polling the tables just reset for the new one.
   *
   * <p>Waiting here, where the container is reused, is what makes this hold however a test orders
   * or closes its resources. The wait is the time the rest of the previous test's fields take to
   * close, normally well under a second.
   *
   * <p>A saturated server refuses this connection too, so "too many clients" counts as not idle yet
   * rather than as a failure.
   */
  private static Connection connectOnceIdle(JdbcDatabaseContainer<?> container, String jdbcUrl)
      throws SQLException, InterruptedException {
    var deadline = System.nanoTime() + IDLE_TIMEOUT.toNanos();
    Connection conn = null;
    try {
      while (true) {
        if (conn == null) {
          try {
            conn =
                DriverManager.getConnection(
                    jdbcUrl, container.getUsername(), container.getPassword());
          } catch (SQLException e) {
            if (!TOO_MANY_CONNECTIONS.equals(e.getSQLState())) {
              throw e;
            }
          }
        }
        if (conn != null && otherSessions(conn) == 0) {
          var idle = conn;
          conn = null;
          return idle;
        }
        if (System.nanoTime() - deadline > 0) {
          return null;
        }
        Thread.sleep(50);
      }
    } finally {
      if (conn != null) {
        conn.close();
      }
    }
  }

  /**
   * Counts the client sessions on the server other than {@code conn}'s own, across every database,
   * since they all draw on the same connection limit.
   *
   * <p>CockroachDB accepts {@code pg_stat_activity} but always returns it empty, so there the
   * sessions come from {@code crdb_internal}, which lists client sessions only.
   */
  private static int otherSessions(Connection conn) throws SQLException {
    var sql =
        USE_COCKROACH_DB
            ? """
              SELECT count(*) FROM crdb_internal.cluster_sessions
              WHERE status <> 'CLOSED' AND session_id <> (SELECT session_id FROM [SHOW session_id])
              """
            : """
              SELECT count(*) FROM pg_stat_activity
              WHERE backend_type = 'client backend' AND pid <> pg_backend_pid()
              """;
    try (var stmt = conn.createStatement();
        var rs = stmt.executeQuery(sql)) {
      rs.next();
      return rs.getInt(1);
    }
  }

  /**
   * Returns a container to the pool. Its previous user may still be connected: {@link #acquire()}
   * waits for that before handing it on, so callers need not close anything first.
   */
  static void release(JdbcDatabaseContainer<?> c) {
    POOL.offer(c);
  }

  /**
   * Empties every DBOS table, leaving the schema in place.
   *
   * <p>{@code DELETE}, not {@code TRUNCATE}: CockroachDB implements {@code TRUNCATE} as a schema
   * change, so it prices like {@code CREATE INDEX} however few rows a table holds. Measured against
   * this schema it costs 1.16s where the equivalent deletes cost 0.05s, and a pooled container is
   * reset once per test -- roughly 900 times a run. {@code TRUNCATE} would win only once a table is
   * big enough for row count to dominate, which no test fixture is.
   *
   * <p>The table list comes from the catalogue rather than a hard-coded list, so a migration that
   * adds a table cannot silently leave it uncleaned. Deleting from all of them in one statement
   * batch is safe in any order -- emptying everything cannot strand a foreign key.
   */
  public static void resetDbosTables(Connection conn) throws SQLException {
    var tables = new ArrayList<String>();
    try (var stmt = conn.createStatement();
        var rs =
            stmt.executeQuery(
                """
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'dbos' AND table_name <> 'dbos_migrations'
                ORDER BY table_name
                """)) {
      while (rs.next()) {
        tables.add(rs.getString(1));
      }
    }
    if (tables.isEmpty()) {
      return;
    }
    var deletes = new StringBuilder();
    for (var table : tables) {
      deletes.append("DELETE FROM \"dbos\".\"").append(table).append("\";");
    }
    try (var stmt = conn.createStatement()) {
      stmt.execute(deletes.toString());
    }
  }

  private final JdbcDatabaseContainer<?> pgContainer;
  private final String jdbcUrl;
  private final boolean pooled;

  public PgContainer() {
    this(false);
  }

  private PgContainer(boolean requireFresh) {
    pooled = !requireFresh;
    pgContainer = pooled ? acquire() : containerSupplier(false);
    var database = pooled ? POOLED_DB_NAME : FRESH_DB_NAME;
    jdbcUrl = pgContainer.getJdbcUrl().replaceFirst("/[^/]+$", "/" + database);
  }

  public static PgContainer createFresh() {
    return new PgContainer(true);
  }

  @Override
  public void close() throws Exception {
    if (pooled) {
      release(pgContainer);
    } else {
      pgContainer.close();
    }
  }

  public String jdbcUrl() {
    return jdbcUrl;
  }

  public String username() {
    return pgContainer.getUsername();
  }

  public String password() {
    return pgContainer.getPassword();
  }

  public DBOSConfig dbosConfig() {
    return dbosConfig(null);
  }

  public DBOSConfig dbosConfig(String appName) {
    return DBOSConfig.defaults(Objects.requireNonNullElse(appName, "transact-java-test"))
        .withDatabaseUrl(jdbcUrl())
        .withDbUser(username())
        .withDbPassword(password());
  }

  public HikariDataSource dataSource() {
    return SystemDatabase.createDataSource(jdbcUrl(), username(), password());
  }

  public DBOSClient dbosClient() {
    return new DBOSClient(jdbcUrl(), username(), password());
  }

  /** A client acting for a named application, for tests where several share this database. */
  public DBOSClient dbosClient(String applicationName) {
    return new DBOSClient(jdbcUrl(), username(), password(), null, null, false, applicationName);
  }

  public void createDatabase() {
    MigrationManager.createDatabaseIfNotExists(jdbcUrl(), username(), password());
  }
}
