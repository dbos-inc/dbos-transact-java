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
    var container = POOL.poll();
    if (container != null) {
      var jdbcUrl = container.getJdbcUrl().replaceFirst("/[^/]+$", "/" + POOLED_DB_NAME);
      try (var conn =
          DriverManager.getConnection(jdbcUrl, container.getUsername(), container.getPassword())) {
        resetDbosTables(conn);
      } catch (SQLException e) {
        // The container came out of the pool, so nothing else is holding it: dropping it here
        // without closing it would leave it running and unreachable for the rest of the run.
        closeQuietly(container);
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
    return new DBOSClient(jdbcUrl(), username(), password(), null, null, true, applicationName);
  }

  public void createDatabase() {
    MigrationManager.createDatabaseIfNotExists(jdbcUrl(), username(), password());
  }
}
