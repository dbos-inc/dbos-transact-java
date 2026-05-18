package dev.dbos.transact.utils;

import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.migrations.MigrationManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.cockroachdb.CockroachContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

// TODO: custom junit.jupiter.execution.parallel.config.strategy / dynamic.factor /
// fixed.parallelism = 2 reader

public class PgContainer implements AutoCloseable {

  public static final boolean USE_COCKROACH_DB =
      Boolean.parseBoolean(System.getenv("DBOS_TEST_USE_COCKROACH_DB"));
  private static final String DB_NAME = "dbos_test_db";

  private static final Queue<JdbcDatabaseContainer<?>> POOL = new ConcurrentLinkedQueue<>();

  public static PostgreSQLContainer getPG() {
    return new PostgreSQLContainer("postgres:latest");
  }

  public static CockroachContainer getCRDB() {
    return new CockroachContainer("cockroachdb/cockroach:latest");
  }

  private static JdbcDatabaseContainer<?> containerSupplier() {
    var container = USE_COCKROACH_DB ? getCRDB() : getPG();
    container.start();
    return container;
  }

  static JdbcDatabaseContainer<?> acquire() {
    var container = POOL.poll();
    if (container != null) {
      var jdbcUrl = container.getJdbcUrl().replaceFirst("/[^/]+$", "/" + DB_NAME);
      try (var conn =
          DriverManager.getConnection(jdbcUrl, container.getUsername(), container.getPassword())) {
        truncateDbosTables(conn);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      return container;
    }
    container = containerSupplier();
    var jdbcUrl = container.getJdbcUrl().replaceFirst("/[^/]+$", "/" + DB_NAME);

    MigrationManager.runMigrations(
        jdbcUrl, container.getUsername(), container.getPassword(), "dbos", true);
    return container;
  }

  static void release(JdbcDatabaseContainer<?> c) {
    POOL.offer(c);
  }

  public static void truncateDbosTables(Connection conn) throws SQLException {
    // truncate the DBOS tables from the test DB before returning to the pool
    var truncate =
        """
        TRUNCATE TABLE
          "dbos".workflow_status,
          "dbos".operation_outputs,
          "dbos".workflow_events,
          "dbos".workflow_events_history,
          "dbos".notifications,
          "dbos".event_dispatch_kv,
          "dbos".streams,
          "dbos".application_versions,
          "dbos".workflow_schedules
        CASCADE
        """;
    try (var stmt = conn.createStatement()) {
      stmt.execute(truncate);
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
    pgContainer = pooled ? acquire() : containerSupplier();
    jdbcUrl = pgContainer.getJdbcUrl().replaceFirst("/[^/]+$", "/" + DB_NAME);
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

  public void createDatabase() {
    MigrationManager.createDatabaseIfNotExists(jdbcUrl(), username(), password());
  }
}
