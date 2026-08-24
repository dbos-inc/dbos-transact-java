package dev.dbos.transact.utils;

import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.migrations.MigrationManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.zaxxer.hikari.HikariDataSource;
import org.testcontainers.cockroachdb.CockroachContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

public class PgContainer implements AutoCloseable {

  public static final boolean USE_COCKROACH_DB =
      Boolean.parseBoolean(System.getenv("DBOS_TEST_USE_COCKROACH_DB"));
  private static final String DB_NAME = "dbos_test_db";

  private static final Queue<JdbcDatabaseContainer<?>> POOL = new ConcurrentLinkedQueue<>();

  public static PostgreSQLContainer getPG() {
    return new PostgreSQLContainer("postgres:18");
  }

  public static CockroachContainer getCRDB() {
    return new CockroachContainer("cockroachdb/cockroach:latest-v26.2");
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
        resetDbosTables(conn);
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

  /**
   * Empties every DBOS table, leaving the schema in place.
   *
   * <p>{@code DELETE}, not {@code TRUNCATE}: CockroachDB implements {@code TRUNCATE} as a schema
   * change, so it prices like {@code CREATE INDEX} however few rows a table holds. Measured against
   * this schema it costs 1.16s where the equivalent deletes cost 0.05s — and a pooled container is
   * reset once per test, roughly 900 times a run. {@code TRUNCATE} would win only once a table is
   * big enough for row count to dominate, which no test fixture is.
   *
   * <p>The table list comes from the catalogue rather than a hard-coded list, so a migration that
   * adds a table cannot silently leave it uncleaned. Deleting from all of them in one statement
   * batch is safe in any order — emptying everything cannot strand a foreign key.
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
