package dev.dbos.transact.migrations;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DbSetupTestBase;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
class MigrationManagerTest extends DbSetupTestBase {

  private DataSource testDataSource;

  @BeforeEach
  void setup() throws Exception {
    dbosConfig =
        DBOSConfig.defaultsFromEnv("migrationtest")
            .withDatabaseUrl(postgres.getJdbcUrl())
            .withDbUser(postgres.getUsername())
            .withDbPassword(postgres.getPassword())
            .withMaximumPoolSize(2);

    DBUtils.recreateDB(dbosConfig);
    testDataSource = SystemDatabase.createDataSource(dbosConfig);
  }

  @AfterEach
  void cleanup() throws Exception {
    ((HikariDataSource) testDataSource).close();
  }

  @Test
  void testRunMigrations_CreatesTables() throws Exception {

    MigrationManager.runMigrations(dbosConfig);

    // Assert
    try (Connection conn = testDataSource.getConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();

      // Verify all expected tables exist in the dbos schema
      assertTableExists(metaData, "operation_outputs");
      assertTableExists(metaData, "workflow_status");
      assertTableExists(metaData, "notifications");
      assertTableExists(metaData, "workflow_events");

      var migrations = new ArrayList<>(MigrationManager.getMigrations(Constants.DB_SCHEMA));
      var version = getVersion(conn);
      assertEquals(migrations.size(), version);
    }
  }

  public static void assertTableExists(DatabaseMetaData metaData, String tableName)
      throws Exception {
    assertTableExists(metaData, tableName, Constants.DB_SCHEMA);
  }

  public static void assertTableExists(
      DatabaseMetaData metaData, String tableName, String schemaName) throws Exception {
    try (ResultSet rs = metaData.getTables(null, schemaName, tableName, null)) {
      assertTrue(rs.next(), "Table %s should exist in schema %s".formatted(tableName, schemaName));
    }
  }

  public static int getVersion(Connection conn) throws Exception {
    return getVersion(conn, Constants.DB_SCHEMA);
  }

  public static int getVersion(Connection conn, String schema) throws Exception {
    schema = SystemDatabase.sanitizeSchema(schema);
    String sql = "SELECT version FROM %s.dbos_migrations".formatted(schema);
    try (var stmt = conn.createStatement();
        var rs = stmt.executeQuery(sql)) {
      assertTrue(rs.next());
      var value = rs.getInt("version");
      assertFalse(rs.next());
      return value;
    }
  }

  @Test
  void testRunMigrations_customSchema() throws Exception {

    String schema = "C\"$+0m'";
    dbosConfig = dbosConfig.withDatabaseSchema(schema);
    MigrationManager.runMigrations(dbosConfig);

    // Assert
    try (Connection conn = testDataSource.getConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();

      // Verify all expected tables exist in the dbos schema
      assertTableExists(metaData, "operation_outputs", schema);
      assertTableExists(metaData, "workflow_status", schema);
      assertTableExists(metaData, "notifications", schema);
      assertTableExists(metaData, "workflow_events", schema);

      var migrations = new ArrayList<>(MigrationManager.getMigrations(schema));
      var version = getVersion(conn, schema);
      assertEquals(migrations.size(), version);
    }
  }

  @Test
  void testRunMigrations_IsIdempotent() throws Exception {

    testRunMigrations_CreatesTables();

    // Running migrations again
    assertDoesNotThrow(
        () -> {
          MigrationManager.runMigrations(dbosConfig);
        },
        "Migrations should run successfully multiple times");
  }

  @Test
  void testAddingNewMigration() throws Exception {
    testRunMigrations_CreatesTables();

    var migrations = new ArrayList<>(MigrationManager.getMigrations(Constants.DB_SCHEMA));
    migrations.add("CREATE TABLE dummy_table(id SERIAL PRIMARY KEY);");

    try (var conn = testDataSource.getConnection()) {
      MigrationManager.runDbosMigrations(conn, Constants.DB_SCHEMA, migrations);
    }

    // Validate the dummy_table was created
    try (Connection conn = testDataSource.getConnection();
        ResultSet rs = conn.getMetaData().getTables(null, null, "dummy_table", null)) {
      Assertions.assertTrue(rs.next(), "Expected 'dummy_table' to exist after new migration.");
    }
  }

  @Test
  public void extractDbAndPostgresUrl() {
    var originalUrl = "jdbc:postgresql://localhost:5432/dbos_java_sys?user=alice&ssl=true";
    var pair = MigrationManager.extractDbAndPostgresUrl(originalUrl);

    assertEquals("dbos_java_sys", pair.database());
    assertEquals("jdbc:postgresql://localhost:5432/postgres?user=alice&ssl=true", pair.url());
  }
}
