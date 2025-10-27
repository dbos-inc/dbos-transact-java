package dev.dbos.transact.migration;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;

@org.junit.jupiter.api.Timeout(value = 2, unit = TimeUnit.MINUTES)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class MigrationManagerTest {

  private static DataSource testDataSource;
  private static DBOSConfig dbosConfig;

  @BeforeAll
  static void setup() throws Exception {

    MigrationManagerTest.dbosConfig =
        DBOSConfig.defaultsFromEnv("migrationtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys_mm_test")
            .withMaximumPoolSize(3);

    DBUtils.recreateDB(MigrationManagerTest.dbosConfig);
    testDataSource = SystemDatabase.createDataSource(dbosConfig);
  }

  @AfterAll
  static void cleanup() throws Exception {
    ((HikariDataSource) testDataSource).close();
  }

  @Test
  @Order(1)
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
      try (var stmt = conn.createStatement();
          var rs = stmt.executeQuery("SELECT version FROM dbos.dbos_migrations")) {
        assertTrue(rs.next());
        assertEquals(migrations.size(), rs.getInt("version"));
        assertFalse(rs.next());
      }
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

  @Test
  @Order(2)
  void testRunMigrations_IsIdempotent() {
    // Running migrations again
    assertDoesNotThrow(
        () -> {
          MigrationManager.runMigrations(dbosConfig);
        },
        "Migrations should run successfully multiple times");
  }

  @Test
  @Order(4)
  void testAddingNewMigration() throws Exception {
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
