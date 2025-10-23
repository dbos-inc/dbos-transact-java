package dev.dbos.transact.migration;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
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
    }
  }

  private void assertTableExists(DatabaseMetaData metaData, String tableName) throws Exception {
    try (ResultSet rs = metaData.getTables(null, "dbos", tableName, null)) {
      assertTrue(rs.next(), "Table " + tableName + " should exist in schema dbos");
    }
  }

  @Test
  @Order(2)
  void testRunMigrations_IsIdempotent() {
    // Running migrations again
    assertDoesNotThrow(
        () -> {
          MigrationManager migrationManager = new MigrationManager(testDataSource);
          migrationManager.migrate();
        },
        "Migrations should run successfully multiple times");
  }

  @Test
  @Order(3)
  void testAddingNewMigration() throws Exception {
    // Create a new dummy migration file in test/resources/db/migrations
    URL testMigrations = getClass().getClassLoader().getResource("db/migrations");
    Assertions.assertNotNull(testMigrations, "Test migration path not found.");

    Path migrationDir = Paths.get(testMigrations.toURI());
    Path newMigration = migrationDir.resolve("999__create_dummy_table.sql");

    String sql = "CREATE TABLE IF NOT EXISTS dummy_table(id SERIAL PRIMARY KEY);";
    Files.writeString(newMigration, sql);

    // Run migrations again
    MigrationManager.runMigrations(dbosConfig);

    // Validate the dummy_table was created
    try (Connection conn = testDataSource.getConnection();
        ResultSet rs = conn.getMetaData().getTables(null, null, "dummy_table", null)) {
      Assertions.assertTrue(rs.next(), "Expected 'dummy_table' to exist after new migration.");
    }

    // Clean up test file
    Files.deleteIfExists(newMigration);
  }

  @AfterAll
  static void cleanup() throws Exception {
    ((HikariDataSource) testDataSource).close();
  }
}
