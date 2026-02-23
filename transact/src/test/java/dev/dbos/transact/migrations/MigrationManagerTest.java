package dev.dbos.transact.migrations;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
class MigrationManagerTest {

  private HikariDataSource dataSource;
  private DBOSConfig dbosConfig;

  @BeforeEach
  void setup() throws Exception {

    dbosConfig =
        DBOSConfig.defaultsFromEnv("migrationtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys_mm_test");

    DBUtils.recreateDB(dbosConfig);
    dataSource = SystemDatabase.createDataSource(dbosConfig);
  }

  @AfterEach
  void cleanup() throws Exception {
    dataSource.close();
  }

  @Test
  void testRunMigrations_CreatesTables() throws Exception {

    MigrationManager.runMigrations(dbosConfig);

    // Assert
    try (Connection conn = dataSource.getConnection()) {
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
    String sql = "SELECT version FROM \"%s\".dbos_migrations".formatted(schema);
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

    var schema = "F8nny_sCHem@-n@m3";
    dbosConfig = dbosConfig.withDatabaseSchema(schema);
    MigrationManager.runMigrations(dbosConfig);

    // Assert
    try (Connection conn = dataSource.getConnection()) {
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

    try (var conn = dataSource.getConnection()) {
      MigrationManager.runDbosMigrations(conn, Constants.DB_SCHEMA, migrations);
    }

    // Validate the dummy_table was created
    try (Connection conn = dataSource.getConnection();
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

  @Test
  void testOriginalMigration1ThenAllMigrations_NotificationsPrimaryKey() throws Exception {
    try (Connection conn = dataSource.getConnection()) {
      // Ensure schema and migration table exist
      MigrationManager.ensureDbosSchema(conn, Constants.DB_SCHEMA);
      MigrationManager.ensureMigrationTable(conn, Constants.DB_SCHEMA);

      // Run only the original migration1 (before primary key was added) to populate database with
      // initial structure
      var originalMigration1 = getOriginalMigration1().formatted(Constants.DB_SCHEMA);
      try (var stmt = conn.createStatement()) {
        stmt.execute(originalMigration1);
      }

      // Update migration version to 1
      var insertSql =
          "INSERT INTO \"%s\".dbos_migrations (version) VALUES (1)".formatted(Constants.DB_SCHEMA);
      try (var stmt = conn.prepareStatement(insertSql)) {
        stmt.executeUpdate();
      }

      // Verify notifications table was created
      DatabaseMetaData metaData = conn.getMetaData();
      assertTableExists(metaData, "notifications");

      // Now run all current migrations (including migration10 which ensures primary key)
      var allMigrations = MigrationManager.getMigrations(Constants.DB_SCHEMA);
      MigrationManager.runDbosMigrations(conn, Constants.DB_SCHEMA, allMigrations);

      // Verify that the notifications table has a primary key
      assertNotificationTableHasPrimaryKey(metaData, "notifications", Constants.DB_SCHEMA);

      // Verify all migrations were applied
      var finalVersion = getVersion(conn);
      assertEquals(allMigrations.size(), finalVersion);
    }
  }

  private static void assertNotificationTableHasPrimaryKey(
      DatabaseMetaData metaData, String tableName, String schemaName) throws Exception {
    try (ResultSet rs = metaData.getPrimaryKeys(null, schemaName, tableName)) {
      assertTrue(
          rs.next(),
          "Table %s should have a primary key in schema %s".formatted(tableName, schemaName));
      assertEquals(
          "message_uuid",
          rs.getString("COLUMN_NAME"),
          "Primary key should be on message_uuid column");
    }
  }

  /**
   * Returns the original migration1 before primary key was added to notifications table. This
   * represents the state before migration10 was introduced to defensively add the primary key.
   */
  private static String getOriginalMigration1() {
    return """
      CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

      CREATE TABLE "%1$s".workflow_status (
          workflow_uuid TEXT PRIMARY KEY,
          status TEXT,
          name TEXT,
          authenticated_user TEXT,
          assumed_role TEXT,
          authenticated_roles TEXT,
          request TEXT,
          output TEXT,
          error TEXT,
          executor_id TEXT,
          created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000::numeric)::bigint,
          updated_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000::numeric)::bigint,
          application_version TEXT,
          application_id TEXT,
          class_name VARCHAR(255) DEFAULT NULL,
          config_name VARCHAR(255) DEFAULT NULL,
          recovery_attempts BIGINT DEFAULT 0,
          queue_name TEXT,
          workflow_timeout_ms BIGINT,
          workflow_deadline_epoch_ms BIGINT,
          inputs TEXT,
          started_at_epoch_ms BIGINT,
          deduplication_id TEXT,
          priority INT4 NOT NULL DEFAULT 0
      );

      CREATE INDEX workflow_status_created_at_index ON "%1$s".workflow_status (created_at);
      CREATE INDEX workflow_status_executor_id_index ON "%1$s".workflow_status (executor_id);
      CREATE INDEX workflow_status_status_index ON "%1$s".workflow_status (status);

      ALTER TABLE "%1$s".workflow_status
      ADD CONSTRAINT uq_workflow_status_queue_name_dedup_id
      UNIQUE (queue_name, deduplication_id);

      CREATE TABLE "%1$s".operation_outputs (
          workflow_uuid TEXT NOT NULL,
          function_id INT4 NOT NULL,
          function_name TEXT NOT NULL DEFAULT '',
          output TEXT,
          error TEXT,
          child_workflow_id TEXT,
          PRIMARY KEY (workflow_uuid, function_id),
          FOREIGN KEY (workflow_uuid) REFERENCES "%1$s".workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );

      CREATE TABLE "%1$s".notifications (
          message_uuid TEXT NOT NULL DEFAULT gen_random_uuid(),
          destination_uuid TEXT NOT NULL,
          topic TEXT,
          message TEXT NOT NULL,
          created_at_epoch_ms BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000::numeric)::bigint,
          FOREIGN KEY (destination_uuid) REFERENCES "%1$s".workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );
      CREATE INDEX idx_workflow_topic ON "%1$s".notifications (destination_uuid, topic);

      -- Create notification function
      CREATE OR REPLACE FUNCTION "%1$s".notifications_function() RETURNS TRIGGER AS $$
      DECLARE
          payload text := NEW.destination_uuid || '::' || NEW.topic;
      BEGIN
          PERFORM pg_notify('dbos_notifications_channel', payload);
          RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      -- Create notification trigger
      CREATE TRIGGER dbos_notifications_trigger
      AFTER INSERT ON "%1$s".notifications
      FOR EACH ROW EXECUTE FUNCTION "%1$s".notifications_function();

      CREATE TABLE "%1$s".workflow_events (
          workflow_uuid TEXT NOT NULL,
          key TEXT NOT NULL,
          value TEXT NOT NULL,
          PRIMARY KEY (workflow_uuid, key),
          FOREIGN KEY (workflow_uuid) REFERENCES "%1$s".workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );

      -- Create events function
      CREATE OR REPLACE FUNCTION "%1$s".workflow_events_function() RETURNS TRIGGER AS $$
      DECLARE
          payload text := NEW.workflow_uuid || '::' || NEW.key;
      BEGIN
          PERFORM pg_notify('dbos_workflow_events_channel', payload);
          RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      -- Create events trigger
      CREATE TRIGGER dbos_workflow_events_trigger
      AFTER INSERT ON "%1$s".workflow_events
      FOR EACH ROW EXECUTE FUNCTION "%1$s".workflow_events_function();

      CREATE TABLE "%1$s".streams (
          workflow_uuid TEXT NOT NULL,
          key TEXT NOT NULL,
          value TEXT NOT NULL,
          "offset" INT4 NOT NULL,
          PRIMARY KEY (workflow_uuid, key, "offset"),
          FOREIGN KEY (workflow_uuid) REFERENCES "%1$s".workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );

      CREATE TABLE "%1$s".event_dispatch_kv (
          service_name TEXT NOT NULL,
          workflow_fn_name TEXT NOT NULL,
          key TEXT NOT NULL,
          value TEXT,
          update_seq NUMERIC(38,0),
          update_time NUMERIC(38,15),
          PRIMARY KEY (service_name, workflow_fn_name, key)
      );
      """;
  }
}
