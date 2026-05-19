package dev.dbos.transact.migrations;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.PgContainer;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.concurrent.CopyOnWriteArrayList;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class MigrationManagerTest {

  // Expected tables after migrations
  static final String[] EXPECTED_TABLES = {
    "application_versions",
    "event_dispatch_kv",
    "notifications",
    "operation_outputs",
    "streams",
    "workflow_events_history",
    "workflow_events",
    "workflow_schedules",
    "workflow_status"
  };

  // Expected functions after migrations (always present)
  static final String[] EXPECTED_FUNCTIONS = {"enqueue_workflow", "send_message"};

  // Expected LISTEN/NOTIFY functions after migrations (PG only, absent on CRDB)
  static final String[] EXPECTED_NOTIFY_FUNCTIONS = {
    "notifications_function", "workflow_events_function"
  };

  // Expected LISTEN/NOTIFY triggers after migrations (PG only, absent on CRDB)
  static final String[] EXPECTED_NOTIFY_TRIGGERS = {
    "dbos_notifications_trigger", "dbos_workflow_events_trigger"
  };

  @AutoClose final PgContainer pgContainer = PgContainer.createFresh();
  @AutoClose HikariDataSource dataSource;

  @BeforeEach
  void setup() throws Exception {
    dataSource = pgContainer.dataSource();
  }

  @Test
  void testRunMigrations_CreatesTables() throws Exception {

    var dbosConfig = pgContainer.dbosConfig();
    MigrationManager.runMigrations(dbosConfig);

    // Assert
    try (Connection conn = dataSource.getConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();

      // Verify all expected tables exist in the dbos schema
      for (String table : EXPECTED_TABLES) {
        assertTableExists(metaData, table);
      }

      for (String function : EXPECTED_FUNCTIONS) {
        assertFunctionExists(metaData, function);
      }
      if (!PgContainer.USE_COCKROACH_DB) {
        for (String function : EXPECTED_NOTIFY_FUNCTIONS) {
          assertFunctionExists(metaData, function);
        }
        for (String trigger : EXPECTED_NOTIFY_TRIGGERS) {
          assertTriggerExists(conn, trigger);
        }
      }

      var migrations =
          new ArrayList<>(
              MigrationManager.getMigrations(
                  Constants.DB_SCHEMA, true, PgContainer.USE_COCKROACH_DB));
      var version = getVersion(conn);
      assertEquals(migrations.size(), version);
    }
  }

  @Test
  void testRunMigrations_NoNotify_OmitsTriggersAndFunctions() throws Exception {
    var dbosConfig = pgContainer.dbosConfig().withUseListenNotify(false);
    MigrationManager.runMigrations(dbosConfig);

    try (Connection conn = dataSource.getConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();

      // All tables should still exist
      for (String table : EXPECTED_TABLES) {
        assertTableExists(metaData, table);
      }

      // enqueue_workflow and send_message are unaffected
      assertFunctionExists(metaData, "enqueue_workflow");
      assertFunctionExists(metaData, "send_message");

      // LISTEN/NOTIFY functions and triggers must be absent
      assertFunctionAbsent(conn, "notifications_function");
      assertFunctionAbsent(conn, "workflow_events_function");
      assertTriggerAbsent(conn, "dbos_notifications_trigger");
      assertTriggerAbsent(conn, "dbos_workflow_events_trigger");
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"invalid\"schema", "invalid'schema"})
  void testRunMigrations_fails_invalid_schema(String invalidSchema) throws Exception {
    var dbosConfig = pgContainer.dbosConfig().withDatabaseSchema(invalidSchema);
    assertThrows(IllegalArgumentException.class, () -> MigrationManager.runMigrations(dbosConfig));
  }

  @ParameterizedTest
  @ValueSource(strings = {"F8nny_sCHem@-n@m3", "embedded\0null"})
  void testRunMigrations_customSchema(String schema) throws Exception {
    var dbosConfig = pgContainer.dbosConfig().withDatabaseSchema(schema);
    MigrationManager.runMigrations(dbosConfig);

    // Assert
    try (Connection conn = dataSource.getConnection()) {
      DatabaseMetaData metaData = conn.getMetaData();

      // Verify all expected tables exist in the custom schema
      for (String table : EXPECTED_TABLES) {
        assertTableExists(metaData, table, schema);
      }

      for (String function : EXPECTED_FUNCTIONS) {
        assertFunctionExists(metaData, function, schema);
      }
      if (!PgContainer.USE_COCKROACH_DB) {
        for (String function : EXPECTED_NOTIFY_FUNCTIONS) {
          assertFunctionExists(metaData, function, schema);
        }
        for (String trigger : EXPECTED_NOTIFY_TRIGGERS) {
          assertTriggerExists(conn, trigger, schema);
        }
      }

      var migrations =
          new ArrayList<>(
              MigrationManager.getMigrations(schema, true, PgContainer.USE_COCKROACH_DB));
      var version = getVersion(conn, schema);
      assertEquals(migrations.size(), version);
    }
  }

  @Test
  void testRunMigrations_CreatesDatabaseIfNotExists() throws Exception {
    var dbosConfig = pgContainer.dbosConfig();
    var pair = MigrationManager.extractDbAndPostgresUrl(pgContainer.jdbcUrl());

    // Verify the database does not exist before running migrations
    try (var conn =
        DriverManager.getConnection(pair.url(), pgContainer.username(), pgContainer.password())) {
      assertFalse(
          databaseExists(conn, pair.database()),
          "Database '%s' should not exist before runMigrations".formatted(pair.database()));
    }

    MigrationManager.runMigrations(dbosConfig);

    // Verify the database now exists after running migrations
    try (var conn =
        DriverManager.getConnection(pair.url(), pgContainer.username(), pgContainer.password())) {
      assertTrue(
          databaseExists(conn, pair.database()),
          "Database '%s' should exist after runMigrations".formatted(pair.database()));
    }
  }

  @Test
  void testRunMigrations_IsIdempotent() throws Exception {

    testRunMigrations_CreatesTables();

    var dbosConfig = pgContainer.dbosConfig();
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

    var migrations =
        new ArrayList<>(
            MigrationManager.getMigrations(
                Constants.DB_SCHEMA, true, PgContainer.USE_COCKROACH_DB));
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
  void testWayFutureVersion() throws Exception {
    testRunMigrations_CreatesTables();

    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.executeUpdate("UPDATE \"dbos\".\"dbos_migrations\" SET \"version\" = 10000;");
    }

    var dbosConfig = pgContainer.dbosConfig();
    assertDoesNotThrow(
        () -> {
          MigrationManager.runMigrations(dbosConfig);
        },
        "Migrations should run successfully multiple times");
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
    Assumptions.assumeFalse(PgContainer.USE_COCKROACH_DB, "PG-only migration history test");

    // need to create database since we are connecting
    // the data source prior to dbos launch
    pgContainer.createDatabase();

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
      var allMigrations =
          MigrationManager.getMigrations(Constants.DB_SCHEMA, true, PgContainer.USE_COCKROACH_DB);
      MigrationManager.runDbosMigrations(conn, Constants.DB_SCHEMA, allMigrations);

      // Verify that the notifications table has a primary key
      assertNotificationTableHasPrimaryKey(metaData, "notifications", Constants.DB_SCHEMA);

      // Verify all migrations were applied
      var finalVersion = getVersion(conn);
      assertEquals(allMigrations.size(), finalVersion);
    }
  }

  @Test
  void testConcurrentMigrations() throws Exception {
    var dbosConfig = pgContainer.dbosConfig();
    int numInstances = 10;

    var errors = new CopyOnWriteArrayList<String>();
    var threads = new ArrayList<Thread>();
    for (int i = 0; i < numInstances; i++) {
      final int idx = i;
      threads.add(
          new Thread(
              () -> {
                try {
                  MigrationManager.runMigrations(dbosConfig);
                } catch (Throwable e) {
                  errors.add(
                      "Instance %d: %s: %s"
                          .formatted(idx, e.getClass().getSimpleName(), e.getMessage()));
                }
              }));
    }
    for (var t : threads) t.start();
    for (var t : threads) t.join();

    assertTrue(
        errors.isEmpty(),
        "%d/%d concurrent migrations failed:\n%s"
            .formatted(errors.size(), numInstances, String.join("\n", errors)));

    try (var conn = dataSource.getConnection()) {
      var migrations =
          MigrationManager.getMigrations(Constants.DB_SCHEMA, true, PgContainer.USE_COCKROACH_DB);
      assertEquals(migrations.size(), getVersion(conn));
    }
  }

  @Test
  void testOnlineMigrationsAreIdempotent() throws Exception {
    Assumptions.assumeFalse(PgContainer.USE_COCKROACH_DB, "PG-only online migration test");

    var dbosConfig = pgContainer.dbosConfig();
    MigrationManager.runMigrations(dbosConfig);

    var schema = Constants.DB_SCHEMA;
    var migrations = MigrationManager.getMigrations(schema, true, false);
    // min(ONLINE_MIGRATIONS) == 22, so rewind to 21
    int rewindTo = 21;
    int expectedFinal = migrations.size();

    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.executeUpdate(
          "UPDATE \"%s\".dbos_migrations SET version = %d".formatted(schema, rewindTo));
    }

    assertDoesNotThrow(
        () -> MigrationManager.runMigrations(dbosConfig),
        "Re-running online migrations against an already-migrated schema must succeed");

    try (var conn = dataSource.getConnection()) {
      assertEquals(expectedFinal, getVersion(conn));
    }
  }

  @Test
  void testVersionNotBumpedOnMigrationFailure() throws Exception {
    Assumptions.assumeFalse(
        PgContainer.USE_COCKROACH_DB, "PG-only: tests the online migration code path");

    var dbosConfig = pgContainer.dbosConfig();
    MigrationManager.runMigrations(dbosConfig);

    var schema = Constants.DB_SCHEMA;
    int rewindTo = 31; // one before migration 32
    var allMigrations = MigrationManager.getMigrations(schema, true, false);
    int expectedFinal = allMigrations.size();

    // Rewind so migration 32 is pending again
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.executeUpdate(
          "UPDATE \"%s\".dbos_migrations SET version = %d".formatted(schema, rewindTo));
    }

    // Replace migration 32 (list index 31) with invalid SQL — must throw without bumping version
    var patchedMigrations = new ArrayList<>(allMigrations);
    patchedMigrations.set(31, "THIS IS NOT VALID SQL");

    try (var conn = dataSource.getConnection()) {
      assertThrows(
          RuntimeException.class,
          () -> MigrationManager.runDbosMigrations(conn, schema, patchedMigrations));
    }

    try (var conn = dataSource.getConnection()) {
      assertEquals(rewindTo, getVersion(conn));
    }

    // Re-run with real migrations: IF NOT EXISTS guards make 32+ idempotent given the index
    // still exists from the original full migration run above.
    try (var conn = dataSource.getConnection()) {
      MigrationManager.runDbosMigrations(conn, schema, allMigrations);
    }

    try (var conn = dataSource.getConnection()) {
      assertEquals(expectedFinal, getVersion(conn));
    }
  }

  @Test
  void testRunnerResumesAfterInvalidIndex() throws Exception {
    Assumptions.assumeFalse(PgContainer.USE_COCKROACH_DB, "PG-only: relies on pg_index.indisvalid");

    var dbosConfig = pgContainer.dbosConfig();
    MigrationManager.runMigrations(dbosConfig);

    var schema = Constants.DB_SCHEMA;
    String targetIndex = "idx_workflow_status_in_flight";
    int rewindTo = 31; // one before migration 32 which creates targetIndex
    int expectedFinal = MigrationManager.getMigrations(schema, true, false).size();

    // Drop the valid index, create a non-CONCURRENTLY copy with the same name, then mark it
    // INVALID — this mimics what Postgres leaves behind when CREATE INDEX CONCURRENTLY aborts
    // mid-build.
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(true);
      try (var stmt = conn.createStatement()) {
        stmt.execute("DROP INDEX IF EXISTS \"%s\".\"%s\"".formatted(schema, targetIndex));
        stmt.execute(
            ("CREATE INDEX \"%s\" ON \"%s\".workflow_status"
                    + " (queue_name, status, priority, created_at)"
                    + " WHERE status IN ('ENQUEUED', 'PENDING')")
                .formatted(targetIndex, schema));
        stmt.execute(
            ("UPDATE pg_index SET indisvalid = false"
                    + " WHERE indexrelid = '\"%s\".\"%s\"'::regclass")
                .formatted(schema, targetIndex));
      }
    }

    // Confirm the planted index is INVALID
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "SELECT indisvalid FROM pg_index WHERE indexrelid = ?::regclass")) {
      stmt.setString(1, "\"%s\".\"%s\"".formatted(schema, targetIndex));
      try (var rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        assertFalse(rs.getBoolean("indisvalid"), "Index should be invalid before migration re-run");
      }
    }

    // Rewind version so the runner re-executes migration 32
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.executeUpdate(
          "UPDATE \"%s\".dbos_migrations SET version = %d".formatted(schema, rewindTo));
    }

    // Re-run migrations: cleanupInvalidIndexes should drop the invalid index, then 32+ rebuild it
    assertDoesNotThrow(() -> MigrationManager.runMigrations(dbosConfig));

    // Index now exists and is valid
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "SELECT indisvalid FROM pg_index WHERE indexrelid = ?::regclass")) {
      stmt.setString(1, "\"%s\".\"%s\"".formatted(schema, targetIndex));
      try (var rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        assertTrue(rs.getBoolean("indisvalid"), "Index should be valid after migration re-run");
      }
    }

    try (var conn = dataSource.getConnection()) {
      assertEquals(expectedFinal, getVersion(conn));
    }
  }

  static void assertTableExists(DatabaseMetaData metaData, String tableName) throws Exception {
    assertTableExists(metaData, tableName, Constants.DB_SCHEMA);
  }

  static void assertTableExists(DatabaseMetaData metaData, String tableName, String schemaName)
      throws Exception {
    schemaName = SystemDatabase.sanitizeSchema(schemaName);
    try (ResultSet rs = metaData.getTables(null, schemaName, tableName, null)) {
      assertTrue(rs.next(), "Table %s should exist in schema %s".formatted(tableName, schemaName));
    }
  }

  static void assertFunctionExists(DatabaseMetaData metaData, String functionName)
      throws Exception {
    assertFunctionExists(metaData, functionName, Constants.DB_SCHEMA);
  }

  static void assertFunctionExists(
      DatabaseMetaData metaData, String functionName, String schemaName) throws Exception {
    schemaName = SystemDatabase.sanitizeSchema(schemaName);
    try (ResultSet rs = metaData.getFunctions(null, schemaName, functionName)) {
      assertTrue(
          rs.next(), "Function %s should exist in schema %s".formatted(functionName, schemaName));
    }
  }

  static int getVersion(Connection conn) throws Exception {
    return getVersion(conn, Constants.DB_SCHEMA);
  }

  static int getVersion(Connection conn, String schema) throws Exception {
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

  static void assertFunctionAbsent(Connection conn, String functionName) throws Exception {
    String sql =
        "SELECT 1 FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid"
            + " WHERE n.nspname = ? AND p.proname = ?";
    try (var ps = conn.prepareStatement(sql)) {
      ps.setString(1, Constants.DB_SCHEMA);
      ps.setString(2, functionName);
      try (var rs = ps.executeQuery()) {
        assertFalse(rs.next(), "Function %s should not exist".formatted(functionName));
      }
    }
  }

  static void assertTriggerExists(Connection conn, String triggerName) throws Exception {
    assertTriggerExists(conn, triggerName, Constants.DB_SCHEMA);
  }

  static void assertTriggerExists(Connection conn, String triggerName, String schema)
      throws Exception {
    schema = SystemDatabase.sanitizeSchema(schema);
    String sql =
        "SELECT 1 FROM pg_trigger t JOIN pg_class c ON t.tgrelid = c.oid"
            + " JOIN pg_namespace n ON c.relnamespace = n.oid"
            + " WHERE n.nspname = ? AND t.tgname = ?";
    try (var ps = conn.prepareStatement(sql)) {
      ps.setString(1, schema);
      ps.setString(2, triggerName);
      try (var rs = ps.executeQuery()) {
        assertTrue(
            rs.next(), "Trigger %s should exist in schema %s".formatted(triggerName, schema));
      }
    }
  }

  static boolean databaseExists(Connection conn, String dbName) throws Exception {
    try (ResultSet rs = conn.getMetaData().getCatalogs()) {
      while (rs.next()) {
        if (dbName.equals(rs.getString("TABLE_CAT"))) return true;
      }
      return false;
    }
  }

  static void assertTriggerAbsent(Connection conn, String triggerName) throws Exception {
    String sql =
        "SELECT 1 FROM pg_trigger t JOIN pg_class c ON t.tgrelid = c.oid"
            + " JOIN pg_namespace n ON c.relnamespace = n.oid"
            + " WHERE n.nspname = ? AND t.tgname = ?";
    try (var ps = conn.prepareStatement(sql)) {
      ps.setString(1, Constants.DB_SCHEMA);
      ps.setString(2, triggerName);
      try (var rs = ps.executeQuery()) {
        assertFalse(rs.next(), "Trigger %s should not exist".formatted(triggerName));
      }
    }
  }

  static void assertNotificationTableHasPrimaryKey(
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
