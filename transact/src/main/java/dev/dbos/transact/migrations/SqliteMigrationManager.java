package dev.dbos.transact.migrations;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class SqliteMigrationManager {

  private static final Logger logger = LoggerFactory.getLogger(SqliteMigrationManager.class);

  private static final String MIGRATIONS_TABLE = "dbos_migrations";

  // Migration numbering mirrors PostgreSQL numbering. Migrations 10, 14, and 20
  // have no SQLite counterpart (PG-only DDL) and are intentionally skipped.
  private record Migration(int version, String sql) {}

  static final String MIGRATION_1 =
      """
      CREATE TABLE workflow_status (
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
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          application_version TEXT,
          application_id TEXT,
          class_name TEXT DEFAULT NULL,
          config_name TEXT DEFAULT NULL,
          recovery_attempts INTEGER DEFAULT 0,
          queue_name TEXT,
          workflow_timeout_ms INTEGER,
          workflow_deadline_epoch_ms INTEGER,
          inputs TEXT,
          started_at_epoch_ms INTEGER,
          deduplication_id TEXT,
          priority INTEGER NOT NULL DEFAULT 0
      );

      CREATE INDEX workflow_status_created_at_index ON workflow_status (created_at);
      CREATE INDEX workflow_status_executor_id_index ON workflow_status (executor_id);
      CREATE INDEX workflow_status_status_index ON workflow_status (status);

      CREATE UNIQUE INDEX uq_workflow_status_queue_name_dedup_id
          ON workflow_status (queue_name, deduplication_id);

      CREATE TABLE operation_outputs (
          workflow_uuid TEXT NOT NULL,
          function_id INTEGER NOT NULL,
          function_name TEXT NOT NULL DEFAULT '',
          output TEXT,
          error TEXT,
          child_workflow_id TEXT,
          PRIMARY KEY (workflow_uuid, function_id),
          FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );

      CREATE TABLE notifications (
          message_uuid TEXT NOT NULL PRIMARY KEY,
          destination_uuid TEXT NOT NULL,
          topic TEXT,
          message TEXT NOT NULL,
          created_at_epoch_ms INTEGER NOT NULL,
          FOREIGN KEY (destination_uuid) REFERENCES workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );
      CREATE INDEX idx_workflow_topic ON notifications (destination_uuid, topic);

      CREATE TABLE workflow_events (
          workflow_uuid TEXT NOT NULL,
          key TEXT NOT NULL,
          value TEXT NOT NULL,
          PRIMARY KEY (workflow_uuid, key),
          FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );

      CREATE TABLE streams (
          workflow_uuid TEXT NOT NULL,
          key TEXT NOT NULL,
          value TEXT NOT NULL,
          "offset" INTEGER NOT NULL,
          PRIMARY KEY (workflow_uuid, key, "offset"),
          FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );

      CREATE TABLE event_dispatch_kv (
          service_name TEXT NOT NULL,
          workflow_fn_name TEXT NOT NULL,
          key TEXT NOT NULL,
          value TEXT,
          update_seq NUMERIC,
          update_time NUMERIC,
          PRIMARY KEY (service_name, workflow_fn_name, key)
      );
      """;

  static final String MIGRATION_2 =
      "ALTER TABLE workflow_status ADD COLUMN queue_partition_key TEXT";

  static final String MIGRATION_3 =
      """
      CREATE INDEX "idx_workflow_status_queue_status_started"
          ON "workflow_status" ("queue_name", "status", "started_at_epoch_ms")
      """;

  static final String MIGRATION_4 =
      """
      ALTER TABLE workflow_status ADD COLUMN forked_from TEXT;
      CREATE INDEX "idx_workflow_status_forked_from" ON "workflow_status" ("forked_from")
      """;

  static final String MIGRATION_5 =
      """
      ALTER TABLE operation_outputs ADD COLUMN started_at_epoch_ms INTEGER;
      ALTER TABLE operation_outputs ADD COLUMN completed_at_epoch_ms INTEGER
      """;

  static final String MIGRATION_6 =
      """
      CREATE TABLE workflow_events_history (
          workflow_uuid TEXT NOT NULL,
          function_id INTEGER NOT NULL,
          key TEXT NOT NULL,
          value TEXT NOT NULL,
          PRIMARY KEY (workflow_uuid, function_id, key),
          FOREIGN KEY (workflow_uuid) REFERENCES workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );
      ALTER TABLE streams ADD COLUMN function_id INTEGER NOT NULL DEFAULT 0
      """;

  static final String MIGRATION_7 =
      "ALTER TABLE workflow_status ADD COLUMN \"owner_xid\" TEXT DEFAULT NULL";

  static final String MIGRATION_8 =
      """
      ALTER TABLE workflow_status ADD COLUMN "parent_workflow_id" TEXT DEFAULT NULL;
      CREATE INDEX "idx_workflow_status_parent_workflow_id" ON "workflow_status" ("parent_workflow_id")
      """;

  static final String MIGRATION_9 =
      """
      CREATE TABLE workflow_schedules (
          schedule_id TEXT PRIMARY KEY,
          schedule_name TEXT NOT NULL UNIQUE,
          workflow_name TEXT NOT NULL,
          workflow_class_name TEXT,
          schedule TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'ACTIVE',
          context TEXT NOT NULL
      )
      """;

  // Migration 10 skipped: PG-only ALTER TABLE notifications ADD PRIMARY KEY (message_uuid)
  // (notifications table already has PRIMARY KEY in migration 1)

  static final String MIGRATION_11 =
      """
      ALTER TABLE "workflow_status" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      ALTER TABLE "notifications" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      ALTER TABLE "workflow_events" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      ALTER TABLE "workflow_events_history" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      ALTER TABLE "operation_outputs" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      ALTER TABLE "streams" ADD COLUMN "serialization" TEXT DEFAULT NULL
      """;

  static final String MIGRATION_12 =
      """
      ALTER TABLE "notifications" ADD COLUMN "consumed" BOOLEAN NOT NULL DEFAULT FALSE;
      CREATE INDEX "idx_notifications" ON "notifications" ("destination_uuid", "topic")
      """;

  static final String MIGRATION_13 =
      """
      CREATE TABLE application_versions (
          version_id TEXT NOT NULL PRIMARY KEY,
          version_name TEXT NOT NULL UNIQUE,
          version_timestamp INTEGER NOT NULL,
          created_at INTEGER NOT NULL
      )
      """;

  // Migration 14 skipped: PG-only CREATE FUNCTION enqueue_workflow (PL/pgSQL stored procedure)

  static final String MIGRATION_15 =
      """
      ALTER TABLE workflow_schedules ADD COLUMN "last_fired_at" TEXT DEFAULT NULL;
      ALTER TABLE workflow_schedules ADD COLUMN "automatic_backfill" BOOLEAN NOT NULL DEFAULT FALSE;
      ALTER TABLE workflow_schedules ADD COLUMN "cron_timezone" TEXT DEFAULT NULL
      """;

  static final String MIGRATION_16 =
      """
      ALTER TABLE "workflow_status" ADD COLUMN "delay_until_epoch_ms" INTEGER DEFAULT NULL;
      CREATE INDEX "idx_workflow_status_delayed" ON "workflow_status" ("delay_until_epoch_ms")
          WHERE status = 'DELAYED'
      """;

  static final String MIGRATION_17 =
      "ALTER TABLE workflow_schedules ADD COLUMN \"queue_name\" TEXT DEFAULT NULL";

  static final String MIGRATION_18 =
      "ALTER TABLE \"workflow_status\" ADD COLUMN \"was_forked_from\" BOOLEAN NOT NULL DEFAULT FALSE";

  static final String MIGRATION_19 =
      """
      CREATE INDEX "idx_operation_outputs_completed_at_function_name"
          ON "operation_outputs" ("completed_at_epoch_ms", "function_name")
      """;

  // Migration 20 skipped: PG-only ALTER FUNCTION (modifies enqueue_workflow and send_message)

  static final String MIGRATION_21 =
      """
      CREATE TABLE queues (
          queue_id TEXT PRIMARY KEY,
          name TEXT NOT NULL UNIQUE,
          concurrency INTEGER,
          worker_concurrency INTEGER,
          rate_limit_max INTEGER,
          rate_limit_period_sec REAL,
          priority_enabled BOOLEAN NOT NULL DEFAULT FALSE,
          partition_queue BOOLEAN NOT NULL DEFAULT FALSE,
          polling_interval_sec REAL NOT NULL DEFAULT 1.0,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL
      )
      """;

  static final String MIGRATION_22 =
      "DROP INDEX IF EXISTS \"idx_workflow_status_forked_from\"";

  static final String MIGRATION_23 =
      """
      CREATE INDEX IF NOT EXISTS "idx_workflow_status_forked_from"
          ON "workflow_status" ("forked_from")
          WHERE "forked_from" IS NOT NULL
      """;

  static final String MIGRATION_24 =
      "DROP INDEX IF EXISTS \"idx_workflow_status_parent_workflow_id\"";

  static final String MIGRATION_25 =
      """
      CREATE INDEX IF NOT EXISTS "idx_workflow_status_parent_workflow_id"
          ON "workflow_status" ("parent_workflow_id")
          WHERE "parent_workflow_id" IS NOT NULL
      """;

  static final String MIGRATION_26 =
      "DROP INDEX IF EXISTS \"workflow_status_executor_id_index\"";

  static final String MIGRATION_27 =
      """
      CREATE UNIQUE INDEX IF NOT EXISTS "uq_workflow_status_dedup_id"
          ON "workflow_status" ("queue_name", "deduplication_id")
          WHERE "deduplication_id" IS NOT NULL
      """;

  static final String MIGRATION_28 =
      // SQLite stores unique constraints as indexes, so we drop the index that backs
      // the legacy constraint. The replacement partial unique index was created by migration 27.
      "DROP INDEX IF EXISTS \"uq_workflow_status_queue_name_dedup_id\"";

  static final String MIGRATION_29 =
      """
      CREATE INDEX IF NOT EXISTS "idx_workflow_status_pending"
          ON "workflow_status" ("created_at")
          WHERE "status" = 'PENDING'
      """;

  static final String MIGRATION_30 =
      """
      CREATE INDEX IF NOT EXISTS "idx_workflow_status_failed"
          ON "workflow_status" ("status", "created_at")
          WHERE "status" IN ('ERROR', 'CANCELLED', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED')
      """;

  static final String MIGRATION_31 =
      "DROP INDEX IF EXISTS \"workflow_status_status_index\"";

  static final String MIGRATION_32 =
      """
      CREATE INDEX IF NOT EXISTS "idx_workflow_status_in_flight"
          ON "workflow_status" ("queue_name", "status", "priority", "created_at")
          WHERE "status" IN ('ENQUEUED', 'PENDING')
      """;

  static final String MIGRATION_33 =
      "ALTER TABLE \"workflow_status\" ADD COLUMN \"rate_limited\" BOOLEAN NOT NULL DEFAULT FALSE";

  static final String MIGRATION_34 =
      """
      CREATE INDEX IF NOT EXISTS "idx_workflow_status_rate_limited"
          ON "workflow_status" ("queue_name", "started_at_epoch_ms")
          WHERE "rate_limited" = TRUE
      """;

  static final String MIGRATION_35 =
      "DROP INDEX IF EXISTS \"idx_workflow_status_queue_status_started\"";

  private static List<Migration> getMigrations() {
    return List.of(
        new Migration(1,  MIGRATION_1),
        new Migration(2,  MIGRATION_2),
        new Migration(3,  MIGRATION_3),
        new Migration(4,  MIGRATION_4),
        new Migration(5,  MIGRATION_5),
        new Migration(6,  MIGRATION_6),
        new Migration(7,  MIGRATION_7),
        new Migration(8,  MIGRATION_8),
        new Migration(9,  MIGRATION_9),
        new Migration(11, MIGRATION_11),
        new Migration(12, MIGRATION_12),
        new Migration(13, MIGRATION_13),
        new Migration(15, MIGRATION_15),
        new Migration(16, MIGRATION_16),
        new Migration(17, MIGRATION_17),
        new Migration(18, MIGRATION_18),
        new Migration(19, MIGRATION_19),
        new Migration(21, MIGRATION_21),
        new Migration(22, MIGRATION_22),
        new Migration(23, MIGRATION_23),
        new Migration(24, MIGRATION_24),
        new Migration(25, MIGRATION_25),
        new Migration(26, MIGRATION_26),
        new Migration(27, MIGRATION_27),
        new Migration(28, MIGRATION_28),
        new Migration(29, MIGRATION_29),
        new Migration(30, MIGRATION_30),
        new Migration(31, MIGRATION_31),
        new Migration(32, MIGRATION_32),
        new Migration(33, MIGRATION_33),
        new Migration(34, MIGRATION_34),
        new Migration(35, MIGRATION_35)
    );
  }

  public static void runMigrations(DataSource dataSource) {
    try (var conn = dataSource.getConnection()) {
      ensureMigrationsTable(conn);
      var currentVersion = getCurrentVersion(conn);
      var migrations = getMigrations();
      var latestVersion = migrations.get(migrations.size() - 1).version();
      if (currentVersion >= latestVersion) {
        return;
      }
      for (var migration : migrations) {
        if (migration.version() <= currentVersion) {
          continue;
        }
        logger.info("Applying DBOS SQLite migration {}", migration.version());
        applyMigration(conn, migration, currentVersion);
        currentVersion = migration.version();
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to run SQLite migrations", e);
    }
  }

  private static void ensureMigrationsTable(Connection conn) throws SQLException {
    var exists = false;
    try (var stmt = conn.prepareStatement(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?")) {
      stmt.setString(1, MIGRATIONS_TABLE);
      try (var rs = stmt.executeQuery()) {
        exists = rs.next();
      }
    }
    if (!exists) {
      try (var stmt = conn.createStatement()) {
        stmt.execute("CREATE TABLE " + MIGRATIONS_TABLE
            + " (version INTEGER NOT NULL PRIMARY KEY)");
      }
    }
  }

  private static int getCurrentVersion(Connection conn) throws SQLException {
    try (var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT version FROM " + MIGRATIONS_TABLE + " LIMIT 1")) {
      return rs.next() ? rs.getInt(1) : 0;
    }
  }

  private static void applyMigration(Connection conn, Migration migration, int lastApplied)
      throws SQLException {
    conn.setAutoCommit(false);
    try {
      for (var statement : splitStatements(migration.sql())) {
        try (var stmt = conn.createStatement()) {
          stmt.execute(statement);
        }
      }
      if (lastApplied == 0) {
        try (var ps = conn.prepareStatement(
            "INSERT INTO " + MIGRATIONS_TABLE + " (version) VALUES (?)")) {
          ps.setInt(1, migration.version());
          ps.executeUpdate();
        }
      } else {
        try (var ps = conn.prepareStatement(
            "UPDATE " + MIGRATIONS_TABLE + " SET version = ?")) {
          ps.setInt(1, migration.version());
          ps.executeUpdate();
        }
      }
      conn.commit();
    } catch (SQLException e) {
      conn.rollback();
      throw e;
    } finally {
      conn.setAutoCommit(true);
    }
  }

  static List<String> splitStatements(String sql) {
    var result = new ArrayList<String>();
    for (var part : sql.split(";")) {
      var s = part.strip();
      if (!s.isEmpty()) {
        result.add(s);
      }
    }
    return result;
  }

  private SqliteMigrationManager() {}
}
