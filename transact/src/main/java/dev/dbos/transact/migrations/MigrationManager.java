package dev.dbos.transact.migrations;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationManager {

  private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

  private static final Set<Integer> ONLINE_MIGRATIONS =
      Set.of(22, 23, 24, 25, 26, 27, 29, 30, 31, 32, 34, 35);

  private static final long MIGRATION_LOCK_ID = 1234567890L;
  private static final int MIGRATION_LOCK_TIMEOUT_SEC = 30;

  public static void runMigrations(DBOSConfig config) {
    Objects.requireNonNull(config, "DBOS Config must not be null");

    if (config.dataSource() != null) {
      runMigrations(config.dataSource(), config.databaseSchema(), config.useListenNotify());
    } else {
      createDatabaseIfNotExists(config.databaseUrl(), config.dbUser(), config.dbPassword());
      try (var ds =
          SystemDatabase.createDataSource(
              config.databaseUrl(), config.dbUser(), config.dbPassword())) {
        runMigrations(ds, config.databaseSchema(), config.useListenNotify());
      }
    }
  }

  public static void runMigrations(
      String url, String user, String password, String schema, boolean useListenNotify) {
    Objects.requireNonNull(url, "database url must not be null");
    Objects.requireNonNull(user, "database user must not be null");
    Objects.requireNonNull(password, "database password must not be null");

    createDatabaseIfNotExists(url, user, password);
    try (var ds = SystemDatabase.createDataSource(url, user, password)) {
      runMigrations(ds, schema, useListenNotify);
    }
  }

  private static boolean shouldMigrate(
      Connection conn, String schema, boolean useListenNotify, boolean isCockroach)
      throws SQLException {
    var schemaSql = "SELECT 1 FROM information_schema.schemata WHERE schema_name = ?";
    try (var stmt = conn.prepareStatement(schemaSql)) {
      stmt.setString(1, schema);
      try (var rs = stmt.executeQuery()) {
        if (!rs.next()) return true;
      }
    }
    var tableSql =
        "SELECT 1 FROM information_schema.tables"
            + " WHERE table_schema = ? AND table_name = 'dbos_migrations'";
    try (var stmt = conn.prepareStatement(tableSql)) {
      stmt.setString(1, schema);
      try (var rs = stmt.executeQuery()) {
        if (!rs.next()) return true;
      }
    }
    var currentVersion = getCurrentSysDbVersion(conn, schema);
    var latestVersion = getMigrations(schema, useListenNotify, isCockroach).size();
    return currentVersion < latestVersion;
  }

  private static void runMigrations(DataSource ds, String schema, boolean useListenNotify) {
    Objects.requireNonNull(ds, "Data Source must not be null");
    schema = SystemDatabase.sanitizeSchema(schema);

    if (schema.contains("'") || schema.contains("\"")) {
      throw new IllegalArgumentException("Schema name must not contain single or double quotes");
    }

    try (var checkConn = ds.getConnection()) {
      var isCockroach = SystemDatabase.isCockroach(checkConn);
      if (isCockroach) {
        useListenNotify = false;
      }

      // Skip advisory lock and migration work entirely if already up-to-date.
      if (!shouldMigrate(checkConn, schema, useListenNotify, isCockroach)) {
        return;
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to run migrations", e);
    }

    // Use a dedicated connection held in autocommit mode to hold the session-level advisory
    // lock for the entire migration run. Keeping the lock on a separate connection prevents
    // CockroachDB (and other databases) from releasing the lock when migration transactions
    // commit on the main connection.
    try (var lockConn = ds.getConnection();
        var migrConn = ds.getConnection()) {

      var isCockroach = SystemDatabase.isCockroach(migrConn);
      if (isCockroach) {
        useListenNotify = false;
      }

      boolean locked = false;
      lockConn.setAutoCommit(true);
      long deadline = System.currentTimeMillis() + MIGRATION_LOCK_TIMEOUT_SEC * 1000L;
      while (true) {
        try (var stmt = lockConn.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
          stmt.setLong(1, MIGRATION_LOCK_ID);
          try (var rs = stmt.executeQuery()) {
            if (rs.next() && rs.getBoolean(1)) {
              locked = true;
              break;
            }
          }
        }
        if (System.currentTimeMillis() >= deadline) {
          logger.warn(
              "Could not acquire migration advisory lock within {}s. Attempting migrations without lock.",
              MIGRATION_LOCK_TIMEOUT_SEC);
          break;
        }
        Thread.sleep(1000);
      }

      try {
        ensureDbosSchema(migrConn, schema);
        ensureMigrationTable(migrConn, schema);
        var migrations = getMigrations(schema, useListenNotify, isCockroach);
        runDbosMigrations(migrConn, schema, migrations, isCockroach);
      } finally {
        if (locked) {
          try (var stmt = lockConn.prepareStatement("SELECT pg_advisory_unlock(?)")) {
            stmt.setLong(1, MIGRATION_LOCK_ID);
            stmt.execute();
          } catch (SQLException e) {
            logger.warn("Failed to release migration advisory lock", e);
          }
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Migration interrupted while waiting for advisory lock", e);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to run migrations", e);
    }
  }

  public static void createDatabaseIfNotExists(String url, String user, String password) {
    Objects.requireNonNull(url, "database url must not be null");
    Objects.requireNonNull(user, "database user must not be null");
    Objects.requireNonNull(password, "database password must not be null");

    var pair = extractDbAndPostgresUrl(url);

    try (var adminDS = SystemDatabase.createDataSource(pair.url(), user, password);
        var conn = adminDS.getConnection()) {
      try (var stmt = conn.prepareStatement("SELECT 1 FROM pg_database WHERE datname = ?")) {
        stmt.setString(1, pair.database());
        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next()) {
            logger.debug("Database '{}' already exists", pair.database());
            return;
          }
        }
      } catch (SQLException e) {
        logger.warn("SQLException thrown looking for {} database", pair.database(), e);
      }

      logger.info("Creating '{}' database", pair.database());
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("CREATE DATABASE \"" + pair.database() + "\"");
      } catch (SQLException e) {
        logger.warn("SQLException thrown creating {} database", pair.database(), e);
      }
    } catch (SQLException e) {
      logger.warn("Failed to connect to database {}", pair.url());
    }
  }

  public record UrlPair(String url, String database) {}

  public static UrlPair extractDbAndPostgresUrl(String url) {
    int qm = Objects.requireNonNull(url, "database url must not be null").indexOf('?');
    var base = qm >= 0 ? url.substring(0, qm) : url;
    var params = qm >= 0 ? url.substring(qm) : "";
    int slash = base.lastIndexOf('/');
    if (slash < "jdbc:postgresql://".length()) {
      throw new IllegalArgumentException(String.format("JDBC URL %s is not valid", url));
    }

    var newUrl = base.substring(0, slash + 1) + "postgres" + params;
    var databaseName = base.substring(slash + 1);
    return new UrlPair(newUrl, databaseName);
  }

  public static void ensureDbosSchema(Connection conn, String schema) {
    Objects.requireNonNull(schema, "schema must not be null");
    var sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?";
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, schema);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return;
        }
      }
    } catch (SQLException e) {
      logger.warn("SQLException thrown looking for {} schema", schema, e);
    }

    try (var stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA IF NOT EXISTS \"%s\"".formatted(schema));
    } catch (SQLException e) {
      logger.warn("SQLException thrown creating the {} schema", schema, e);
    }
  }

  public static void ensureMigrationTable(Connection conn, String schema) {
    Objects.requireNonNull(schema, "schema must not be null");
    var sql =
        "SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_name = 'dbos_migrations'";
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, schema);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return;
        }
      }
    } catch (SQLException e) {
      logger.warn("SQLException thrown looking for dbos_migrations table", e);
    }

    try (var stmt = conn.createStatement()) {
      stmt.execute(
          "CREATE TABLE IF NOT EXISTS \"%s\".dbos_migrations (version BIGINT NOT NULL PRIMARY KEY)"
              .formatted(schema));
    } catch (SQLException e) {
      logger.warn("SQLException thrown creating the dbos_migrations table", e);
    }
  }

  public static int getCurrentSysDbVersion(Connection conn, String schema) {
    Objects.requireNonNull(schema, "schema must not be null");
    var sql =
        "SELECT version FROM \"%s\".dbos_migrations ORDER BY version DESC limit 1"
            .formatted(schema);
    try (var stmt = conn.createStatement();
        var rs = stmt.executeQuery(sql)) {
      if (rs.next()) {
        return rs.getInt("version");
      }
    } catch (SQLException e) {
      logger.warn("SQLException thrown querying dbos_migrations table", e);
    }

    return 0;
  }

  private static boolean notificationsPrimaryKeyExists(Connection conn, String schema)
      throws SQLException {
    try (var rs = conn.getMetaData().getPrimaryKeys(null, schema, "notifications")) {
      return rs.next();
    }
  }

  static void runDbosMigrations(Connection conn, String schema, List<String> migrations) {
    try {
      runDbosMigrations(conn, schema, migrations, SystemDatabase.isCockroach(conn));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @FunctionalInterface
  private interface SqlAction {
    void run(Connection conn) throws SQLException;
  }

  private static void runInTransaction(Connection conn, SqlAction action) throws SQLException {
    conn.setAutoCommit(false);
    try {
      action.run(conn);
      conn.commit();
    } catch (SQLException e) {
      conn.rollback();
      throw e;
    } finally {
      conn.setAutoCommit(true);
    }
  }

  static void runDbosMigrations(
      Connection conn, String schema, List<String> migrations, boolean isCockroach) {
    Objects.requireNonNull(schema, "schema must not be null");
    var lastApplied = getCurrentSysDbVersion(conn, schema);

    for (var i = 0; i < migrations.size(); i++) {
      var migrationIndex = i + 1;
      if (migrationIndex <= lastApplied) {
        continue;
      }

      logger.info("Applying DBOS system database schema migration {}", migrationIndex);

      var migrationSql = migrations.get(i);
      var versionBefore = lastApplied;

      try {
        if (migrationSql.isBlank()) {
          // No DDL (e.g. migration 20 on CockroachDB); just record the version.
          logger.info("Migration {} has no statements; skipping.", migrationIndex);
          runInTransaction(
              conn, c -> bumpMigrationVersion(c, schema, migrationIndex, versionBefore));
        } else if (migrationIndex == 10 && notificationsPrimaryKeyExists(conn, schema)) {
          // Migration 10 adds a primary key to notifications. Skip the DDL if one already exists
          // (guard for installs created before the primary key was added to migration 1).
          logger.info("Migration 10 skipped, primary key already exists");
          runInTransaction(
              conn, c -> bumpMigrationVersion(c, schema, migrationIndex, versionBefore));
        } else if (ONLINE_MIGRATIONS.contains(migrationIndex) && !isCockroach) {
          // CONCURRENTLY index DDL cannot run inside a transaction. Clean up any indexes left
          // invalid by a prior failed attempt, run the DDL in autocommit, then bump the version
          // in its own transaction.
          cleanupInvalidIndexes(conn, schema);
          try (var stmt = conn.createStatement()) {
            stmt.execute(migrationSql);
          }
          runInTransaction(
              conn, c -> bumpMigrationVersion(c, schema, migrationIndex, versionBefore));
        } else {
          // Standard migration: DDL and version bump in one transaction.
          runInTransaction(
              conn,
              c -> {
                try (var stmt = c.createStatement()) {
                  stmt.execute(migrationSql);
                }
                bumpMigrationVersion(c, schema, migrationIndex, versionBefore);
              });
        }
      } catch (SQLException e) {
        throw new RuntimeException("Failed to run migration %d".formatted(migrationIndex), e);
      }

      lastApplied = migrationIndex;
    }
  }

  private static void bumpMigrationVersion(
      Connection conn, String schema, int version, int versionBefore) throws SQLException {
    if (versionBefore == 0) {
      var sql = "INSERT INTO \"%s\".dbos_migrations (version) VALUES (?)".formatted(schema);
      try (var stmt = conn.prepareStatement(sql)) {
        stmt.setLong(1, version);
        stmt.executeUpdate();
      }
    } else {
      var sql = "UPDATE \"%s\".dbos_migrations SET version = ?".formatted(schema);
      try (var stmt = conn.prepareStatement(sql)) {
        stmt.setLong(1, version);
        stmt.executeUpdate();
      }
    }
  }

  private static void cleanupInvalidIndexes(Connection conn, String schema) throws SQLException {
    var sql =
        "SELECT i.relname FROM pg_index ix "
            + "JOIN pg_class i ON i.oid = ix.indexrelid "
            + "JOIN pg_class t ON t.oid = ix.indrelid "
            + "JOIN pg_namespace n ON n.oid = t.relnamespace "
            + "WHERE NOT ix.indisvalid AND n.nspname = ?";
    var invalidIndexes = new ArrayList<String>();
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, schema);
      try (var rs = stmt.executeQuery()) {
        while (rs.next()) {
          invalidIndexes.add(rs.getString(1));
        }
      }
    }
    for (var idxName : invalidIndexes) {
      logger.warn("Dropping invalid index {}.{} left by a prior failed migration", schema, idxName);
      try (var stmt = conn.createStatement()) {
        stmt.execute("DROP INDEX CONCURRENTLY IF EXISTS \"%s\".\"%s\"".formatted(schema, idxName));
      }
    }
  }

  public static List<String> getMigrations(
      String schema, boolean useListenNotify, boolean isCockroach) {
    Objects.requireNonNull(schema);
    var migrations =
        List.of(
            migration1(useListenNotify),
            MIGRATION_2,
            MIGRATION_3,
            MIGRATION_4,
            MIGRATION_5,
            MIGRATION_6,
            MIGRATION_7,
            MIGRATION_8,
            MIGRATION_9,
            MIGRATION_10,
            MIGRATION_11,
            MIGRATION_12,
            MIGRATION_13,
            MIGRATION_14,
            MIGRATION_15,
            MIGRATION_16,
            MIGRATION_17,
            MIGRATION_18,
            MIGRATION_19,
            migration20(useListenNotify, isCockroach),
            MIGRATION_21,
            migration22(isCockroach),
            migration23(isCockroach),
            migration24(isCockroach),
            migration25(isCockroach),
            migration26(isCockroach),
            migration27(isCockroach),
            migration28(isCockroach),
            migration29(isCockroach),
            migration30(isCockroach),
            migration31(isCockroach),
            migration32(isCockroach),
            MIGRATION_33,
            migration34(isCockroach),
            migration35(isCockroach));
    return migrations.stream().map(m -> m.formatted(schema)).toList();
  }

  static String migration1(boolean useListenNotify) {
    return useListenNotify ? MIGRATION_1 + MIGRATION_1_NOTIFY : MIGRATION_1;
  }

  static final String MIGRATION_1 =
      """
      -- Enable uuid extension for generating UUIDs
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
          created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
          updated_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
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
          message_uuid TEXT NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY, -- Built-in function
          destination_uuid TEXT NOT NULL,
          topic TEXT,
          message TEXT NOT NULL,
          created_at_epoch_ms BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
          FOREIGN KEY (destination_uuid) REFERENCES "%1$s".workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );
      CREATE INDEX idx_workflow_topic ON "%1$s".notifications (destination_uuid, topic);

      CREATE TABLE "%1$s".workflow_events (
          workflow_uuid TEXT NOT NULL,
          key TEXT NOT NULL,
          value TEXT NOT NULL,
          PRIMARY KEY (workflow_uuid, key),
          FOREIGN KEY (workflow_uuid) REFERENCES "%1$s".workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );

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

  static final String MIGRATION_1_NOTIFY =
      """
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
      """;

  static final String MIGRATION_2 =
      """
      ALTER TABLE "%1$s".workflow_status ADD COLUMN queue_partition_key TEXT;
      """;

  static final String MIGRATION_3 =
      """
      create index "idx_workflow_status_queue_status_started" on "%1$s"."workflow_status" ("queue_name", "status", "started_at_epoch_ms")
      """;

  static final String MIGRATION_4 =
      """
      ALTER TABLE "%1$s".workflow_status ADD COLUMN forked_from TEXT;
      CREATE INDEX "idx_workflow_status_forked_from" ON "%1$s"."workflow_status" ("forked_from");
      """;

  static final String MIGRATION_5 =
      """
      ALTER TABLE "%1$s".operation_outputs ADD COLUMN started_at_epoch_ms BIGINT, ADD COLUMN completed_at_epoch_ms BIGINT;
      """;

  static final String MIGRATION_6 =
      """
      CREATE TABLE "%1$s".workflow_events_history (
          workflow_uuid TEXT NOT NULL,
          function_id INT4 NOT NULL,
          key TEXT NOT NULL,
          value TEXT NOT NULL,
          PRIMARY KEY (workflow_uuid, function_id, key),
          FOREIGN KEY (workflow_uuid) REFERENCES "%1$s".workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );
      ALTER TABLE "%1$s".streams ADD COLUMN function_id INT4 NOT NULL DEFAULT 0;
      """;

  static final String MIGRATION_7 =
      """
      ALTER TABLE "%1$s"."workflow_status" ADD COLUMN "owner_xid" TEXT DEFAULT NULL
      """;

  static final String MIGRATION_8 =
      """
      ALTER TABLE "%1$s"."workflow_status" ADD COLUMN "parent_workflow_id" TEXT DEFAULT NULL;
      CREATE INDEX "idx_workflow_status_parent_workflow_id" ON "%1$s"."workflow_status" ("parent_workflow_id");
      """;

  static final String MIGRATION_9 =
      """
      CREATE TABLE "%1$s".workflow_schedules (
          schedule_id TEXT PRIMARY KEY,
          schedule_name TEXT NOT NULL UNIQUE,
          workflow_name TEXT NOT NULL,
          workflow_class_name TEXT,
          schedule TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'ACTIVE',
          context TEXT NOT NULL
      );
      """;

  static final String MIGRATION_10 =
      """
      ALTER TABLE "%1$s".notifications ADD PRIMARY KEY (message_uuid);
      """;

  static final String MIGRATION_11 =
      """
      ALTER TABLE "%1$s"."workflow_status" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      ALTER TABLE "%1$s"."notifications" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      ALTER TABLE "%1$s"."workflow_events" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      ALTER TABLE "%1$s"."workflow_events_history" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      ALTER TABLE "%1$s"."operation_outputs" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      ALTER TABLE "%1$s"."streams" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      """;

  static final String MIGRATION_12 =
      """
      ALTER TABLE "%1$s"."notifications" ADD COLUMN "consumed" BOOLEAN NOT NULL DEFAULT FALSE;
      CREATE INDEX "idx_notifications" ON "%1$s"."notifications" ("destination_uuid", "topic");
      """;

  static final String MIGRATION_13 =
      """
      CREATE TABLE "%1$s".application_versions (
        version_id TEXT NOT NULL PRIMARY KEY,
        version_name TEXT NOT NULL UNIQUE,
        version_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
        created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
      );
      """;

  static final String MIGRATION_14 =
      """
      CREATE FUNCTION "%1$s".enqueue_workflow(
          workflow_name TEXT,
          queue_name TEXT,
          positional_args JSON[] DEFAULT ARRAY[]::JSON[],
          named_args JSON DEFAULT '{}'::JSON,
          class_name TEXT DEFAULT NULL,
          config_name TEXT DEFAULT NULL,
          workflow_id TEXT DEFAULT NULL,
          app_version TEXT DEFAULT NULL,
          timeout_ms BIGINT DEFAULT NULL,
          deadline_epoch_ms BIGINT DEFAULT NULL,
          deduplication_id TEXT DEFAULT NULL,
          priority INT4 DEFAULT NULL,
          queue_partition_key TEXT DEFAULT NULL
      ) RETURNS TEXT AS $$
      DECLARE
          v_workflow_id TEXT;
          v_serialized_inputs TEXT;
          v_owner_xid TEXT;
          v_now BIGINT;
          v_recovery_attempts INT4 := 0;
          v_priority INT4;
      BEGIN

          -- Validate required parameters
          IF workflow_name IS NULL OR workflow_name = '' THEN
              RAISE EXCEPTION 'Workflow name cannot be null or empty';
          END IF;
          IF queue_name IS NULL OR queue_name = '' THEN
              RAISE EXCEPTION 'Queue name cannot be null or empty';
          END IF;
          IF named_args IS NOT NULL AND jsonb_typeof(named_args::jsonb) != 'object' THEN
              RAISE EXCEPTION 'Named args must be a JSON object';
          END IF;
          IF workflow_id IS NOT NULL AND workflow_id = '' THEN
              RAISE EXCEPTION 'Workflow ID cannot be an empty string if provided.';
          END IF;

          v_workflow_id := COALESCE(workflow_id, gen_random_uuid()::TEXT);
          v_owner_xid := gen_random_uuid()::TEXT;
          v_priority := COALESCE(priority, 0);
          v_serialized_inputs := json_build_object(
              'positionalArgs', positional_args,
              'namedArgs', named_args
          )::TEXT;
          v_now := EXTRACT(epoch FROM now()) * 1000;

          INSERT INTO "%1$s".workflow_status (
              workflow_uuid, status, inputs,
              name, class_name, config_name,
              queue_name, deduplication_id, priority, queue_partition_key,
              application_version,
              created_at, updated_at, recovery_attempts,
              workflow_timeout_ms, workflow_deadline_epoch_ms,
              parent_workflow_id, owner_xid, serialization
          ) VALUES (
              v_workflow_id, 'ENQUEUED', v_serialized_inputs,
              workflow_name, class_name, config_name,
              queue_name, deduplication_id, v_priority, queue_partition_key,
              app_version,
              v_now, v_now, v_recovery_attempts,
              timeout_ms, deadline_epoch_ms,
              NULL, v_owner_xid, 'portable_json'
          )
          ON CONFLICT (workflow_uuid)
          DO UPDATE SET
              updated_at = EXCLUDED.updated_at;

          RETURN v_workflow_id;

      EXCEPTION
          WHEN unique_violation THEN
              RAISE EXCEPTION 'DBOS queue duplicated'
                 USING DETAIL = format('Workflow %%s with queue %%s and deduplication ID %%s already exists', v_workflow_id, queue_name, deduplication_id),
                      ERRCODE = 'unique_violation';
      END;
      $$ LANGUAGE plpgsql;

      CREATE FUNCTION "%1$s".send_message(
          destination_id TEXT,
          message JSON,
          topic TEXT DEFAULT NULL,
          idempotency_key TEXT DEFAULT NULL
      ) RETURNS VOID AS $$
      DECLARE
          v_topic TEXT := COALESCE(topic, '__null__topic__');
          v_message_id TEXT := COALESCE(idempotency_key, gen_random_uuid()::TEXT);
      BEGIN
          INSERT INTO "%1$s".notifications (
              destination_uuid, topic, message, message_uuid, serialization
          ) VALUES (
              destination_id, v_topic, message, v_message_id, 'portable_json'
          )
          ON CONFLICT (message_uuid) DO NOTHING;
      EXCEPTION
          WHEN foreign_key_violation THEN
              RAISE EXCEPTION 'DBOS non-existent workflow'
                 USING DETAIL = format('Destination workflow %%s does not exist', destination_id),
                      ERRCODE = 'foreign_key_violation';
      END;
      $$ LANGUAGE plpgsql;
      """;

  static final String MIGRATION_15 =
      """
      ALTER TABLE "%1$s".workflow_schedules ADD COLUMN "last_fired_at" TEXT DEFAULT NULL;
      ALTER TABLE "%1$s".workflow_schedules ADD COLUMN "automatic_backfill" BOOLEAN NOT NULL DEFAULT FALSE;
      ALTER TABLE "%1$s".workflow_schedules ADD COLUMN "cron_timezone" TEXT DEFAULT NULL;
      """;

  static final String MIGRATION_16 =
      """
      ALTER TABLE "%1$s"."workflow_status" ADD COLUMN "delay_until_epoch_ms" BIGINT DEFAULT NULL;
      CREATE INDEX "idx_workflow_status_delayed" ON "%1$s"."workflow_status" ("delay_until_epoch_ms") WHERE status = 'DELAYED';
      """;

  static final String MIGRATION_17 =
      """
      ALTER TABLE "%1$s".workflow_schedules ADD COLUMN "queue_name" TEXT DEFAULT NULL;
      """;

  static final String MIGRATION_18 =
      """
      ALTER TABLE "%1$s"."workflow_status" ADD COLUMN "was_forked_from" BOOLEAN NOT NULL DEFAULT FALSE;
      """;

  static final String MIGRATION_19 =
      """
      CREATE INDEX "idx_operation_outputs_completed_at_function_name" ON "%1$s"."operation_outputs" ("completed_at_epoch_ms", "function_name");
      """;

  static String migration20(boolean useListenNotify, boolean isCockroach) {
    if (isCockroach) return "";
    var m =
        """
        ALTER FUNCTION "%1$s".enqueue_workflow(
            TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INT4, TEXT
        ) SET search_path = pg_catalog, pg_temp;

        ALTER FUNCTION "%1$s".send_message(
            TEXT, JSON, TEXT, TEXT
        ) SET search_path = pg_catalog, pg_temp;
        """;
    if (useListenNotify) {
      m +=
          """
          ALTER FUNCTION "%1$s".notifications_function() SET search_path = pg_catalog, pg_temp;
          ALTER FUNCTION "%1$s".workflow_events_function() SET search_path = pg_catalog, pg_temp;
          """;
    }
    return m;
  }

  static final String MIGRATION_21 =
      """
      CREATE TABLE "%1$s".queues (
          queue_id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
          name TEXT NOT NULL UNIQUE,
          concurrency INT4,
          worker_concurrency INT4,
          rate_limit_max INT4,
          rate_limit_period_sec DOUBLE PRECISION,
          priority_enabled BOOLEAN NOT NULL DEFAULT FALSE,
          partition_queue BOOLEAN NOT NULL DEFAULT FALSE,
          polling_interval_sec DOUBLE PRECISION NOT NULL DEFAULT 1.0,
          created_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint,
          updated_at BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
      );
      """;

  private static String concurrently(boolean isCockroach) {
    return isCockroach ? "" : "CONCURRENTLY";
  }

  static String migration22(boolean isCockroach) {
    return "DROP INDEX "
        + concurrently(isCockroach)
        + " IF EXISTS \"%1$s\".\"idx_workflow_status_forked_from\"";
  }

  static String migration23(boolean isCockroach) {
    return "CREATE INDEX "
        + concurrently(isCockroach)
        + " IF NOT EXISTS \"idx_workflow_status_forked_from\""
        + " ON \"%1$s\".\"workflow_status\" (\"forked_from\") WHERE \"forked_from\" IS NOT NULL";
  }

  static String migration24(boolean isCockroach) {
    return "DROP INDEX "
        + concurrently(isCockroach)
        + " IF EXISTS \"%1$s\".\"idx_workflow_status_parent_workflow_id\"";
  }

  static String migration25(boolean isCockroach) {
    return "CREATE INDEX "
        + concurrently(isCockroach)
        + " IF NOT EXISTS \"idx_workflow_status_parent_workflow_id\""
        + " ON \"%1$s\".\"workflow_status\" (\"parent_workflow_id\")"
        + " WHERE \"parent_workflow_id\" IS NOT NULL";
  }

  static String migration26(boolean isCockroach) {
    return "DROP INDEX "
        + concurrently(isCockroach)
        + " IF EXISTS \"%1$s\".\"workflow_status_executor_id_index\"";
  }

  static String migration27(boolean isCockroach) {
    // New partial unique index uses a different name to avoid collision with the old constraint
    return "CREATE UNIQUE INDEX "
        + concurrently(isCockroach)
        + " IF NOT EXISTS \"uq_workflow_status_dedup_id\""
        + " ON \"%1$s\".\"workflow_status\" (\"queue_name\", \"deduplication_id\")"
        + " WHERE \"deduplication_id\" IS NOT NULL";
  }

  static String migration28(boolean isCockroach) {
    // CockroachDB implements unique constraints as indexes and rejects ALTER TABLE DROP CONSTRAINT;
    // Postgres rejects DROP INDEX on a constraint-backed index.
    if (isCockroach) {
      return "DROP INDEX IF EXISTS \"%1$s\".\"uq_workflow_status_queue_name_dedup_id\" CASCADE";
    }
    return "ALTER TABLE \"%1$s\".workflow_status"
        + " DROP CONSTRAINT IF EXISTS uq_workflow_status_queue_name_dedup_id";
  }

  static String migration29(boolean isCockroach) {
    return "CREATE INDEX "
        + concurrently(isCockroach)
        + " IF NOT EXISTS \"idx_workflow_status_pending\""
        + " ON \"%1$s\".\"workflow_status\" (\"created_at\") WHERE \"status\" = 'PENDING'";
  }

  static String migration30(boolean isCockroach) {
    return "CREATE INDEX "
        + concurrently(isCockroach)
        + " IF NOT EXISTS \"idx_workflow_status_failed\""
        + " ON \"%1$s\".\"workflow_status\" (\"status\", \"created_at\")"
        + " WHERE \"status\" IN ('ERROR', 'CANCELLED', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED')";
  }

  static String migration31(boolean isCockroach) {
    return "DROP INDEX "
        + concurrently(isCockroach)
        + " IF EXISTS \"%1$s\".\"workflow_status_status_index\"";
  }

  static String migration32(boolean isCockroach) {
    return "CREATE INDEX "
        + concurrently(isCockroach)
        + " IF NOT EXISTS \"idx_workflow_status_in_flight\""
        + " ON \"%1$s\".\"workflow_status\" (\"queue_name\", \"status\", \"priority\", \"created_at\")"
        + " WHERE \"status\" IN ('ENQUEUED', 'PENDING')";
  }

  // ALTER TABLE ADD COLUMN with constant default is a fast catalog-only update on Postgres.
  static final String MIGRATION_33 =
      "ALTER TABLE \"%1$s\".\"workflow_status\""
          + " ADD COLUMN IF NOT EXISTS \"rate_limited\" BOOLEAN NOT NULL DEFAULT FALSE";

  static String migration34(boolean isCockroach) {
    return "CREATE INDEX "
        + concurrently(isCockroach)
        + " IF NOT EXISTS \"idx_workflow_status_rate_limited\""
        + " ON \"%1$s\".\"workflow_status\" (\"queue_name\", \"started_at_epoch_ms\")"
        + " WHERE \"rate_limited\" = TRUE";
  }

  static String migration35(boolean isCockroach) {
    return "DROP INDEX "
        + concurrently(isCockroach)
        + " IF EXISTS \"%1$s\".\"idx_workflow_status_queue_status_started\"";
  }
}
