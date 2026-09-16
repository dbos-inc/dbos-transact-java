package dev.dbos.transact.migrations;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SqlTransaction;
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
      Set.of(22, 23, 24, 25, 26, 27, 29, 30, 31, 32, 34, 35, 37, 45, 46, 47, 107, 111);

  // From this index on, every SDK defines the same migration at the same index, so a migration
  // added here must be added to all of them.
  public static final int SHARED_MIGRATION_BASE = 100;

  /**
   * The oldest system database schema version this SDK can run against.
   *
   * <p>Migration 109 created the workflow_input and workflow_output tables, which every workflow
   * status read now consults, so anything older fails those reads outright. Migrations 110 and 111
   * add operation_outputs.retention_timestamp and its index, which the payload retention sweep
   * probes once per batch; at 110 the column exists but its index does not, and the sweep degrades
   * to a full scan and sort of the largest payload table per batch. That presents as a retention
   * round that never finishes rather than as an error, so 111 is the floor, not 110.
   *
   * <p>Migration 112 deliberately does not raise this. It drops a constraint rather than adding
   * anything to read, and every delete path clears the child tables by ID, so this SDK behaves
   * identically whether or not the cascade is still there.
   *
   * <p>This is a floor, not an equality: an executor here still reads a schema migrated ahead of
   * it, which is what makes rolling upgrades work. Raise it whenever new code starts depending
   * unconditionally on a later migration.
   */
  public static final int MINIMUM_SYSDB_VERSION = 111;

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

  /**
   * Reads the highest applied migration version, or 0 if dbos_migrations is empty.
   *
   * <p>Unlike {@link #getCurrentSysDbVersion}, this lets a {@link SQLException} propagate, so a
   * caller can tell an unmigrated schema from one it merely cannot read. Swallowed, both arrive as
   * version 0, and a database DBOS lacks privileges on is reported as one needing the DBOS schema
   * applied -- sending the operator off to re-apply a schema that is already correct.
   */
  private static int readSysDbVersion(Connection conn, String schema) throws SQLException {
    var sql =
        "SELECT version FROM \"%s\".dbos_migrations ORDER BY version DESC limit 1"
            .formatted(schema);
    try (var stmt = conn.createStatement();
        var rs = stmt.executeQuery(sql)) {
      return rs.next() ? rs.getInt("version") : 0;
    }
  }

  private static boolean migrationTableExists(Connection conn, String schema) throws SQLException {
    var sql =
        "SELECT 1 FROM information_schema.tables"
            + " WHERE table_schema = ? AND table_name = 'dbos_migrations'";
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, schema);
      try (var rs = stmt.executeQuery()) {
        return rs.next();
      }
    }
  }

  /**
   * Verifies that the system database schema is new enough for this SDK, without modifying it.
   *
   * <p>Used when {@link DBOSConfig#migrate()} is false and the deployment owns schema management.
   * Without this check the SDK never reads dbos_migrations at all, and a schema older than {@link
   * #MINIMUM_SYSDB_VERSION} surfaces much later as a raw "relation does not exist" on the first
   * workflow status read.
   *
   * @throws IllegalStateException if the schema is missing, unversioned, or too old
   */
  public static void validateSysDbVersion(DBOSConfig config) {
    Objects.requireNonNull(config, "DBOS Config must not be null");

    if (config.dataSource() != null) {
      validateSysDbVersion(config.dataSource(), config.databaseSchema());
    } else {
      validateSysDbVersion(
          config.databaseUrl(), config.dbUser(), config.dbPassword(), config.databaseSchema());
    }
  }

  /**
   * Verifies that the system database schema is new enough for this SDK, without modifying it.
   *
   * @throws IllegalStateException if the schema is missing, unversioned, or too old
   */
  public static void validateSysDbVersion(String url, String user, String password, String schema) {
    Objects.requireNonNull(url, "database url must not be null");

    try (var ds = SystemDatabase.createDataSource(url, user, password)) {
      validateSysDbVersion(ds, schema);
    }
  }

  /**
   * Verifies that the system database schema is new enough for this SDK, without modifying it.
   *
   * @throws IllegalStateException if the schema is missing, unversioned, or too old
   */
  public static void validateSysDbVersion(DataSource ds, String schema) {
    Objects.requireNonNull(ds, "Data Source must not be null");
    schema = SystemDatabase.sanitizeSchema(schema);

    if (schema.contains("'") || schema.contains("\"")) {
      throw new IllegalArgumentException("Schema name must not contain single or double quotes");
    }

    int version;
    try (var conn = ds.getConnection()) {
      version = readSysDbVersion(conn, schema);
    } catch (SQLException e) {
      // 42P01 undefined_table, 3F000 invalid_schema_name: the schema has never been migrated.
      // Any other failure -- notably 42501 insufficient_privilege -- says nothing about the
      // version, so it propagates rather than being reported as a schema that needs applying.
      var state = e.getSQLState();
      if ("42P01".equals(state) || "3F000".equals(state)) {
        throw new IllegalStateException(
            ("Schema \"%s\" has no dbos_migrations table, so its version cannot be determined."
                    + " DBOS requires system database schema version %d or later. Apply the DBOS"
                    + " schema to this database, or let DBOS migrate it, first.")
                .formatted(schema, MINIMUM_SYSDB_VERSION),
            e);
      }
      throw new RuntimeException("Failed to read the system database schema version", e);
    }

    if (version < MINIMUM_SYSDB_VERSION) {
      throw new IllegalStateException(
          ("Schema \"%s\" is at system database version %d, but this version of DBOS requires %d"
                  + " or later. Bring the schema up to date, or let DBOS migrate it, first.")
              .formatted(schema, version, MINIMUM_SYSDB_VERSION));
    }

    logger.debug(
        "Schema {} is at system database version {} (minimum {})",
        schema,
        version,
        MINIMUM_SYSDB_VERSION);
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
    if (!migrationTableExists(conn, schema)) return true;
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
    try {
      return readSysDbVersion(conn, schema);
    } catch (SQLException e) {
      logger.warn("SQLException thrown querying dbos_migrations table", e);
      return 0;
    }
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

  static void runDbosMigrations(
      Connection conn, String schema, List<String> migrations, boolean isCockroach) {
    Objects.requireNonNull(schema, "schema must not be null");
    var lastApplied = getCurrentSysDbVersion(conn, schema);

    for (var i = 0; i < migrations.size(); i++) {
      var migrationIndex = i + 1;
      if (migrationIndex <= lastApplied) {
        continue;
      }

      var migrationSql = migrations.get(i);
      // No DDL: either migration 20 on CockroachDB, or padding left by the renumbering onto
      // SHARED_MIGRATION_BASE. Skip without a round trip; the bump after the loop records them.
      if (migrationSql.isBlank()) {
        continue;
      }

      logger.info("Applying DBOS system database schema migration {}", migrationIndex);

      var versionBefore = lastApplied;

      try {
        if (migrationIndex == 10 && notificationsPrimaryKeyExists(conn, schema)) {
          // Migration 10 adds a primary key to notifications. Skip the DDL if one already exists
          // (guard for installs created before the primary key was added to migration 1).
          logger.info("Migration 10 skipped, primary key already exists");
          SqlTransaction.run(
              conn, c -> bumpMigrationVersion(c, schema, migrationIndex, versionBefore));
        } else if (ONLINE_MIGRATIONS.contains(migrationIndex) && !isCockroach) {
          // CONCURRENTLY index DDL cannot run inside a transaction. Clean up any indexes left
          // invalid by a prior failed attempt, run the DDL in autocommit, then bump the version
          // in its own transaction.
          cleanupInvalidIndexes(conn, schema);
          try (var stmt = conn.createStatement()) {
            stmt.execute(migrationSql);
          }
          SqlTransaction.run(
              conn, c -> bumpMigrationVersion(c, schema, migrationIndex, versionBefore));
        } else {
          // Standard migration: DDL and version bump in one transaction.
          SqlTransaction.run(
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

    // Empty migrations at the end still count as applied, so record them in one write.
    if (migrations.size() > lastApplied) {
      var versionBefore = lastApplied;
      try {
        SqlTransaction.run(
            conn, c -> bumpMigrationVersion(c, schema, migrations.size(), versionBefore));
      } catch (SQLException e) {
        throw new RuntimeException("Failed to record migration %d".formatted(migrations.size()), e);
      }
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
    var history =
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
            migration35(isCockroach),
            MIGRATION_36,
            migration37(isCockroach),
            migration38(isCockroach),
            migration39(useListenNotify),
            MIGRATION_40,
            MIGRATION_41,
            MIGRATION_42,
            MIGRATION_43,
            MIGRATION_44,
            migration45(isCockroach),
            migration46(isCockroach),
            migration47(isCockroach));
    var migrations = new ArrayList<>(padToSharedBase(history));
    // Versions from SHARED_MIGRATION_BASE on are defined identically by every DBOS SDK.
    migrations.addAll(
        List.of(
            MIGRATION_100,
            MIGRATION_101,
            MIGRATION_102,
            MIGRATION_103,
            MIGRATION_104,
            migration105(isCockroach),
            MIGRATION_106,
            migration107(isCockroach),
            MIGRATION_108,
            MIGRATION_109,
            MIGRATION_110,
            migration111(isCockroach),
            MIGRATION_112));
    return migrations.stream().map(m -> m.formatted(schema)).toList();
  }

  /**
   * Pads a language's own history out to {@code SHARED_MIGRATION_BASE - 1}. Indices below the base
   * stay per-language; the gap is safe to skip only because the schemas converge there.
   */
  private static List<String> padToSharedBase(List<String> history) {
    var padded = new ArrayList<>(history);
    while (padded.size() < SHARED_MIGRATION_BASE - 1) {
      padded.add("");
    }
    return padded;
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

  // ADD COLUMN with no default is catalog-only; partial index covers zero rows so no CONCURRENTLY.
  static final String MIGRATION_36 =
      """
      ALTER TABLE "%1$s"."workflow_status" ADD COLUMN IF NOT EXISTS "completed_at" BIGINT;
      CREATE INDEX IF NOT EXISTS "idx_workflow_status_completed_at" ON "%1$s"."workflow_status" ("completed_at") WHERE "completed_at" IS NOT NULL;
      """;

  static String migration37(boolean isCockroach) {
    return "CREATE INDEX "
        + concurrently(isCockroach)
        + " IF NOT EXISTS \"idx_workflow_status_started_at\""
        + " ON \"%1$s\".\"workflow_status\" (\"started_at_epoch_ms\")"
        + " WHERE \"started_at_epoch_ms\" IS NOT NULL";
  }

  static String migration38(boolean isCockroach) {
    var migration =
        """
        DROP FUNCTION IF EXISTS "%1$s".enqueue_workflow(
            TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INT4, TEXT
        );

        CREATE OR REPLACE FUNCTION "%1$s".enqueue_workflow(
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
            queue_partition_key TEXT DEFAULT NULL,
            authenticated_user TEXT DEFAULT NULL,
            authenticated_roles TEXT DEFAULT NULL,
            delay_until_epoch_ms BIGINT DEFAULT NULL
        ) RETURNS TEXT AS $$
        DECLARE
            v_workflow_id TEXT;
            v_serialized_inputs TEXT;
            v_owner_xid TEXT;
            v_now BIGINT;
            v_recovery_attempts INT4 := 0;
            v_priority INT4;
            v_status TEXT;
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
            IF delay_until_epoch_ms IS NOT NULL AND delay_until_epoch_ms < 0 THEN
                RAISE EXCEPTION 'delay_until_epoch_ms must be >= 0';
            END IF;

            v_workflow_id := COALESCE(workflow_id, gen_random_uuid()::TEXT);
            v_owner_xid := gen_random_uuid()::TEXT;
            v_priority := COALESCE(priority, 0);
            v_serialized_inputs := json_build_object(
                'positionalArgs', positional_args,
                'namedArgs', named_args
            )::TEXT;
            v_now := EXTRACT(epoch FROM now()) * 1000;
            v_status := CASE WHEN delay_until_epoch_ms IS NULL THEN 'ENQUEUED' ELSE 'DELAYED' END;

            INSERT INTO "%1$s".workflow_status (
                workflow_uuid, status, inputs,
                name, class_name, config_name,
                queue_name, deduplication_id, priority, queue_partition_key,
                application_version,
                created_at, updated_at, recovery_attempts,
                workflow_timeout_ms, workflow_deadline_epoch_ms,
                parent_workflow_id, owner_xid, serialization,
                authenticated_user, authenticated_roles,
                delay_until_epoch_ms
            ) VALUES (
                v_workflow_id, v_status, v_serialized_inputs,
                workflow_name, class_name, config_name,
                queue_name, deduplication_id, v_priority, queue_partition_key,
                app_version,
                v_now, v_now, v_recovery_attempts,
                timeout_ms, deadline_epoch_ms,
                NULL, v_owner_xid, 'portable_json',
                authenticated_user, authenticated_roles,
                delay_until_epoch_ms
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
        """;
    if (!isCockroach) {
      migration +=
          """
          ALTER FUNCTION "%1$s".enqueue_workflow(
              TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INT4, TEXT, TEXT, TEXT, BIGINT
          ) SET search_path = pg_catalog, pg_temp;
          """;
    }
    return migration;
  }

  static String migration39(boolean useListenNotify) {
    if (!useListenNotify) return "";
    return """
        -- Create streams notification function
        CREATE OR REPLACE FUNCTION "%1$s".streams_function() RETURNS TRIGGER AS $$
        DECLARE
            payload text := NEW.workflow_uuid || '::' || NEW.key;
        BEGIN
            PERFORM pg_notify('dbos_streams_channel', payload);
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        ALTER FUNCTION "%1$s".streams_function() SET search_path = pg_catalog, pg_temp;

        -- Create streams trigger
        DROP TRIGGER IF EXISTS dbos_streams_trigger ON "%1$s".streams;
        CREATE TRIGGER dbos_streams_trigger
        AFTER INSERT ON "%1$s".streams
        FOR EACH ROW EXECUTE FUNCTION "%1$s".streams_function();
        """;
  }

  // ADD COLUMN with no default is catalog-only; the partial index built in the same transaction
  // covers zero rows, so no CONCURRENTLY is needed. The index supports containment (@>) filters on
  // workflow attributes; on CockroachDB, USING GIN creates an inverted index.
  static final String MIGRATION_40 =
      """
      ALTER TABLE "%1$s"."workflow_status" ADD COLUMN IF NOT EXISTS "attributes" JSONB;
      CREATE INDEX IF NOT EXISTS "idx_workflow_status_attributes" ON "%1$s"."workflow_status" USING GIN ("attributes") WHERE "attributes" IS NOT NULL;
      """;

  // ADD COLUMN with no default is catalog-only; the partial index covers zero rows, so no
  // CONCURRENTLY is needed. Tracks which schedule (if any) triggered a workflow instance.
  static final String MIGRATION_41 =
      """
      ALTER TABLE "%1$s"."workflow_status" ADD COLUMN IF NOT EXISTS "schedule_name" TEXT;
      CREATE INDEX IF NOT EXISTS "idx_workflow_status_schedule_name" ON "%1$s"."workflow_status" ("schedule_name") WHERE "schedule_name" IS NOT NULL;
      """;

  // A debounced workflow is enqueued DELAYED holding its debounce key as its deduplication_id; each
  // bounce extends delay_until_epoch_ms, capped at debounce_deadline_epoch_ms. is_debounced marks
  // the deduplication ID as a debounce key to clear on the DELAYED -> ENQUEUED transition. Java's
  // debouncer does not use these columns, but a peer SDK sharing this system database does, and
  // only
  // one SDK gets to migrate a given database. ADD COLUMN with a constant default is catalog-only,
  // so
  // no CONCURRENTLY is needed.
  static final String MIGRATION_42 =
      """
      ALTER TABLE "%1$s"."workflow_status" ADD COLUMN IF NOT EXISTS "debounce_deadline_epoch_ms" BIGINT DEFAULT NULL;
      ALTER TABLE "%1$s"."workflow_status" ADD COLUMN IF NOT EXISTS "is_debounced" BOOLEAN NOT NULL DEFAULT FALSE;
      """;

  // Drop the streams NOTIFY trigger; stream writes are pushed by the notifier off the write path.
  //
  // Unconditional, unlike the migrations that create these triggers. Both statements are IF EXISTS
  // no-ops where the objects were never created, on PostgreSQL and on CockroachDB alike, and
  // gating them on useListenNotify would leave a hole: a process running without LISTEN/NOTIFY
  // that happens to be the one advancing the version past here would skip the drop, and no later
  // process would ever retry it. The triggers would survive forever on that database, still
  // sending a notification inside every write transaction -- which is the cost this removes.
  static final String MIGRATION_43 =
      """
      DROP TRIGGER IF EXISTS dbos_streams_trigger ON "%1$s".streams;
      DROP FUNCTION IF EXISTS "%1$s".streams_function();
      """;

  // Drop the workflow_events NOTIFY trigger; events are pushed by the notifier off the write path.
  // Unconditional, for the reason on migration 43.
  static final String MIGRATION_44 =
      """
      DROP TRIGGER IF EXISTS dbos_workflow_events_trigger ON "%1$s".workflow_events;
      DROP FUNCTION IF EXISTS "%1$s".workflow_events_function();
      """;

  // Extends idx_workflow_status_in_flight with queue_partition_key, so a lookup scoped to one
  // partition stays selective when many partitions are active. Superseded by the v2 index in
  // migration 46 and dropped in 47, so on a fresh database this exists only in between.
  static String migration45(boolean isCockroach) {
    return "CREATE INDEX "
        + concurrently(isCockroach)
        + " IF NOT EXISTS \"idx_workflow_status_partition_dequeue\""
        + " ON \"%1$s\".\"workflow_status\""
        + " (\"queue_name\", \"status\", \"queue_partition_key\", \"priority\", \"created_at\")"
        + " WHERE \"status\" IN ('ENQUEUED', 'PENDING') AND \"queue_partition_key\" IS NOT NULL";
  }

  // The trailing workflow_uuid totalizes the dequeue order, which the v1 index left ambiguous.
  // This is the index the partitioned dequeue in QueuesDAO.startQueuedWorkflows reads through:
  // queue_name, status and queue_partition_key are all equality-matched there, and priority and
  // created_at are its ordering.
  static String migration46(boolean isCockroach) {
    return "CREATE INDEX "
        + concurrently(isCockroach)
        + " IF NOT EXISTS \"idx_workflow_status_partition_dequeue_v2\""
        + " ON \"%1$s\".\"workflow_status\""
        + " (\"queue_name\", \"status\", \"queue_partition_key\", \"priority\", \"created_at\","
        + " \"workflow_uuid\")"
        + " WHERE \"status\" IN ('ENQUEUED', 'PENDING') AND \"queue_partition_key\" IS NOT NULL";
  }

  // Superseded by idx_workflow_status_partition_dequeue_v2.
  static String migration47(boolean isCockroach) {
    return "DROP INDEX "
        + concurrently(isCockroach)
        + " IF EXISTS \"%1$s\".\"idx_workflow_status_partition_dequeue\"";
  }

  // ── Shared migrations ───────────────────────────────────────────────────────────────────────
  // Migrations from SHARED_MIGRATION_BASE on are defined identically by every DBOS SDK, so
  // applications in different languages converge on one schema in a shared system database.

  // Migration 100: application_name on workflow_status, the first of the cross-SDK shared
  // history. NULL means unclaimed: any application may read and claim the row, which is what
  // keeps this migration safe for databases already holding another SDK's rows. One table per
  // migration, so a blocked table does not hold the others' locks.
  static final String MIGRATION_100 =
      """
      ALTER TABLE "%1$s"."workflow_status" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
      """;

  static final String MIGRATION_101 =
      """
      ALTER TABLE "%1$s"."queues" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
      """;

  static final String MIGRATION_102 =
      """
      ALTER TABLE "%1$s"."workflow_schedules" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
      """;

  static final String MIGRATION_103 =
      """
      ALTER TABLE "%1$s"."application_versions" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
      """;

  static final String MIGRATION_104 =
      """
      ALTER TABLE "%1$s"."operation_outputs" ADD COLUMN IF NOT EXISTS "application_name" TEXT DEFAULT NULL;
      """;

  // Migration 105: replace enqueue_workflow with a signature that also accepts a trailing
  // application_name. Every parameter is defaulted, so a caller omitting it -- an SDK predating
  // the feature -- still resolves to this function and enqueues an unclaimed workflow. The
  // 16-argument overload from migration 38 is dropped first so only one signature remains.
  static String migration105(boolean isCockroach) {
    var migration =
        """
        DROP FUNCTION IF EXISTS "%1$s".enqueue_workflow(
            TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INT4, TEXT, TEXT, TEXT, BIGINT
        );

        CREATE OR REPLACE FUNCTION "%1$s".enqueue_workflow(
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
            queue_partition_key TEXT DEFAULT NULL,
            authenticated_user TEXT DEFAULT NULL,
            authenticated_roles TEXT DEFAULT NULL,
            delay_until_epoch_ms BIGINT DEFAULT NULL,
            application_name TEXT DEFAULT NULL
        ) RETURNS TEXT AS $$
        DECLARE
            v_workflow_id TEXT;
            v_serialized_inputs TEXT;
            v_owner_xid TEXT;
            v_now BIGINT;
            v_recovery_attempts INT4 := 0;
            v_priority INT4;
            v_status TEXT;
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
            IF delay_until_epoch_ms IS NOT NULL AND delay_until_epoch_ms < 0 THEN
                RAISE EXCEPTION 'delay_until_epoch_ms must be >= 0';
            END IF;

            v_workflow_id := COALESCE(workflow_id, gen_random_uuid()::TEXT);
            v_owner_xid := gen_random_uuid()::TEXT;
            v_priority := COALESCE(priority, 0);
            v_serialized_inputs := json_build_object(
                'positionalArgs', positional_args,
                'namedArgs', named_args
            )::TEXT;
            v_now := EXTRACT(epoch FROM now()) * 1000;
            v_status := CASE WHEN delay_until_epoch_ms IS NULL THEN 'ENQUEUED' ELSE 'DELAYED' END;

            INSERT INTO "%1$s".workflow_status (
                workflow_uuid, status, inputs,
                name, class_name, config_name,
                queue_name, deduplication_id, priority, queue_partition_key,
                application_version,
                created_at, updated_at, recovery_attempts,
                workflow_timeout_ms, workflow_deadline_epoch_ms,
                parent_workflow_id, owner_xid, serialization,
                authenticated_user, authenticated_roles,
                delay_until_epoch_ms, application_name
            ) VALUES (
                v_workflow_id, v_status, v_serialized_inputs,
                workflow_name, class_name, config_name,
                queue_name, deduplication_id, v_priority, queue_partition_key,
                app_version,
                v_now, v_now, v_recovery_attempts,
                timeout_ms, deadline_epoch_ms,
                NULL, v_owner_xid, 'portable_json',
                authenticated_user, authenticated_roles,
                delay_until_epoch_ms, application_name
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
        """;
    if (!isCockroach) {
      migration +=
          """
          ALTER FUNCTION "%1$s".enqueue_workflow(
              TEXT, TEXT, JSON[], JSON, TEXT, TEXT, TEXT, TEXT, BIGINT, BIGINT, TEXT, INT4, TEXT, TEXT, TEXT, BIGINT, TEXT
          ) SET search_path = pg_catalog, pg_temp;
          """;
    }
    return migration;
  }

  // Migration 106: with 107, the pair of keys replacing version_name's retiring global
  // uniqueness, unclaimed counting as its own owner. The old unique constraint may not be
  // dropped until every SDK reaching this database is past 107.
  static final String MIGRATION_106 =
      """
      CREATE UNIQUE INDEX IF NOT EXISTS "uq_application_versions_owner_version"
          ON "%1$s"."application_versions" ("application_name", "version_name")
          WHERE "application_name" IS NOT NULL;
      """;

  // Migration 107: the unclaimed half of the key pair started in 106. Runs online because every
  // pre-upgrade row is unclaimed, so the index covers them all.
  static String migration107(boolean isCockroach) {
    return "CREATE UNIQUE INDEX "
        + concurrently(isCockroach)
        + " IF NOT EXISTS \"uq_application_versions_unclaimed_version\""
        + " ON \"%1$s\".\"application_versions\" (\"version_name\")"
        + " WHERE \"application_name\" IS NULL";
  }

  // Migration 108: per-partition limits on queues. Any of these being set partitions the queue;
  // each applies per partition. ADD COLUMN with a constant default is catalog-only, so no
  // CONCURRENTLY is needed.
  static final String MIGRATION_108 =
      """
      ALTER TABLE "%1$s"."queues" ADD COLUMN IF NOT EXISTS "partition_concurrency" INT4 DEFAULT NULL;
      ALTER TABLE "%1$s"."queues" ADD COLUMN IF NOT EXISTS "partition_worker_concurrency" INT4 DEFAULT NULL;
      ALTER TABLE "%1$s"."queues" ADD COLUMN IF NOT EXISTS "partition_rate_limit_max" INT4 DEFAULT NULL;
      ALTER TABLE "%1$s"."queues" ADD COLUMN IF NOT EXISTS "partition_rate_limit_period_sec" DOUBLE PRECISION DEFAULT NULL;
      """;

  // Migration 109: the tables that payloads move into, so a status update no longer rewrites a
  // large input. Creating them is all this release does: the reads below COALESCE over both
  // shapes, but every write here still fills the legacy workflow_status columns. Migration 113,
  // which stops the shared enqueue_workflow function writing them, comes with the writes in a
  // later release, once every executor can read both shapes.
  static final String MIGRATION_109 =
      """
      CREATE TABLE IF NOT EXISTS "%1$s"."workflow_input" (
          workflow_uuid TEXT NOT NULL PRIMARY KEY,
          inputs TEXT,
          retention_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
      );

      CREATE TABLE IF NOT EXISTS "%1$s"."workflow_output" (
          workflow_uuid TEXT NOT NULL PRIMARY KEY,
          output TEXT,
          error TEXT,
          retention_timestamp BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint
      );

      CREATE INDEX IF NOT EXISTS "idx_workflow_input_retention"
          ON "%1$s"."workflow_input" ("retention_timestamp");

      CREATE INDEX IF NOT EXISTS "idx_workflow_output_retention"
          ON "%1$s"."workflow_output" ("retention_timestamp");
      """;

  // Migration 110: sweep order only. The payload sweep deletes by absence of a status row, so
  // this bounds a round rather than deciding what it may delete.
  static final String MIGRATION_110 =
      """
      ALTER TABLE "%1$s"."operation_outputs"
          ADD COLUMN IF NOT EXISTS "retention_timestamp" BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000.0)::bigint;
      """;

  static String migration111(boolean isCockroach) {
    return "CREATE INDEX "
        + concurrently(isCockroach)
        + " IF NOT EXISTS \"idx_operation_outputs_retention\""
        + " ON \"%1$s\".\"operation_outputs\" (\"retention_timestamp\")";
  }

  // Migration 112: drop the operation_outputs -> workflow_status cascade. The cascade was the only
  // thing deleting a workflow's steps, and it did so one parent row at a time; the retention sweep
  // now takes all three child tables in batches instead, which is the point of the redesign. It
  // also charges a referential check to every step write. Both names appear because the SDKs
  // created the constraint differently: Java and Python let Postgres name it, TypeScript's Knex
  // migration named it "_foreign".
  static final String MIGRATION_112 =
      """
      ALTER TABLE "%1$s"."operation_outputs"
          DROP CONSTRAINT IF EXISTS "operation_outputs_workflow_uuid_foreign";

      ALTER TABLE "%1$s"."operation_outputs"
          DROP CONSTRAINT IF EXISTS "operation_outputs_workflow_uuid_fkey";
      """;
}
