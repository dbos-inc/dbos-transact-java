package dev.dbos.transact.migrations;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationManager {

  private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);
  private static final List<String> IGNORABLE_SQL_STATES =
      List.of(
          // Relation / object already exists
          "42P07", // duplicate_table
          "42710", // duplicate_object (e.g., index)
          "42701", // duplicate_column
          "42P06", // duplicate_schema
          // Uniqueness (e.g., insert seed rows twice)
          "23505" // unique_violation
          );

  public static void runMigrations(DBOSConfig config) {
    Objects.requireNonNull(config, "DBOS Config must not be null");

    if (config.dataSource() != null) {
      runMigrations(config.dataSource(), config.databaseSchema());
    } else {
      createDatabaseIfNotExists(config);
      try (var ds = SystemDatabase.createDataSource(config)) {
        runMigrations(ds, config.databaseSchema());
      }
    }
  }

  public static void runMigrations(String url, String user, String password, String schema) {
    Objects.requireNonNull(url, "database url must not be null");
    Objects.requireNonNull(user, "database user must not be null");
    Objects.requireNonNull(password, "database password must not be null");

    createDatabaseIfNotExists(url, user, password);
    try (var ds = SystemDatabase.createDataSource(url, user, password, 0, 0)) {
      runMigrations(ds, schema);
    }
  }

  private static void runMigrations(HikariDataSource ds, String schema) {
    Objects.requireNonNull(ds, "Data Source must not be null");
    schema = SystemDatabase.sanitizeSchema(schema);

    try (var conn = ds.getConnection()) {
      ensureDbosSchema(conn, schema);
      ensureMigrationTable(conn, schema);
      var migrations = getMigrations(schema);
      runDbosMigrations(conn, schema, migrations);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to run migrations", e);
    }
  }

  public static void createDatabaseIfNotExists(DBOSConfig config) {
    Objects.requireNonNull(config, "DBOS Config must not be null");
    if (config.dataSource() != null) {
      logger.debug("DBOSConfig specifies data source, skipping createDatabaseIfNotExists");
    } else {
      createDatabaseIfNotExists(config.databaseUrl(), config.dbUser(), config.dbPassword());
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
      stmt.execute("CREATE SCHEMA IF NOT EXISTS %s".formatted(schema));
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
          "CREATE TABLE IF NOT EXISTS %s.dbos_migrations (version BIGINT NOT NULL PRIMARY KEY)"
              .formatted(schema));
    } catch (SQLException e) {
      logger.warn("SQLException thrown creating the dbos_migrations table", e);
    }
  }

  public static int getCurrentSysDbVersion(Connection conn, String schema) {
    Objects.requireNonNull(schema, "schema must not be null");
    var sql =
        "SELECT version FROM %s.dbos_migrations ORDER BY version DESC limit 1".formatted(schema);
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

  static void runDbosMigrations(Connection conn, String schema, List<String> migrations) {
    Objects.requireNonNull(schema, "schema must not be null");
    var lastApplied = getCurrentSysDbVersion(conn, schema);

    for (var i = 0; i < migrations.size(); i++) {
      var migrationIndex = i + 1;
      if (migrationIndex <= lastApplied) {
        continue;
      }

      logger.info("Applying DBOS system database schema migration {}", migrationIndex);
      try (var stmt = conn.createStatement()) {
        stmt.execute(migrations.get(i));
      } catch (SQLException e) {
        if (IGNORABLE_SQL_STATES.contains(e.getSQLState())) {
          logger.warn(
              "Ignoring migration {} error; Migration was likely already applied. Occurred while executing {}",
              migrationIndex,
              migrations.get(i));
        } else {
          throw new RuntimeException("Failed to run migration %d".formatted(migrationIndex), e);
        }
      }

      try {
        int rowCount = 0;
        var updateSQL = "UPDATE %s.dbos_migrations SET version = ?".formatted(schema);
        try (var stmt = conn.prepareStatement(updateSQL)) {
          stmt.setLong(1, migrationIndex);
          rowCount = stmt.executeUpdate();
        }

        if (rowCount == 0) {
          var insertSql = "INSERT INTO %s.dbos_migrations (version) VALUES (?)".formatted(schema);
          try (var stmt = conn.prepareStatement(insertSql)) {
            stmt.setLong(1, migrationIndex);
            stmt.executeUpdate();
          }
        }
      } catch (SQLException e) {
        throw new RuntimeException("Failed to update dbos migration version", e);
      }

      lastApplied = migrationIndex;
    }
  }

  public static List<String> getMigrations(String schema) {
    Objects.requireNonNull(schema);
    var migrations =
        List.of(migration1, migration2, migration3, migration4, migration5, migration6);
    return migrations.stream().map(m -> m.formatted(schema)).toList();
  }

  static final String migration1 =
      """
      CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

      CREATE TABLE %1$s.workflow_status (
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
          priority INTEGER NOT NULL DEFAULT 0
      );

      CREATE INDEX workflow_status_created_at_index ON %1$s.workflow_status (created_at);
      CREATE INDEX workflow_status_executor_id_index ON %1$s.workflow_status (executor_id);
      CREATE INDEX workflow_status_status_index ON %1$s.workflow_status (status);

      ALTER TABLE %1$s.workflow_status
      ADD CONSTRAINT uq_workflow_status_queue_name_dedup_id
      UNIQUE (queue_name, deduplication_id);

      CREATE TABLE %1$s.operation_outputs (
          workflow_uuid TEXT NOT NULL,
          function_id INTEGER NOT NULL,
          function_name TEXT NOT NULL DEFAULT '',
          output TEXT,
          error TEXT,
          child_workflow_id TEXT,
          PRIMARY KEY (workflow_uuid, function_id),
          FOREIGN KEY (workflow_uuid) REFERENCES %1$s.workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );

      CREATE TABLE %1$s.notifications (
          destination_uuid TEXT NOT NULL,
          topic TEXT,
          message TEXT NOT NULL,
          created_at_epoch_ms BIGINT NOT NULL DEFAULT (EXTRACT(epoch FROM now()) * 1000::numeric)::bigint,
          message_uuid TEXT NOT NULL DEFAULT gen_random_uuid(), -- Built-in function
          FOREIGN KEY (destination_uuid) REFERENCES %1$s.workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );
      CREATE INDEX idx_workflow_topic ON %1$s.notifications (destination_uuid, topic);

      -- Create notification function
      CREATE OR REPLACE FUNCTION %1$s.notifications_function() RETURNS TRIGGER AS $$
      DECLARE
          payload text := NEW.destination_uuid || '::' || NEW.topic;
      BEGIN
          PERFORM pg_notify('dbos_notifications_channel', payload);
          RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      -- Create notification trigger
      CREATE TRIGGER dbos_notifications_trigger
      AFTER INSERT ON %1$s.notifications
      FOR EACH ROW EXECUTE FUNCTION %1$s.notifications_function();

      CREATE TABLE %1$s.workflow_events (
          workflow_uuid TEXT NOT NULL,
          key TEXT NOT NULL,
          value TEXT NOT NULL,
          PRIMARY KEY (workflow_uuid, key),
          FOREIGN KEY (workflow_uuid) REFERENCES %1$s.workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );

      -- Create events function
      CREATE OR REPLACE FUNCTION %1$s.workflow_events_function() RETURNS TRIGGER AS $$
      DECLARE
          payload text := NEW.workflow_uuid || '::' || NEW.key;
      BEGIN
          PERFORM pg_notify('dbos_workflow_events_channel', payload);
          RETURN NEW;
      END;
      $$ LANGUAGE plpgsql;

      -- Create events trigger
      CREATE TRIGGER dbos_workflow_events_trigger
      AFTER INSERT ON %1$s.workflow_events
      FOR EACH ROW EXECUTE FUNCTION %1$s.workflow_events_function();

      CREATE TABLE %1$s.streams (
          workflow_uuid TEXT NOT NULL,
          key TEXT NOT NULL,
          value TEXT NOT NULL,
          "offset" INTEGER NOT NULL,
          PRIMARY KEY (workflow_uuid, key, "offset"),
          FOREIGN KEY (workflow_uuid) REFERENCES %1$s.workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );

      CREATE TABLE %1$s.event_dispatch_kv (
          service_name TEXT NOT NULL,
          workflow_fn_name TEXT NOT NULL,
          key TEXT NOT NULL,
          value TEXT,
          update_seq NUMERIC(38,0),
          update_time NUMERIC(38,15),
          PRIMARY KEY (service_name, workflow_fn_name, key)
      );
      """;

  static final String migration2 =
      """
      ALTER TABLE %1$s.workflow_status ADD COLUMN queue_partition_key TEXT;
      """;

  static final String migration3 =
      """
      create index "idx_workflow_status_queue_status_started" on %1$s."workflow_status" ("queue_name", "status", "started_at_epoch_ms")
      """;

  static final String migration4 =
      """
      ALTER TABLE %1$s.workflow_status ADD COLUMN forked_from TEXT;
      """;

  static final String migration5 =
      """
      ALTER TABLE %1$s.operation_outputs ADD COLUMN started_at_epoch_ms BIGINT, ADD COLUMN completed_at_epoch_ms BIGINT;
      """;

  static final String migration6 =
      """
      CREATE TABLE %1$s.workflow_events_history (
          workflow_uuid TEXT NOT NULL,
          function_id INTEGER NOT NULL,
          key TEXT NOT NULL,
          value TEXT NOT NULL,
          PRIMARY KEY (workflow_uuid, function_id, key),
          FOREIGN KEY (workflow_uuid) REFERENCES %1$s.workflow_status(workflow_uuid)
              ON UPDATE CASCADE ON DELETE CASCADE
      );
      ALTER TABLE %1$s.streams ADD COLUMN function_id INTEGER NOT NULL DEFAULT 0;
      """;
}
