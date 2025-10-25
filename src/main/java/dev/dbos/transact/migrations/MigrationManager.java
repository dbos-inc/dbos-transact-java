package dev.dbos.transact.migrations;

import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;

import java.sql.*;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationManager {

  private static final Logger logger = LoggerFactory.getLogger(MigrationManager.class);

  public static void runMigrations(DBOSConfig config) {

    createDatabaseIfNotExists(Objects.requireNonNull(config, "DBOS Config must not be null"));

    try (var ds = SystemDatabase.createDataSource(config);
        var conn = ds.getConnection()) {
      ensureDbosSchema(conn, Constants.DB_SCHEMA);
      ensureMigrationTable(conn, Constants.DB_SCHEMA);
      var migrations = getMigrations(Constants.DB_SCHEMA);
      runDbosMigrations(conn, Constants.DB_SCHEMA, migrations);
    } catch (SQLException e) {
      throw new RuntimeException("Failed to run migrations", e);
    }
  }

  public static void createDatabaseIfNotExists(DBOSConfig config) {
    var dbUrl =
        Objects.requireNonNull(config.databaseUrl(), "DBOSConfig databaseUrl must not be null");
    var pair = extractDbAndPostgresUrl(dbUrl);

    try (var adminDS =
            SystemDatabase.createDataSource(pair.url(), config.dbUser(), config.dbPassword());
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
        throw new RuntimeException("Failed to check database", e);
      }

      logger.info("Creating '{}' database", pair.database());
      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("CREATE DATABASE \"" + pair.database() + "\"");
      } catch (SQLException e) {
        throw new RuntimeException("Failed to create database", e);
      }
    } catch (SQLException e) {
      var msg = "Failed to connect to database {}".formatted(pair.url());
      throw new RuntimeException(msg, e);
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
    Objects.requireNonNull(schema);
    var sql = "SELECT schema_name FROM information_schema.schemata WHERE schema_name = ?";
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, schema);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return;
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to check dbos schema", e);
    }

    try (var stmt = conn.createStatement()) {
      stmt.execute("CREATE SCHEMA %s".formatted(schema));
    } catch (SQLException e) {
      throw new RuntimeException("Failed to create dbos schema", e);
    }
  }

  public static void ensureMigrationTable(Connection conn, String schema) {
    Objects.requireNonNull(schema);
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
      throw new RuntimeException("Failed to check dbos migration table", e);
    }

    try (var stmt = conn.createStatement()) {
      stmt.execute(
          "CREATE TABLE %s.dbos_migrations (version BIGINT NOT NULL PRIMARY KEY)"
              .formatted(schema));
    } catch (SQLException e) {
      throw new RuntimeException("Failed to create dbos migration table", e);
    }
  }

  public static void runDbosMigrations(Connection conn, String schema, List<String> migrations) {
    Objects.requireNonNull(schema);

    long lastApplied = 0;
    try (var stmt = conn.createStatement()) {
      try (var rs = stmt.executeQuery("SELECT version FROM %s.dbos_migrations".formatted(schema))) {
        if (rs.next()) {
          lastApplied = rs.getLong("version");
        }
      }
    } catch (SQLException e) {
      throw new RuntimeException("Failed to retrieve migration version", e);
    }

    for (var i = 0; i < migrations.size(); i++) {
      var migrationIndex = i + 1;
      if (migrationIndex <= lastApplied) {
        continue;
      }

      logger.info("Applying DBOS system database schema migration {}", migrationIndex);
      try (var stmt = conn.createStatement()) {
        stmt.execute(migrations.get(i));
      } catch (SQLException e) {
        throw new RuntimeException("Failed to run migration %d".formatted(migrationIndex), e);
      }

      var sql =
          lastApplied == 0
              ? "INSERT INTO %s.dbos_migrations (version) VALUES (?)"
              : "UPDATE %s.dbos_migrations SET version = ?";
      try (var stmt = conn.prepareStatement(sql.formatted(schema))) {
        stmt.setLong(1, migrationIndex);
        stmt.executeUpdate();
      } catch (SQLException e) {
        throw new RuntimeException("Failed to save migration %d".formatted(migrationIndex), e);
      }

      lastApplied = migrationIndex;
    }
  }

  public static List<String> getMigrations(String schema) {
    Objects.requireNonNull(schema);
    return List.of(migrationOne.formatted(schema), migrationTwo.formatted(schema));
  }

  static final String migrationOne =
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

  static final String migrationTwo =
      "ALTER TABLE %1$s.workflow_status ADD COLUMN queue_partition_key TEXT;";
}
