package dev.dbos.transact.migrations;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;

import javax.sql.DataSource;

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
      createDatabaseIfNotExists(config.databaseUrl(), config.dbUser(), config.dbPassword());
      try (var ds =
          SystemDatabase.createDataSource(
              config.databaseUrl(), config.dbUser(), config.dbPassword())) {
        runMigrations(ds, config.databaseSchema());
      }
    }
  }

  public static void runMigrations(String url, String user, String password, String schema) {
    Objects.requireNonNull(url, "database url must not be null");
    Objects.requireNonNull(user, "database user must not be null");
    Objects.requireNonNull(password, "database password must not be null");

    createDatabaseIfNotExists(url, user, password);
    try (var ds = SystemDatabase.createDataSource(url, user, password)) {
      runMigrations(ds, schema);
    }
  }

  private static void runMigrations(DataSource ds, String schema) {
    Objects.requireNonNull(ds, "Data Source must not be null");
    schema = SystemDatabase.sanitizeSchema(schema);

    if (schema.contains("'") || schema.contains("\"")) {
      throw new IllegalArgumentException("Schema name must not contain single or double quotes");
    }

    try (var conn = ds.getConnection()) {
      ensureDbosSchema(conn, schema);
      ensureMigrationTable(conn, schema);
      var migrations = getMigrations(schema);
      runDbosMigrations(conn, schema, migrations);
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
        var updateSQL = "UPDATE \"%s\".dbos_migrations SET version = ?".formatted(schema);
        try (var stmt = conn.prepareStatement(updateSQL)) {
          stmt.setLong(1, migrationIndex);
          rowCount = stmt.executeUpdate();
        }

        if (rowCount == 0) {
          var insertSql =
              "INSERT INTO \"%s\".dbos_migrations (version) VALUES (?)".formatted(schema);
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
        List.of(
            migration1,
            migration2,
            migration3,
            migration4,
            migration5,
            migration6,
            migration7,
            migration8,
            migration9,
            migration10,
            migration11,
            migration12);
    return migrations.stream().map(m -> m.formatted(schema)).toList();
  }

  static final String migration1 =
      """
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
          message_uuid TEXT NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY, -- Built-in function
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

  static final String migration2 =
      """
      ALTER TABLE "%1$s".workflow_status ADD COLUMN queue_partition_key TEXT;
      """;

  static final String migration3 =
      """
      create index "idx_workflow_status_queue_status_started" on "%1$s"."workflow_status" ("queue_name", "status", "started_at_epoch_ms")
      """;

  static final String migration4 =
      """
      ALTER TABLE "%1$s".workflow_status ADD COLUMN forked_from TEXT;
      CREATE INDEX "idx_workflow_status_forked_from" ON "%1$s"."workflow_status" ("forked_from");
      """;

  static final String migration5 =
      """
      ALTER TABLE "%1$s".operation_outputs ADD COLUMN started_at_epoch_ms BIGINT, ADD COLUMN completed_at_epoch_ms BIGINT;
      """;

  static final String migration6 =
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

  static final String migration7 =
      """
      ALTER TABLE "%1$s"."workflow_status" ADD COLUMN "owner_xid" VARCHAR(40) DEFAULT NULL
      """;

  static final String migration8 =
      """
      ALTER TABLE "%1$s"."workflow_status" ADD COLUMN "parent_workflow_id" TEXT DEFAULT NULL;
      CREATE INDEX "idx_workflow_status_parent_workflow_id" ON "%1$s"."workflow_status" ("parent_workflow_id");
      """;

  static final String migration9 =
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

  static final String migration10 =
      """
      DO $$
      BEGIN
          IF NOT EXISTS (
              SELECT 1 FROM information_schema.table_constraints
              WHERE table_schema = '%1$s'
              AND table_name = 'notifications'
              AND constraint_type = 'PRIMARY KEY'
          ) THEN
              ALTER TABLE "%1$s".notifications ADD PRIMARY KEY (message_uuid);
          END IF;
      END $$;
      """;

  static final String migration11 =
      """
      ALTER TABLE "%1$s"."workflow_status" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      ALTER TABLE "%1$s"."notifications" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      ALTER TABLE "%1$s"."workflow_events" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      ALTER TABLE "%1$s"."workflow_events_history" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      ALTER TABLE "%1$s"."operation_outputs" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      ALTER TABLE "%1$s"."streams" ADD COLUMN "serialization" TEXT DEFAULT NULL;
      """;

  static final String migration12 =
      """
      -- Add PL/pgSQL functions for portable workflow client
      
      CREATE FUNCTION "%1$s"._init_workflow_status(
        p_workflow_uuid TEXT,
        p_status TEXT,
        p_inputs TEXT,
        p_name TEXT,
        p_class_name TEXT,
        p_config_name TEXT,
        p_queue_name TEXT,
        p_deduplication_id TEXT,
        p_priority INTEGER,
        p_queue_partition_key TEXT,
        p_application_version TEXT,
        p_workflow_timeout_ms BIGINT,
        p_workflow_deadline_epoch_ms BIGINT,
        p_parent_workflow_id TEXT,
        p_serialization TEXT
      ) RETURNS VOID AS $$
      --
      -- INTERNAL FUNCTION: Only intended to be called by send_message and enqueue_workflow.
      -- The underscore prefix indicates this is an internal implementation detail.
      --
      DECLARE
        v_owner_xid TEXT := gen_random_uuid()::TEXT;
        v_now BIGINT := EXTRACT(epoch FROM now()) * 1000;
        v_recovery_attempts INTEGER := CASE WHEN p_status = 'ENQUEUED' THEN 0 ELSE 1 END;
        v_priority INTEGER := COALESCE(p_priority, 0);
        -- Variables to validate existing workflow metadata
        v_existing_name TEXT;
        v_existing_class_name TEXT;
        v_existing_config_name TEXT;
      BEGIN
        -- Validate workflow UUID
        IF p_workflow_uuid IS NULL OR p_workflow_uuid = '' THEN
          RAISE EXCEPTION 'Workflow UUID cannot be null or empty';
        END IF;
        
        -- Validate workflow name
        IF p_name IS NULL OR p_name = '' THEN
          RAISE EXCEPTION 'Workflow name cannot be null or empty';
        END IF;
        
        -- Validate status is one of the allowed values
        IF p_status NOT IN ('PENDING', 'SUCCESS', 'ERROR', 'MAX_RECOVERY_ATTEMPTS_EXCEEDED', 'CANCELLED', 'ENQUEUED') THEN
          RAISE EXCEPTION 'Invalid status: %%. Status must be one of: PENDING, SUCCESS, ERROR, MAX_RECOVERY_ATTEMPTS_EXCEEDED, CANCELLED, ENQUEUED', p_status;
        END IF;
        
        -- Atomic insert with conflict resolution
        INSERT INTO "%1$s".workflow_status (
          workflow_uuid, status, inputs,
          name, class_name, config_name,
          queue_name, deduplication_id, priority, queue_partition_key,
          application_version,
          created_at, updated_at, recovery_attempts,
          workflow_timeout_ms, workflow_deadline_epoch_ms,
          parent_workflow_id, owner_xid, serialization
        ) VALUES (
          p_workflow_uuid, p_status, p_inputs,
          p_name, p_class_name, p_config_name,
          p_queue_name, p_deduplication_id, v_priority, p_queue_partition_key,
          p_application_version,
          v_now, v_now, v_recovery_attempts,
          p_workflow_timeout_ms, p_workflow_deadline_epoch_ms,
          p_parent_workflow_id, v_owner_xid, p_serialization
        )
        ON CONFLICT (workflow_uuid)
        DO UPDATE SET
          updated_at = EXCLUDED.updated_at
        RETURNING name, class_name, config_name
        INTO v_existing_name, v_existing_class_name, v_existing_config_name;

        -- Validate workflow metadata matches
        IF v_existing_name IS DISTINCT FROM p_name THEN
          RAISE EXCEPTION 'DBOS_CONFLICTING_WORKFLOW: Workflow already exists with a different function name: %%, but the provided function name is: %%', v_existing_name, p_name;
        END IF;
        
        IF v_existing_class_name IS DISTINCT FROM p_class_name THEN
          RAISE EXCEPTION 'DBOS_CONFLICTING_WORKFLOW: Workflow already exists with a different class name: %%, but the provided class name is: %%', v_existing_class_name, p_class_name;
        END IF;
        
        IF v_existing_config_name IS DISTINCT FROM p_config_name THEN
          RAISE EXCEPTION 'DBOS_CONFLICTING_WORKFLOW: Workflow already exists with a different class configuration: %%, but the provided class configuration is: %%', v_existing_config_name, p_config_name;
        END IF;
        
      EXCEPTION
        WHEN unique_violation THEN
          -- Handle duplicate deduplication_id
          IF SQLERRM LIKE '%%deduplication_id%%' THEN
            RAISE EXCEPTION 'DBOS_QUEUE_DUPLICATED: Workflow %% with queue %% and deduplication ID %% already exists', p_workflow_uuid, COALESCE(p_queue_name, ''), COALESCE(p_deduplication_id, '');
          END IF;
          RAISE;
        WHEN OTHERS THEN
          -- Re-raise custom exceptions and other errors
          RAISE;
      END;
      $$ LANGUAGE plpgsql;
      
      CREATE FUNCTION "%1$s".send_message(
        p_destination_id TEXT,
        p_message JSON,
        p_topic TEXT DEFAULT NULL,
        p_idempotency_key TEXT DEFAULT NULL
      ) RETURNS TEXT AS $$
      DECLARE
        v_idempotency_key TEXT;
        v_workflow_id TEXT;
        v_topic TEXT;
        v_current_time_ms BIGINT := extract(epoch from now()) * 1000;
      BEGIN
        -- Validate required parameters
        IF p_destination_id IS NULL OR p_destination_id = '' THEN
          RAISE EXCEPTION 'Destination ID cannot be null or empty';
        END IF;
        
        -- Generate UUID if idempotency key not provided
        v_idempotency_key := COALESCE(p_idempotency_key, gen_random_uuid()::TEXT);
        
        -- Handle null topic
        v_topic := COALESCE(p_topic, '__null__topic__');
        
        -- Create workflow ID by combining destination ID and idempotency key
        v_workflow_id := p_destination_id || '-' || v_idempotency_key;
        
        -- Initialize temporary workflow status for sending
        PERFORM "%1$s"._init_workflow_status(
          v_workflow_id,
          'SUCCESS', -- WorkflowState.SUCCESS
          NULL, -- inputs (not needed for send workflow)
          'temp_workflow-send-client', -- workflow_name
          NULL, -- class_name
          NULL, -- config_name
          NULL, -- queue_name (not queued)
          NULL, -- deduplication_id
          NULL, -- priority
          NULL, -- queue_partition_key
          NULL, -- app_version
          NULL, -- timeout_ms
          NULL, -- deadline_epoch_ms
          NULL, -- parent_workflow_id
          'portable_json' -- serialization_format (always portable JSON)
        );
        
        -- Send the message by inserting into the notifications table
        INSERT INTO "%1$s".notifications (
          destination_uuid,
          topic,
          message,
          serialization
        ) VALUES (
          p_destination_id,
          v_topic,
          p_message::TEXT, -- serialize message as JSON text
          'portable_json'
        );
        
        -- Record this send operation as a step in the temporary workflow
        INSERT INTO "%1$s".operation_outputs (
          workflow_uuid,
          function_id,
          function_name,
          output,
          error,
          child_workflow_id,
          started_at_epoch_ms,
          completed_at_epoch_ms,
          serialization
        ) VALUES (
          v_workflow_id,
          0, -- function_id (step 0 for send operation)
          'DBOS.send', -- function_name
          NULL, -- output (send doesn't return anything)
          NULL, -- error
          NULL, -- child_workflow_id
          v_current_time_ms, -- started_at_epoch_ms
          v_current_time_ms, -- completed_at_epoch_ms
          'portable_json' -- serialization
        );
        
        -- Return the workflow ID used for this send operation
        RETURN v_workflow_id;
        
      EXCEPTION
        WHEN OTHERS THEN
          -- Re-raise any exceptions
          RAISE;
      END;
      $$ LANGUAGE plpgsql;
      
      CREATE FUNCTION "%1$s".enqueue_workflow(
        p_workflow_name TEXT,
        p_queue_name TEXT,
        p_positional_args JSON[] DEFAULT ARRAY[]::JSON[],
        p_named_args JSON DEFAULT '{}'::JSON,
        p_class_name TEXT DEFAULT NULL,
        p_config_name TEXT DEFAULT NULL,
        p_workflow_id TEXT DEFAULT NULL,
        p_app_version TEXT DEFAULT NULL,
        p_timeout_ms BIGINT DEFAULT NULL,
        p_deadline_epoch_ms BIGINT DEFAULT NULL,
        p_deduplication_id TEXT DEFAULT NULL,
        p_priority INTEGER DEFAULT NULL,
        p_queue_partition_key TEXT DEFAULT NULL,
        p_parent_workflow_id TEXT DEFAULT NULL
      ) RETURNS TEXT AS $$
      DECLARE
        v_workflow_id TEXT;
        v_serialized_inputs TEXT;
      BEGIN
        -- Validate required parameters
        IF p_workflow_name IS NULL OR p_workflow_name = '' THEN
          RAISE EXCEPTION 'Workflow name cannot be null or empty';
        END IF;
        
        IF p_queue_name IS NULL OR p_queue_name = '' THEN
          RAISE EXCEPTION 'Queue name cannot be null or empty';
        END IF;
        
        -- Validate p_named_args is an object if not null
        IF p_named_args IS NOT NULL AND jsonb_typeof(p_named_args::jsonb) != 'object' THEN
          RAISE EXCEPTION 'Named args must be a JSON object';
        END IF;
        
        -- Serialize the arguments in portable format
        v_serialized_inputs := json_build_object(
          'positionalArgs', p_positional_args,
          'namedArgs', p_named_args
        )::TEXT;
        
        -- Generate UUID if workflow ID not provided
        v_workflow_id := COALESCE(p_workflow_id, gen_random_uuid()::TEXT);
        
        -- Call _init_workflow_status to enqueue the workflow
        PERFORM "%1$s"._init_workflow_status(
          v_workflow_id,
          'ENQUEUED',
          v_serialized_inputs,
          p_workflow_name,
          p_class_name,
          p_config_name,
          p_queue_name,
          p_deduplication_id,
          COALESCE(p_priority, 0),
          p_queue_partition_key,
          p_app_version,
          p_timeout_ms,
          p_deadline_epoch_ms,
          p_parent_workflow_id,
          'portable_json' -- serialization_format
        );
        
        -- Return the workflow ID
        RETURN v_workflow_id;
        
      EXCEPTION
        WHEN OTHERS THEN
          -- Re-raise any exceptions from _init_workflow_status
          RAISE;
      END;
      $$ LANGUAGE plpgsql;
      """;
}
