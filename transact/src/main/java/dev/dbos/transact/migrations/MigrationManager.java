package dev.dbos.transact.migrations;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Objects;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

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
    List<String> migrations = new ArrayList<>();
    int migrationNum = 1;
    while (true) {
      String resourceName = "/db/migrations/migration" + migrationNum + ".sql";
      try (InputStream is = MigrationManager.class.getResourceAsStream(resourceName)) {
        if (is == null) {
          break;
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
          StringBuilder sb = new StringBuilder();
          String line;
          while ((line = reader.readLine()) != null) {
            sb.append(line).append("\n");
          }
          migrations.add(sb.toString().formatted(schema));
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to load migration resource: " + resourceName, e);
      }
      migrationNum++;
    }
    return migrations;
  }
}
