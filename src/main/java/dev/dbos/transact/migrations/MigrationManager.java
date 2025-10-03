package dev.dbos.transact.migrations;

import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSException;
import dev.dbos.transact.exceptions.ErrorCode;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationManager {

  static Logger logger = LoggerFactory.getLogger(MigrationManager.class);
  private final DataSource dataSource;

  public MigrationManager(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public static void runMigrations(DBOSConfig dbconfig) {

    if (dbconfig == null) {
      logger.warn("No database configuration. Skipping migration");
      return;
    }

    String dbName;
    if (dbconfig.sysDbName() != null) {
      dbName = dbconfig.sysDbName();
    } else {
      dbName = dbconfig.appName() + Constants.SYS_DB_SUFFIX;
    }

    createDatabaseIfNotExists(dbconfig, dbName);

    DataSource dataSource = SystemDatabase.createDataSource(dbconfig, dbName);

    try {
      MigrationManager m = new MigrationManager(dataSource);
      m.migrate();
    } catch (Exception e) {
      throw new DBOSException(ErrorCode.UNEXPECTED.getCode(), e.getMessage());

    } finally {
      ((HikariDataSource) dataSource).close();
    }

    logger.info("Database migrations completed successfully");
  }

  public static void createDatabaseIfNotExists(DBOSConfig config, String dbName) {
    DataSource adminDS = SystemDatabase.createPostgresDataSource(config);
    try {
      try (Connection conn = adminDS.getConnection();
          PreparedStatement ps =
              conn.prepareStatement("SELECT 1 FROM pg_database WHERE datname = ?")) {

        ps.setString(1, dbName);
        try (ResultSet rs = ps.executeQuery()) {
          if (rs.next()) {
            logger.info("Database '{}' already exists", dbName);
            // createSchemaIfNotExists(config, dbName, Constants.DB_SCHEMA);
            return;
          }
        }

        try (Statement stmt = conn.createStatement()) {
          stmt.executeUpdate("CREATE DATABASE \"" + dbName + "\"");
          logger.info("Database '{}' created successfully", dbName);
        }
      } catch (SQLException e) {
        throw new RuntimeException("Failed to check or create database: " + dbName, e);
      }
    } finally {
      // temporary
      // later keep the datasource global so we are not recreating them
      ((HikariDataSource) adminDS).close();
    }
  }

  public void migrate() throws Exception {

    try {
      try (Connection conn = dataSource.getConnection()) {

        ensureMigrationTable(conn);

        Set<String> appliedMigrations = getAppliedMigrations(conn);
        List<MigrationFile> migrationFiles = loadMigrationFiles();
        for (MigrationFile migrationFile : migrationFiles) {
          String filename = migrationFile.getFilename().toString();
          logger.info("processing migration file {}", filename);
          String version = filename.split("_")[0];

          if (!appliedMigrations.contains(version)) {
            applyMigrationFile(conn, migrationFile.getSql());
            markMigrationApplied(conn, version);
          }
        }
      }
    } catch (Exception t) {
      logger.error("Migration error", t);
      throw t;
    }
  }

  private void ensureMigrationTable(Connection conn) throws SQLException {
    try (Statement stmt = conn.createStatement()) {

      stmt.execute("CREATE SCHEMA IF NOT EXISTS dbos");

      stmt.execute(
          "CREATE TABLE IF NOT EXISTS dbos.migration_history (version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ DEFAULT now() )");
    }
  }

  private Set<String> getAppliedMigrations(Connection conn) throws SQLException {
    Set<String> applied = new HashSet<>();
    try (Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT version FROM dbos.migration_history")) {
      while (rs.next()) {
        applied.add(rs.getString("version"));
      }
    }
    return applied;
  }

  private void markMigrationApplied(Connection conn, String version) throws SQLException {
    try (PreparedStatement ps =
        conn.prepareStatement("INSERT INTO dbos.migration_history (version) VALUES (?)")) {
      ps.setString(1, version);
      ps.executeUpdate();
    }
  }

  private List<MigrationFile> loadMigrationFiles() throws IOException, URISyntaxException {
    String migrationsPath = "db/migrations";
    URL resource = getClass().getClassLoader().getResource(migrationsPath);
    if (resource == null) {
      logger.error("db/migrations not found");
      throw new IllegalStateException("Migration folder not found in classpath");
    }

    URI uri = resource.toURI();

    if ("jar".equals(uri.getScheme())) {
      try (FileSystem fs = FileSystems.newFileSystem(uri, Collections.emptyMap())) {
        Path pathInJar = fs.getPath("/" + migrationsPath);
        return Files.list(pathInJar)
            .filter(p -> p.getFileName().toString().matches("\\d+_.*\\.sql"))
            .sorted(Comparator.comparing(p -> p.getFileName().toString()))
            .map(
                p -> {
                  try {
                    String filename = p.getFileName().toString();
                    String sql = Files.readString(p);
                    return new MigrationFile(filename, sql);
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                })
            .collect(Collectors.toList());
      }
    } else {
      Path path = Paths.get(uri);
      return Files.list(path)
          .filter(p -> p.getFileName().toString().matches("\\d+_.*\\.sql"))
          .sorted(Comparator.comparing(p -> p.getFileName().toString()))
          .map(
              p -> {
                try {
                  String filename = p.getFileName().toString();
                  String sql = Files.readString(p);
                  return new MigrationFile(filename, sql);
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              })
          .collect(Collectors.toList());
    }
  }

  private void applyMigrationFile(Connection conn, String sql) throws IOException, SQLException {
    if (sql.isEmpty()) return;

    boolean originalAutoCommit = conn.getAutoCommit();
    conn.setAutoCommit(false);

    try (Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
      conn.commit();
    } catch (SQLException ex) {
      conn.rollback();
      throw ex;
    } finally {
      conn.setAutoCommit(originalAutoCommit);
    }
  }
}
