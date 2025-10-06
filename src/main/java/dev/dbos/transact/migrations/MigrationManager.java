package dev.dbos.transact.migrations;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MigrationManager {

  static Logger logger = LoggerFactory.getLogger(MigrationManager.class);
  private final DataSource dataSource;

  public MigrationManager(DataSource dataSource) {
    this.dataSource = dataSource;
  }

  public static void runMigrations(DBOSConfig config) {

    createDatabaseIfNotExists(Objects.requireNonNull(config));

    try (var ds = SystemDatabase.createDataSource(config)) {
      MigrationManager m = new MigrationManager(ds);
      m.migrate();
    }
  }

  public static void createDatabaseIfNotExists(DBOSConfig config) {
    var dbUrl = Objects.requireNonNull(config.databaseUrl());
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

      try (Statement stmt = conn.createStatement()) {
        stmt.executeUpdate("CREATE DATABASE \"" + pair.database() + "\"");
        logger.info("'{}' database created", pair.database());
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
    int qm = Objects.requireNonNull(url).indexOf('?');
    var base = qm >= 0 ? url.substring(0, qm) : url;
    var params = qm >= 0 ? url.substring(qm) : "";
    int slash = base.lastIndexOf('/');
    if (slash < "jdbc:postgresql://".length()) {
      throw new IllegalArgumentException();
    }

    var newUrl = base.substring(0, slash + 1) + "postgres" + params;
    var databaseName = base.substring(slash + 1);
    return new UrlPair(newUrl, databaseName);
  }

  public void migrate() {
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
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (Exception t) {
      logger.error("Migration error", t);
      throw new RuntimeException("Migration Error", t);
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
