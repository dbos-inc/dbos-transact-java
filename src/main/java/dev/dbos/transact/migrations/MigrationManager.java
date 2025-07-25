package dev.dbos.transact.migrations;


import com.zaxxer.hikari.HikariDataSource;
import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.DBOSException;
import dev.dbos.transact.exceptions.ErrorCode;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class MigrationManager {

    static Logger logger = LoggerFactory.getLogger(MigrationManager.class) ;
    private final DataSource dataSource;


    public MigrationManager(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public static void runMigrations(DBOSConfig dbconfig) {

        if (dbconfig == null) {
            logger.warn("No database configuration. Skipping migration");
            return ;
        }

        String dbName;
        if (dbconfig.getSysDbName() != null) {
            dbName = dbconfig.getSysDbName();
        } else {
            dbName = dbconfig.getName() + Constants.SYS_DB_SUFFIX;
        }

        createDatabaseIfNotExists(dbconfig, dbName);

        DataSource dataSource = dbconfig.createDataSource(dbName);

        try {
            MigrationManager m = new MigrationManager(dataSource);
            m.migrate();
        } catch(Exception e) {
            throw new DBOSException(ErrorCode.UNEXPECTED.getCode(), e.getMessage()) ;

        } finally {
            ((HikariDataSource)dataSource).close();
        }


        logger.info("Database migrations completed successfully");
    }

    public static void createDatabaseIfNotExists(DBOSConfig config, String dbName) {
        DataSource adminDS = config.createDataSource(Constants.POSTGRES_DEFAULT_DB);
        try {
            try (Connection conn = adminDS.getConnection();
                 PreparedStatement ps = conn.prepareStatement(
                         "SELECT 1 FROM pg_database WHERE datname = ?")) {

                ps.setString(1, dbName);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        logger.info("Database '{}' already exists", dbName);
                        // DatabaseMigrator.createSchemaIfNotExists(config, dbName, Constants.DB_SCHEMA);
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
            ((HikariDataSource)adminDS).close() ;
        }

    }

    public void migrate() throws Exception {
        try (Connection conn = dataSource.getConnection()) {
            ensureMigrationTable(conn);
            Set<String> appliedMigrations = getAppliedMigrations(conn);
            List<Path> migrationFiles = listMigrationFiles();

            for (Path path : migrationFiles) {
                String filename = path.getFileName().toString();
                logger.info("processing migration file " + filename);
                String version = filename.split("_")[0];

                if (!appliedMigrations.contains(version)) {
                    System.out.println("Applying migration: " + filename);
                    applyMigrationFile(conn, path);
                    markMigrationApplied(conn, version);
                }
            }
        }
    }

    private void ensureMigrationTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS migration_history ( " +
                    "version TEXT PRIMARY KEY, " +
                    " applied_at TIMESTAMPTZ DEFAULT now() )");
        }
    }

    private Set<String> getAppliedMigrations(Connection conn) throws SQLException {
        Set<String> applied = new HashSet<>();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT version FROM migration_history")) {
            while (rs.next()) {
                applied.add(rs.getString("version"));
            }
        }
        return applied;
    }

    private void markMigrationApplied(Connection conn, String version) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(
                "INSERT INTO migration_history (version) VALUES (?)")) {
            ps.setString(1, version);
            ps.executeUpdate();
        }
    }

    private List<Path> listMigrationFiles() throws IOException, URISyntaxException {
        URL resource = getClass().getClassLoader().getResource("db/migrations");
        if (resource == null) {
            throw new IllegalStateException("Migration folder not found in classpath");
        }

        Path dir = Paths.get(resource.toURI());
        return Files.list(dir)
                .filter(p -> p.getFileName().toString().matches("\\d+_.*\\.sql"))
                .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                .collect(Collectors.toList());
    }

    private void applyMigrationFile(Connection conn, Path filePath) throws IOException, SQLException {
        String sql = Files.readString(filePath).trim();
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

