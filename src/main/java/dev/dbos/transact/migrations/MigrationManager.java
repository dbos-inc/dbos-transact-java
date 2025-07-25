package dev.dbos.transact.migrations;


import com.zaxxer.hikari.HikariDataSource;
import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.DBOSException;
import dev.dbos.transact.exceptions.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
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
        logger.info("in migrate") ;

        if (dataSource == null) {
            logger.info("datasource is null cannot migrate") ;
            return ;
        } else {
            logger.info("dataSource in not null") ;
        }

        try {
            try (Connection conn = dataSource.getConnection()) {

                if (conn == null) {
                    logger.info("conn is null cannot migrate") ;
                    return ;
                }


                ensureMigrationTable(conn);
                logger.info("done with ensureMigration");

                Set<String> appliedMigrations = getAppliedMigrations(conn);
                logger.info("getAppliedMigrations") ;
                List<MigrationFile> migrationFiles = loadMigrationFiles();
                logger.info("files found == " + migrationFiles.size());

                for (MigrationFile migrationFile : migrationFiles) {
                    String filename = migrationFile.getFilename().toString();
                    logger.info("processing migration file " + filename);
                    String version = filename.split("_")[0];

                    if (!appliedMigrations.contains(version)) {
                        System.out.println("Applying migration: " + filename);
                        applyMigrationFile(conn, migrationFile.getSql());
                        markMigrationApplied(conn, version);
                    }
                }
            }
        } catch(SQLException e ) {
            logger.error("mjjjj " + e.getMessage()) ;
        } catch(Throwable t) {
            t.printStackTrace();
            logger.error("mjjjj " + t.getMessage()) ;
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
                        .map(p -> {
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
                    .map(p -> {
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
        //  String sql = Files.readString(filePath).trim();
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

