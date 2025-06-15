package dev.dbos.transact.migration;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;

public class DatabaseMigrator {

    static Logger logger = LoggerFactory.getLogger(DatabaseMigrator.class) ;

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

        DatabaseMigrator.createDatabaseIfNotExists(dbconfig, dbName);

        DataSource dataSource = dbconfig.createDataSource(dbName);


        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .schemas(Constants.DB_SCHEMA)
                .defaultSchema(Constants.DB_SCHEMA)
                .locations("classpath:db/migration")
                .load();

        flyway.migrate();
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
                        DatabaseMigrator.createSchemaIfNotExists(config, dbName, Constants.DB_SCHEMA);
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

    /**
     * Creates the schema if it does not already exist.
     *
     * @param dbName The name of the database to create schema in
     * @param schemaName The name of the schema to ensure exists.
     * @throws SQLException if an error occurs while executing SQL.
     */
    public static void createSchemaIfNotExists(DBOSConfig config, String dbName, String schemaName) throws SQLException {
        DataSource ds = config.createDataSource(dbName);
        try (Connection conn = ds.getConnection();
             Statement stmt = conn.createStatement()) {

            String sql = "CREATE SCHEMA IF NOT EXISTS " + schemaName;
            stmt.execute(sql);

        } catch (SQLException e) {
            throw new RuntimeException("Failed to create schema: " + schemaName, e);
        } finally {
            if (ds instanceof HikariDataSource) {
                ((HikariDataSource) ds).close();
            }
        }
    }

}
