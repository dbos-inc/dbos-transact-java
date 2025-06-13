package dev.dbos.transact.migration;

import dev.dbos.transact.config.DatabaseConfig;
import org.junit.jupiter.api.*;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class DatabaseMigratorTest {

    private static DataSource testDataSource;

    @BeforeAll
    static void setup() throws Exception {
        // Create admin connection to recreate test database
        String adminUrl = "jdbc:postgresql://localhost:5432/postgres";
        try (Connection conn = DriverManager.getConnection(adminUrl, "postgres", "postgres");
             Statement stmt = conn.createStatement()) {

            // Terminate existing connections to our test DB
            stmt.execute("SELECT pg_terminate_backend(pg_stat_activity.pid) " +
                    "FROM pg_stat_activity " +
                    "WHERE pg_stat_activity.datname = 'dbos_java_sys'");

            // Drop and recreate test database
            stmt.execute("DROP DATABASE IF EXISTS dbos_java_sys");
            stmt.execute("CREATE DATABASE dbos_java_sys");
        }

        // Initialize test data source
        testDataSource = DatabaseConfig.createDataSource();

        // Create schema (since it's a fresh DB)
        try (Connection conn = testDataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE SCHEMA IF NOT EXISTS dbos");
        }
    }

    @Test
    @Order(1)
    void testRunMigrations_CreatesTables() throws Exception {
        // Act
        DatabaseMigrator.runMigrations();

        // Assert
        try (Connection conn = testDataSource.getConnection()) {
            DatabaseMetaData metaData = conn.getMetaData();

            // Verify all expected tables exist in the dbos schema
            assertTableExists(metaData, "operation_outputs");
            assertTableExists(metaData, "workflow_inputs");
            assertTableExists(metaData, "workflow_status");
            assertTableExists(metaData, "notifications");
            assertTableExists(metaData, "workflow_events");
        }
    }

    private void assertTableExists(DatabaseMetaData metaData, String tableName) throws Exception {
        try (ResultSet rs = metaData.getTables(null, "dbos", tableName, null)) {
            assertTrue(rs.next(), "Table " + tableName + " should exist in schema dbos");
        }
    }

    @Test
    @Order(2)
    void testRunMigrations_IsIdempotent() {
        // Act - Run migrations again
        assertDoesNotThrow(() -> DatabaseMigrator.runMigrations(),
                "Migrations should run successfully multiple times");
    }

    @AfterAll
    static void cleanup() throws Exception {
        
    }
}