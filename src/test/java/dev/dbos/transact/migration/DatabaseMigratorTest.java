package dev.dbos.transact.migration;

import com.zaxxer.hikari.HikariDataSource;
import dev.dbos.transact.config.DBOSConfig;
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
    private static DBOSConfig dbosConfig;

    @BeforeAll
    static void setup() throws Exception {

        DatabaseMigratorTest.dbosConfig = new DBOSConfig
                .Builder()
                .name("migrationtest")
                .dbHost("localhost")
                .dbPort(5432)
                .dbUser("postgres")
                .sysDbName("dbos_java_sys")
                .maximumPoolSize(3)
                .build();

        String dbUrl = String.format("jdbc:postgresql://%s:%d/%s",dbosConfig.getDbHost(),dbosConfig.getDbPort(),"postgres") ;

        String sysDb = dbosConfig.getSysDbName();
        try (Connection conn = DriverManager.getConnection(dbUrl,dbosConfig.getDbUser(), dbosConfig.getDbPassword());
             Statement stmt = conn.createStatement()) {


            String dropDbSql = String.format("DROP DATABASE IF EXISTS %s",sysDb) ;
            String createDbSql = String.format("CREATE DATABASE %s",sysDb) ;
            stmt.execute(dropDbSql);
            stmt.execute(createDbSql);
        }

        testDataSource = dbosConfig.createDataSource(dbosConfig.getSysDbName());

    }

    @Test
    @Order(1)
    void testRunMigrations_CreatesTables() throws Exception {
        // Act
        DatabaseMigrator.runMigrations(dbosConfig);

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
        // Running migrations again
        assertDoesNotThrow(() -> DatabaseMigrator.runMigrations(dbosConfig),
                "Migrations should run successfully multiple times");
    }

    @AfterAll
    static void cleanup() throws Exception {
        ((HikariDataSource)testDataSource).close();
    }
}