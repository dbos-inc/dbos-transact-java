package dev.dbos.transact.database;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.migrations.DatabaseMigrator;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.InsertWorkflowResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;
import org.junit.jupiter.api.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class SystemDatabaseTest {

    private static SystemDatabase systemDatabase ;
    private static DBOSConfig dbosConfig;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        SystemDatabaseTest.dbosConfig = new DBOSConfig
                .Builder()
                .name("systemdbtest")
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


            String dropDbSql = String.format("DROP DATABASE IF EXISTS %s", sysDb);
            String createDbSql = String.format("CREATE DATABASE %s", sysDb);
            stmt.execute(dropDbSql);
            stmt.execute(createDbSql);
        }

        DatabaseMigrator.runMigrations(dbosConfig);
        SystemDatabase.initialize(dbosConfig);
        systemDatabase = SystemDatabase.getInstance();


    }

    @AfterAll
    static void onetimeTearDown() {
        SystemDatabase.destroy();
    }

    @BeforeEach
    void setUp() {
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void insertWorkflowStatusSuccess() throws Exception {

        String workflowId = UUID.randomUUID().toString();

        WorkflowStatusInternal wfStatusInternal = new WorkflowStatusInternal(
                workflowId,
                WorkflowState.SUCCESS,
                "OrderProcessingWorkflow",
                "com.example.workflows.OrderWorkflow",
                "prod-config",
                "user123@example.com",
                "admin",
                "admin,operator",
                "{\"result\":\"success\"}",
                null,
                System.currentTimeMillis() - 3600000,
                System.currentTimeMillis(),
                "order-queue",
                "exec-98765",
                "1.2.3",
                "order-app-123",
                0,
                300000l,
                System.currentTimeMillis() + 2400000,
                "dedup-112233",
                1,
                "{\"orderId\":\"ORD-12345\"}"
        );

        try (Connection conn = systemDatabase.getSysDBConnection()) {
            InsertWorkflowResult result = systemDatabase.insertWorkflowStatus(conn, wfStatusInternal);


            assertNotNull(result);
            assertEquals(0, result.getRecoveryAttempts());
            assertEquals(wfStatusInternal.getStatus().toString(), result.getStatus());
            assertEquals(wfStatusInternal.getName(), result.getName());
            assertEquals(wfStatusInternal.getClassName(), result.getClassName());
            assertEquals(wfStatusInternal.getConfigName(), result.getConfigName());
            assertEquals(wfStatusInternal.getQueueName(), result.getQueueName());
            assertEquals(wfStatusInternal.getWorkflowDeadlineEpochMs(), result.getWorkflowDeadlineEpochMs());

        }

    }
}