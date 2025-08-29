package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.InsertWorkflowResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.Connection;
import java.util.UUID;

import org.junit.jupiter.api.*;

class SystemDatabaseTest {

    private static SystemDatabase systemDatabase;
    private static DBOSConfig dbosConfig;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        SystemDatabaseTest.dbosConfig = new DBOSConfig.Builder().name("systemdbtest")
                .dbHost("localhost").dbPort(5432).dbUser("postgres").sysDbName("dbos_java_sys")
                .maximumPoolSize(3).build();

        DBUtils.recreateDB(dbosConfig);
        MigrationManager.runMigrations(dbosConfig);
        systemDatabase = new SystemDatabase(dbosConfig);
    }

    @AfterAll
    static void onetimeTearDown() throws Exception {
        systemDatabase.close();
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

        WorkflowStatusInternal wfStatusInternal = new WorkflowStatusInternal(workflowId,
                WorkflowState.SUCCESS, "OrderProcessingWorkflow",
                "com.example.workflows.OrderWorkflow", "prod-config", "user123@example.com",
                "admin", "admin,operator", "{\"result\":\"success\"}", null,
                System.currentTimeMillis() - 3600000, System.currentTimeMillis(), "order-queue",
                "exec-98765", "1.2.3", "order-app-123", 0, 300000l,
                System.currentTimeMillis() + 2400000, "dedup-112233", 1,
                "{\"orderId\":\"ORD-12345\"}");

        try (Connection conn = systemDatabase.getSysDBConnection()) {
            InsertWorkflowResult result = systemDatabase.insertWorkflowStatus(conn,
                    wfStatusInternal);

            assertNotNull(result);
            assertEquals(0, result.getRecoveryAttempts());
            assertEquals(wfStatusInternal.getStatus().toString(), result.getStatus());
            assertEquals(wfStatusInternal.getName(), result.getName());
            assertEquals(wfStatusInternal.getClassName(), result.getClassName());
            assertEquals(wfStatusInternal.getConfigName(), result.getConfigName());
            assertEquals(wfStatusInternal.getQueueName(), result.getQueueName());
            assertEquals(wfStatusInternal.getWorkflowDeadlineEpochMs(),
                    result.getWorkflowDeadlineEpochMs());
        }
    }

}
