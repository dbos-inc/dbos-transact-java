package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.context.SetWorkflowOptions;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;

import java.sql.*;
import java.time.Instant;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RecoveryServiceTest {

    private static DBOSConfig dbosConfig;
    private static DataSource dataSource;
    private DBOS dbos;
    private static SystemDatabase systemDatabase;
    private DBOSExecutor dbosExecutor;
    private RecoveryService recoveryService;
    Logger logger = LoggerFactory.getLogger(RecoveryServiceTest.class);

    @BeforeAll
    public static void onetimeBefore() throws SQLException {

        RecoveryServiceTest.dbosConfig = new DBOSConfig.Builder().name("systemdbtest")
                .dbHost("localhost").dbPort(5432).dbUser("postgres").sysDbName("dbos_java_sys")
                .maximumPoolSize(2).build();
    }

    @BeforeEach
    void setUp() throws SQLException {
        DBUtils.recreateDB(dbosConfig);
        RecoveryServiceTest.dataSource = SystemDatabase.createDataSource(dbosConfig);
        systemDatabase = new SystemDatabase(dataSource);
        dbosExecutor = new DBOSExecutor(dbosConfig, systemDatabase);
        recoveryService = new RecoveryService(dbosExecutor, systemDatabase);
        dbos = DBOS.initialize(dbosConfig, systemDatabase, dbosExecutor, null, null);
        dbos.launch();
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dbos.shutdown();
    }

    @Test
    void recoverWorkflows() throws Exception {

        ExecutingService executingService = dbos.<ExecutingService>Workflow()
                .interfaceClass(ExecutingService.class).implementation(new ExecutingServiceImpl())
                .build();

        String wfid = "wf-123";
        try (SetWorkflowID id = new SetWorkflowID(wfid)) {
            executingService.workflowMethod("test-item");
        }
        wfid = "wf-124";
        try (SetWorkflowID id = new SetWorkflowID(wfid)) {
            executingService.workflowMethod("test-item");
        }
        wfid = "wf-125";
        try (SetWorkflowID id = new SetWorkflowID(wfid)) {
            executingService.workflowMethod("test-item");
        }
        wfid = "wf-126";
        WorkflowHandle<String> handle6 = null;
        try (SetWorkflowID id = new SetWorkflowID(wfid)) {
            handle6 = dbos.startWorkflow(() -> executingService.workflowMethod("test-item"));
        }
        handle6.getResult();

        wfid = "wf-127";
        WorkflowHandle<String> handle7 = null;
        Queue q = new DBOS.QueueBuilder("q1").build();
        WorkflowOptions options = new WorkflowOptions.Builder(wfid).queue(q).build();
        try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
            handle7 = dbos.startWorkflow(() -> executingService.workflowMethod("test-item"));
        }
        assertEquals("q1", handle7.getStatus().getQueueName());
        handle7.getResult();

        setWorkflowStateToPending(dataSource);

        List<GetPendingWorkflowsOutput> pending = recoveryService.getPendingWorkflows();
        assertEquals(5, pending.size());

        List<WorkflowHandle> recoveredHandles = recoveryService.recoverWorkflows(pending);
        assertEquals(5, recoveredHandles.size());

        recoveredHandles.forEach((handle) -> {
            try {
                handle.getResult();
                assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());
            } catch (Exception e) {
                assertTrue(false); // fail the test
            }
        });
    }

    @Test
    public void recoveryThreadTest() throws SQLException {

        ExecutingService executingService = dbos.<ExecutingService>Workflow()
                .interfaceClass(ExecutingService.class).implementation(new ExecutingServiceImpl())
                .build();

        String wfid = "wf-123";
        try (SetWorkflowID id = new SetWorkflowID(wfid)) {
            executingService.workflowMethod("test-item");
        }
        wfid = "wf-124";
        try (SetWorkflowID id = new SetWorkflowID(wfid)) {
            executingService.workflowMethod("test-item");
        }

        setWorkflowStateToPending(dataSource);

        WorkflowStatus s = systemDatabase.getWorkflowStatus("wf-123");
        assertEquals(WorkflowState.PENDING.name(), s.getStatus());

        dbos.shutdown();

        dbos = DBOS.initialize(dbosConfig);
        // dbos = DBOS.getInstance();

        // need to register again
        // towatch: we are registering after launch. could lead to a race condition
        // toimprove : allow registration before launch
        executingService = dbos.<ExecutingService>Workflow().interfaceClass(ExecutingService.class)
                .implementation(new ExecutingServiceImpl()).build();

        dbos.launch();

        WorkflowHandle h = DBOS.getInstance().retrieveWorkflow("wf-123");
        h.getResult();
        assertEquals(WorkflowState.SUCCESS.name(), h.getStatus().getStatus());

        h = dbos.retrieveWorkflow("wf-124");
        h.getResult();
        assertEquals(WorkflowState.SUCCESS.name(), h.getStatus().getStatus());
    }

    private void setWorkflowStateToPending(DataSource ds) throws SQLException {

        String sql = "UPDATE dbos.workflow_status SET status = ?, updated_at = ? ;";

        try (Connection connection = ds.getConnection();
                PreparedStatement pstmt = connection.prepareStatement(sql)) {

            pstmt.setString(1, WorkflowState.PENDING.name());
            pstmt.setLong(2, Instant.now().toEpochMilli());

            // Execute the update and get the number of rows affected
            int rowsAffected = pstmt.executeUpdate();

            logger.info("Number of workflows made pending " + rowsAffected);
        }
    }
}
