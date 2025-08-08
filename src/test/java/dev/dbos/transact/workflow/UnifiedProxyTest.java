package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowOptions;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class UnifiedProxyTest {

    private static DBOSConfig dbosConfig;
    private static DataSource dataSource;
    private DBOS dbos;
    private static SystemDatabase systemDatabase;
    private DBOSExecutor dbosExecutor;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        UnifiedProxyTest.dbosConfig = new DBOSConfig.Builder().name("systemdbtest")
                .dbHost("localhost").dbPort(5432).dbUser("postgres").sysDbName("dbos_java_sys")
                .maximumPoolSize(2).build();
    }

    @BeforeEach
    void beforeEachTest() throws SQLException {
        DBUtils.recreateDB(dbosConfig);
        UnifiedProxyTest.dataSource = SystemDatabase.createDataSource(dbosConfig);
        systemDatabase = new SystemDatabase(dataSource);
        dbosExecutor = new DBOSExecutor(dbosConfig, systemDatabase);
        dbos = DBOS.initialize(dbosConfig, systemDatabase, dbosExecutor, null, null);
        dbos.launch();
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dbos.shutdown();
    }

    @Test
    public void optionsWithCall() throws Exception {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class).implementation(new SimpleServiceImpl())
                .build();

        // synchronous
        String wfid1 = "wf-123";
        WorkflowOptions options = new WorkflowOptions.Builder(wfid1).build();
        String result;
        try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
            result = simpleService.workWithString("test-item");
        }
        assertEquals("Processed: test-item", result);

        // asynchronous

        String wfid2 = "wf-124";
        options = new WorkflowOptions.Builder(wfid2).build();
        WorkflowHandle<String> handle = null;
        try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
            handle = dbos.startWorkflow(() -> simpleService.workWithString("test-item-async"));
        }

        result = handle.getResult();
        assertEquals("Processed: test-item-async", result);
        assertEquals(wfid2, handle.getWorkflowId());
        assertEquals("SUCCESS", handle.getStatus().getStatus());

        // Queued
        String wfid3 = "wf-125";
        Queue q = new DBOS.QueueBuilder("simpleQ").build();
        options = new WorkflowOptions.Builder(wfid3).queue(q).build();
        try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
            result = simpleService.workWithString("test-item-q");
        }
        assertNull(result);

        handle = dbosExecutor.retrieveWorkflow(wfid3);;
        result = (String) handle.getResult();
        assertEquals("Processed: test-item-q", result);
        assertEquals(wfid3, handle.getWorkflowId());
        assertEquals("SUCCESS", handle.getStatus().getStatus());

        ListWorkflowsInput input = new ListWorkflowsInput();
        input.setWorkflowIDs(Arrays.asList(wfid3));
        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(input);
        assertEquals("simpleQ", wfs.get(0).getQueueName());
    }

    @Test
    public void syncParentWithQueuedChildren() throws Exception {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class).implementation(new SimpleServiceImpl())
                .build();

        simpleService.setSimpleService(simpleService);

        String wfid1 = "wf-123";
        WorkflowOptions options = new WorkflowOptions.Builder(wfid1).build();
        String result;
        try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
            result = simpleService.syncWithQueued();
        }
        assertEquals("QueuedChildren", result);

        for (int i = 0; i < 3; i++) {
            String wid = "child" + i;
            WorkflowHandle h = dbos.retrieveWorkflow(wid);
            assertEquals(wid, h.getResult());
        }

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
        assertEquals(wfs.size(), 4);

        assertEquals(wfid1, wfs.get(0).getWorkflowId());
        assertEquals("child0", wfs.get(1).getWorkflowId());
        assertEquals("childQ", wfs.get(1).getQueueName());
        assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).getStatus());

        assertEquals("child1", wfs.get(2).getWorkflowId());
        assertEquals("childQ", wfs.get(2).getQueueName());
        assertEquals(WorkflowState.SUCCESS.name(), wfs.get(2).getStatus());

        assertEquals("child2", wfs.get(3).getWorkflowId());
        assertEquals("childQ", wfs.get(3).getQueueName());
        assertEquals(WorkflowState.SUCCESS.name(), wfs.get(3).getStatus());
    }
}
