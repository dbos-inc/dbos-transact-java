package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.*;

public class SyncWorkflowTest {

    private static DBOSConfig dbosConfig;
    private static DataSource dataSource;
    private DBOS dbos;
    private static SystemDatabase systemDatabase;
    private DBOSExecutor dbosExecutor;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        SyncWorkflowTest.dbosConfig = new DBOSConfig.Builder().name("systemdbtest")
                .dbHost("localhost").dbPort(5432).dbUser("postgres").sysDbName("dbos_java_sys")
                .maximumPoolSize(2).runAdminServer().adminAwaitOnStart(false).build();
    }

    @BeforeEach
    void beforeEachTest() throws SQLException {
        DBUtils.recreateDB(dbosConfig);
        SyncWorkflowTest.dataSource = SystemDatabase.createDataSource(dbosConfig, null);
        systemDatabase = new SystemDatabase(dataSource);
        this.dbosExecutor = new DBOSExecutor(dbosConfig, systemDatabase);
        dbos = DBOS.initialize(dbosConfig, systemDatabase, dbosExecutor, null, null);
        dbos.launch();
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dbos.shutdown();
    }

    @Test
    public void workflowWithOneInput() throws SQLException {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class).implementation(new SimpleServiceImpl())
                .build();

        String result = simpleService.workWithString("test-item");
        assertEquals("Processed: test-item", result);

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
        assertEquals(1, wfs.size());
        assertEquals(wfs.get(0).getName(), "workWithString");
        assertNotNull(wfs.get(0).getWorkflowId());
        assertEquals("test-item", wfs.get(0).getInput()[0]);
        assertEquals("Processed: test-item", wfs.get(0).getOutput());
    }

    @Test
    public void workflowWithError() throws SQLException {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class).implementation(new SimpleServiceImpl())
                .build();

        try {
            simpleService.workWithError();
        } catch (Exception e) {
            assertEquals("DBOS Test error", e.getMessage());
        }

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
        assertEquals(1, wfs.size());
        assertEquals(wfs.get(0).getName(), "workError");
        assertNotNull(wfs.get(0).getWorkflowId());
    }

    @Test
    public void setWorkflowId() throws SQLException {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class).implementation(new SimpleServiceImpl())
                .build();

        String result = null;

        try (SetWorkflowID id = new SetWorkflowID("wf-123")) {
            result = simpleService.workWithString("test-item");
        }

        assertEquals("Processed: test-item", result);

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
        assertEquals(1, wfs.size());
        assertEquals(wfs.get(0).getName(), "workWithString");
        assertEquals("wf-123", wfs.get(0).getWorkflowId());
    }

    @Test
    public void sameWorkflowId() throws SQLException {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class).implementation(new SimpleServiceImpl())
                .build();

        String result = null;
        SimpleServiceImpl.executionCount = 0;

        try (SetWorkflowID id = new SetWorkflowID("wf-123")) {
            result = simpleService.workWithString("test-item");
        }

        assertEquals("Processed: test-item", result);

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
        assertEquals(1, wfs.size());
        assertEquals(wfs.get(0).getName(), "workWithString");
        assertEquals("wf-123", wfs.get(0).getWorkflowId());

        assertEquals(1, SimpleServiceImpl.executionCount);

        try (SetWorkflowID id = new SetWorkflowID("wf-123")) {
            result = simpleService.workWithString("test-item");
        }
        assertEquals(1, SimpleServiceImpl.executionCount);
        wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
        assertEquals(1, wfs.size());
        assertEquals("wf-123", wfs.get(0).getWorkflowId());

        try (SetWorkflowID id = new SetWorkflowID("wf-124")) {
            result = simpleService.workWithString("test-item");
        }

        assertEquals(2, SimpleServiceImpl.executionCount);
        wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
        assertEquals(2, wfs.size());
        assertEquals("wf-124", wfs.get(1).getWorkflowId());
    }

    @Test
    public void childWorkflowWithoutSet() throws Exception {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class).implementation(new SimpleServiceImpl())
                .build();

        simpleService.setSimpleService(simpleService);

        String result = null;

        try (SetWorkflowID id = new SetWorkflowID("wf-123456")) {
            result = simpleService.parentWorkflowWithoutSet("123");
        }

        assertEquals("123abc", result);

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());

        assertEquals(2, wfs.size());
        assertEquals("wf-123456", wfs.get(0).getWorkflowId());
        assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).getStatus());

        assertEquals("wf-123456-0", wfs.get(1).getWorkflowId());
        assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).getStatus());

        List<StepInfo> steps = systemDatabase.listWorkflowSteps("wf-123456");
        assertEquals(1, steps.size());
        assertEquals("wf-123456-0", steps.get(0).getChildWorkflowId());
        assertEquals(0, steps.get(0).getFunctionId());
        assertEquals("childWorkflow", steps.get(0).getFunctionName());
    }

    @Test
    public void multipleChildren() throws Exception {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class).implementation(new SimpleServiceImpl())
                .build();

        simpleService.setSimpleService(simpleService);

        String result = null;

        try (SetWorkflowID id = new SetWorkflowID("wf-123456")) {
            simpleService.WorkflowWithMultipleChildren("123");
        }

        WorkflowHandle<?> handle = dbosExecutor.retrieveWorkflow("wf-123456");
        assertEquals("123abcdefghi", handle.getResult());

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());

        assertEquals(4, wfs.size());
        assertEquals("wf-123456", wfs.get(0).getWorkflowId());
        assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).getStatus());

        assertEquals("child1", wfs.get(1).getWorkflowId());
        assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).getStatus());

        assertEquals("child2", wfs.get(2).getWorkflowId());
        assertEquals(WorkflowState.SUCCESS.name(), wfs.get(2).getStatus());

        assertEquals("child3", wfs.get(3).getWorkflowId());
        assertEquals(WorkflowState.SUCCESS.name(), wfs.get(3).getStatus());

        List<StepInfo> steps = systemDatabase.listWorkflowSteps("wf-123456");
        assertEquals(3, steps.size());
        assertEquals("child1", steps.get(0).getChildWorkflowId());
        assertEquals(0, steps.get(0).getFunctionId());
        assertEquals("childWorkflow", steps.get(0).getFunctionName());

        assertEquals("child2", steps.get(1).getChildWorkflowId());
        assertEquals(1, steps.get(1).getFunctionId());
        assertEquals("childWorkflow2", steps.get(1).getFunctionName());

        assertEquals("child3", steps.get(2).getChildWorkflowId());
        assertEquals(2, steps.get(2).getFunctionId());
        assertEquals("childWorkflow3", steps.get(2).getFunctionName());
    }

    @Test
    public void nestedChildren() throws Exception {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class).implementation(new SimpleServiceImpl())
                .build();

        simpleService.setSimpleService(simpleService);

        String result = null;

        try (SetWorkflowID id = new SetWorkflowID("wf-123456")) {
            simpleService.grandParent("123");
        }

        WorkflowHandle<?> handle = dbosExecutor.retrieveWorkflow("wf-123456");
        assertEquals("p-c-gc-123", handle.getResult());

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());

        assertEquals(3, wfs.size());
        assertEquals("wf-123456", wfs.get(0).getWorkflowId());
        assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).getStatus());

        assertEquals("child4", wfs.get(1).getWorkflowId());
        assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).getStatus());

        assertEquals("child5", wfs.get(2).getWorkflowId());
        assertEquals(WorkflowState.SUCCESS.name(), wfs.get(2).getStatus());

        List<StepInfo> steps = systemDatabase.listWorkflowSteps("wf-123456");
        assertEquals(1, steps.size());
        assertEquals("child4", steps.get(0).getChildWorkflowId());
        assertEquals(0, steps.get(0).getFunctionId());
        assertEquals("childWorkflow4", steps.get(0).getFunctionName());

        steps = systemDatabase.listWorkflowSteps("child4");
        assertEquals(1, steps.size());
        assertEquals("child5", steps.get(0).getChildWorkflowId());
        assertEquals(0, steps.get(0).getFunctionId());
        assertEquals("grandchildWorkflow", steps.get(0).getFunctionName());
    }
}
