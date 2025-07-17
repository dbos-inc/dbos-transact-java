package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSOptions;
import dev.dbos.transact.context.SetDBOSOptions;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSAppException;
import dev.dbos.transact.exceptions.SerializableException;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.utils.DBUtils;
import org.junit.jupiter.api.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AsyncWorkflowTest {

    private static DBOSConfig dbosConfig;
    private static DataSource dataSource ;
    private DBOS dbos ;
    private static SystemDatabase systemDatabase ;
    private DBOSExecutor dbosExecutor;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        AsyncWorkflowTest.dbosConfig = new DBOSConfig
                .Builder()
                .name("systemdbtest")
                .dbHost("localhost")
                .dbPort(5432)
                .dbUser("postgres")
                .sysDbName("dbos_java_sys")
                .maximumPoolSize(2)
                .build();

        String dbUrl = String.format("jdbc:postgresql://%s:%d/%s", dbosConfig.getDbHost(), dbosConfig.getDbPort(), "postgres");

        String sysDb = dbosConfig.getSysDbName();
        try (Connection conn = DriverManager.getConnection(dbUrl, dbosConfig.getDbUser(), dbosConfig.getDbPassword());
             Statement stmt = conn.createStatement()) {


            String dropDbSql = String.format("DROP DATABASE IF EXISTS %s", sysDb);
            String createDbSql = String.format("CREATE DATABASE %s", sysDb);
            stmt.execute(dropDbSql);
            stmt.execute(createDbSql);
        }

    }

    @BeforeEach
    void beforeEachTest() throws SQLException {
        AsyncWorkflowTest.dataSource = DBUtils.createDataSource(dbosConfig) ;
        DBOS.initialize(dbosConfig);
        dbos = DBOS.getInstance();
        SystemDatabase.initialize(dataSource);
        systemDatabase = SystemDatabase.getInstance();
        dbosExecutor = new DBOSExecutor(dbosConfig, systemDatabase);
        dbos.setDbosExecutor(dbosExecutor);
        dbos.launch();
        DBUtils.clearTables(dataSource);
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dbos.shutdown();
    }


    @Test
    public void setWorkflowId() throws Exception  {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .async()
                .build();


        String wfid = "wf-123";
        try (SetWorkflowID id = new SetWorkflowID(wfid)){
            simpleService.workWithString("test-item");
        }

        WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow(wfid); ;
        String result = handle.getResult();
        assertEquals("Processed: test-item", result);
        assertEquals("wf-123", handle.getWorkflowId());
        assertEquals("SUCCESS", handle.getStatus().getStatus()) ;

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput()) ;
        assertEquals(1, wfs.size());
        assertEquals(wfs.get(0).getName(),"workWithString");
        assertEquals("wf-123",wfs.get(0).getWorkflowId());

    }

   @Test
    public void sameWorkflowId() throws Exception  {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .async()
                .build();

        SimpleServiceImpl.executionCount =0 ;

        String wfid = "wf-123";
        try (SetWorkflowID id = new SetWorkflowID(wfid)){
            simpleService.workWithString("test-item");
        }

        WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow(wfid);
        String result = handle.getResult() ;
        assertEquals("Processed: test-item", result);
        assertEquals("wf-123",handle.getWorkflowId());

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput()) ;
        assertEquals(1, wfs.size());
        assertEquals(wfs.get(0).getName(),"workWithString");
        assertEquals("wf-123",wfs.get(0).getWorkflowId());

        try (SetWorkflowID id = new SetWorkflowID("wf-123")){
            simpleService.workWithString("test-item");
        }
       handle = dbosExecutor.retrieveWorkflow(wfid);
        result = handle.getResult();
        assertEquals(1, SimpleServiceImpl.executionCount);
        // TODO fix deser has quotes assertEquals("Processed: test-item", result);
        assertEquals("wf-123",handle.getWorkflowId());

        wfs = systemDatabase.listWorkflows(new ListWorkflowsInput()) ;
        assertEquals(1, wfs.size());
        assertEquals("wf-123",wfs.get(0).getWorkflowId());

        String wfid2 = "wf-124" ;
        try (SetWorkflowID id = new SetWorkflowID(wfid2)){
            simpleService.workWithString("test-item");
        }

        handle = dbosExecutor.retrieveWorkflow(wfid2);
        result = handle.getResult();
        assertEquals("wf-124",handle.getWorkflowId());

        assertEquals(2, SimpleServiceImpl.executionCount);
        wfs = systemDatabase.listWorkflows(new ListWorkflowsInput()) ;
        assertEquals(2, wfs.size());
        assertEquals("wf-124",wfs.get(1).getWorkflowId());

    }


    @Test
    public void workflowWithError() throws Exception {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .async()
                .build();

        WorkflowHandle handle = null;
        String wfid = "abc" ;
        try (SetWorkflowID id = new SetWorkflowID(wfid)) {
            simpleService.workWithError();
        }

        handle = dbosExecutor.retrieveWorkflow(wfid);
        try {
            handle.getResult();
        } catch (DBOSAppException e) {
            assertEquals("Exception of type java.lang.Exception", e.getMessage());
            SerializableException se = e.original;
            assertEquals("DBOS Test error", se.message);
        }
        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput()) ;
        assertEquals(1, wfs.size());
        assertEquals(wfs.get(0).getName(),"workError");
        assertNotNull(wfs.get(0).getWorkflowId());
        assertEquals(wfs.get(0).getWorkflowId(),handle.getWorkflowId());
        assertEquals(WorkflowState.ERROR.name(), handle.getStatus().getStatus());

    }

    @Test
    public void childWorkflowWithoutSet() throws Exception{

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .async()
                .build();

        simpleService.setSimpleService(simpleService);

        try (SetWorkflowID id = new SetWorkflowID("wf-123456")){
            simpleService.parentWorkflowWithoutSet("123");
        }

        WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow("wf-123456");
        System.out.println(handle.getResult());

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput()) ;

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
    public void multipleChildren() throws Exception{

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
        //        .async() unified instead
                .build();

        simpleService.setSimpleService(simpleService);

        DBOSOptions options = new DBOSOptions.Builder("wf-123456").async().build();

        // try (SetWorkflowID id = new SetWorkflowID("wf-123456")){
        try(SetDBOSOptions o = new SetDBOSOptions(options)) {
            simpleService.WorkflowWithMultipleChildren("123");
        }

        WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow("wf-123456");
        assertEquals("123abcdefghi", handle.getResult());

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput()) ;

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
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .async()
                .build();

        simpleService.setSimpleService(simpleService);

        try (SetWorkflowID id = new SetWorkflowID("wf-123456")) {
            simpleService.grandParent("123");
        }

        WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow("wf-123456");
        assertEquals("p-c-gc-123",handle.getResult());

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput()) ;

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
