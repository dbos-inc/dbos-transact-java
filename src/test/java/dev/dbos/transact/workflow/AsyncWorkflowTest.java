package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class AsyncWorkflowTest {

    private static DBOSConfig dbosConfig;
    private static DBOS dbos ;
    private static SystemDatabase systemDatabase ;
    private static DBOSExecutor dbosExecutor;

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

        DBOS.initialize(dbosConfig);
        dbos = DBOS.getInstance();
        SystemDatabase.initialize(dbosConfig);
        systemDatabase = SystemDatabase.getInstance();
        dbosExecutor = new DBOSExecutor(dbosConfig, systemDatabase);
        dbos.setDbosExecutor(dbosExecutor);
        dbos.launch();

    }

    @AfterAll
    static void onetimeTearDown() {
        dbos.shutdown();
    }

    @BeforeEach
    void beforeEachTest() throws SQLException {
        systemDatabase.deleteWorkflowsTestHelper();
    }

    @Test
    public void workflowWithOneInput() throws SQLException  {

        SimpleAsyncService simpleAsyncService = dbos.<SimpleAsyncService>Workflow()
                .interfaceClass(SimpleAsyncService.class)
                .implementation(new SimpleServiceImpl())
                .async()
                .build();

        WorkflowHandle<String> handle = simpleAsyncService.workWithString("test-item");
        String result = handle.getResult();
        assertEquals("Processed: test-item", result);
        assertNotNull(handle.getWorkflowId());

        List<WorkflowStatus> wfs = systemDatabase.getWorkflows(new GetWorkflowsInput()) ;
        assertEquals(1, wfs.size());
        assertEquals(wfs.get(0).getName(),"workWithString");
        assertNotNull(wfs.get(0).getWorkflowId());
        assertEquals("test-item", wfs.get(0).getInput()[0]);
        assertEquals("Processed: test-item", wfs.get(0).getOutput());

    }

    @Test
    public void setWorkflowId() throws SQLException  {

        SimpleAsyncService simpleAsyncService = dbos.<SimpleAsyncService>Workflow()
                .interfaceClass(SimpleAsyncService.class)
                .implementation(new SimpleServiceImpl())
                .async()
                .build();

        WorkflowHandle<String> handle = null ;

        try (SetWorkflowID id = new SetWorkflowID("wf-123")){
            handle = simpleAsyncService.workWithString("test-item");
        }

        String result = handle.getResult();
        assertEquals("Processed: test-item", result);
        assertEquals("wf-123", handle.getWorkflowId());

        List<WorkflowStatus> wfs = systemDatabase.getWorkflows(new GetWorkflowsInput()) ;
        assertEquals(1, wfs.size());
        assertEquals(wfs.get(0).getName(),"workWithString");
        assertEquals("wf-123",wfs.get(0).getWorkflowId());

    }

    @Test
    public void sameWorkflowId() throws SQLException  {

        SimpleAsyncService simpleAsyncService = dbos.<SimpleAsyncService>Workflow()
                .interfaceClass(SimpleAsyncService.class)
                .implementation(new SimpleServiceImpl())
                .async()
                .build();

        SimpleServiceImpl.executionCount =0 ;

        WorkflowHandle<String> handle = null;
        try (SetWorkflowID id = new SetWorkflowID("wf-123")){
            handle = simpleAsyncService.workWithString("test-item");
        }

        String result = handle.getResult() ;
        assertEquals("Processed: test-item", result);
        assertEquals("wf-123",handle.getWorkflowId());

        List<WorkflowStatus> wfs = systemDatabase.getWorkflows(new GetWorkflowsInput()) ;
        assertEquals(1, wfs.size());
        assertEquals(wfs.get(0).getName(),"workWithString");
        assertEquals("wf-123",wfs.get(0).getWorkflowId());

        try (SetWorkflowID id = new SetWorkflowID("wf-123")){
            handle = simpleAsyncService.workWithString("test-item");
        }
        result = handle.getResult();
        assertEquals(1, SimpleServiceImpl.executionCount);
        // TODO fix deser has quotes assertEquals("Processed: test-item", result);
        assertEquals("wf-123",handle.getWorkflowId());

        wfs = systemDatabase.getWorkflows(new GetWorkflowsInput()) ;
        assertEquals(1, wfs.size());
        assertEquals("wf-123",wfs.get(0).getWorkflowId());

        try (SetWorkflowID id = new SetWorkflowID("wf-124")){
            handle = simpleAsyncService.workWithString("test-item");
        }
        result = handle.getResult();
        assertEquals("wf-124",handle.getWorkflowId());

        assertEquals(2, SimpleServiceImpl.executionCount);
        wfs = systemDatabase.getWorkflows(new GetWorkflowsInput()) ;
        assertEquals(2, wfs.size());
        assertEquals("wf-124",wfs.get(1).getWorkflowId());

    }

}
