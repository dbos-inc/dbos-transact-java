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

import static org.junit.jupiter.api.Assertions.*;

public class BasicWorkflowTest {


    private static DBOSConfig dbosConfig;
    private static DBOS dbos ;
    private static SystemDatabase systemDatabase ;
    private static DBOSExecutor dbosExecutor;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        BasicWorkflowTest.dbosConfig = new DBOSConfig
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

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .build();

        String result = simpleService.workWithString("test-item");
        assertEquals("Processed: test-item", result);

        List<WorkflowStatus> wfs = systemDatabase.getWorkflows(new GetWorkflowsInput()) ;
        assertEquals(1, wfs.size());
        assertEquals(wfs.get(0).getName(),"workWithString");
        assertNotNull(wfs.get(0).getWorkflowId());
        assertEquals("test-item", wfs.get(0).getInput()[0]);
        assertEquals("Processed: test-item", wfs.get(0).getOutput());

    }

    @Test
    public void workflowWithError() throws SQLException{

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .build();

        try {
            simpleService.workWithError();
        } catch (Exception e) {
            assertEquals("DBOS Test error", e.getMessage());
        }

        List<WorkflowStatus> wfs = systemDatabase.getWorkflows(new GetWorkflowsInput()) ;
        assertEquals(1, wfs.size());
        assertEquals(wfs.get(0).getName(),"workError");
        assertNotNull(wfs.get(0).getWorkflowId());

    }

    @Test
    public void setWorkflowId() throws SQLException  {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .build();

        String result = null ;

        try (SetWorkflowID id = new SetWorkflowID("wf-123")){
            result = simpleService.workWithString("test-item");
        }

        assertEquals("Processed: test-item", result);

        List<WorkflowStatus> wfs = systemDatabase.getWorkflows(new GetWorkflowsInput()) ;
        assertEquals(1, wfs.size());
        assertEquals(wfs.get(0).getName(),"workWithString");
        assertEquals("wf-123",wfs.get(0).getWorkflowId());

    }


    @Test
    public void sameWorkflowId() throws SQLException  {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .build();

        String result = null ;

        SimpleServiceImpl.executionCount =0 ;

        try (SetWorkflowID id = new SetWorkflowID("wf-123")){
            result = simpleService.workWithString("test-item");
        }

        assertEquals("Processed: test-item", result);

        List<WorkflowStatus> wfs = systemDatabase.getWorkflows(new GetWorkflowsInput()) ;
        assertEquals(1, wfs.size());
        assertEquals(wfs.get(0).getName(),"workWithString");
        assertEquals("wf-123",wfs.get(0).getWorkflowId());

        assertEquals(1, SimpleServiceImpl.executionCount);

        try (SetWorkflowID id = new SetWorkflowID("wf-123")){
            result = simpleService.workWithString("test-item");
        }
        assertEquals(1, SimpleServiceImpl.executionCount);
        wfs = systemDatabase.getWorkflows(new GetWorkflowsInput()) ;
        assertEquals(1, wfs.size());
        assertEquals("wf-123",wfs.get(0).getWorkflowId());

        try (SetWorkflowID id = new SetWorkflowID("wf-124")){
            result = simpleService.workWithString("test-item");
        }

        assertEquals(2, SimpleServiceImpl.executionCount);
        wfs = systemDatabase.getWorkflows(new GetWorkflowsInput()) ;
        assertEquals(2, wfs.size());
        assertEquals("wf-124",wfs.get(1).getWorkflowId());

    }
}
