package dev.dbos.transact.execution;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.NonExistentWorkflowException;
import dev.dbos.transact.exceptions.WorkflowFunctionNotFoundException;
import dev.dbos.transact.utlils.DBUtils;
import dev.dbos.transact.workflow.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class DBOSExecutorTest {

    private static DBOSConfig dbosConfig;
    private static DataSource dataSource ;
    private DBOS dbos ;
    private static SystemDatabase systemDatabase ;
    private DBOSExecutor dbosExecutor;

    @BeforeAll
    public static void onetimeBefore() throws SQLException {

        DBOSExecutorTest.dbosConfig = new DBOSConfig
                .Builder()
                .name("systemdbtest")
                .dbHost("localhost")
                .dbPort(5432)
                .dbUser("postgres")
                .sysDbName("dbos_java_sys")
                .maximumPoolSize(2)
                .build();

        String sysDb = dbosConfig.getSysDbName();
        String dbUrl = String.format("jdbc:postgresql://%s:%d/%s", dbosConfig.getDbHost(), dbosConfig.getDbPort(), "postgres");
        try (Connection conn = DriverManager.getConnection(dbUrl, dbosConfig.getDbUser(), dbosConfig.getDbPassword());
             Statement stmt = conn.createStatement()) {

            String dropDbSql = String.format("DROP DATABASE IF EXISTS %s", sysDb);
            String createDbSql = String.format("CREATE DATABASE %s", sysDb);
            stmt.execute(dropDbSql);
            stmt.execute(createDbSql);
        }



    }

    @BeforeEach
    void setUp() throws SQLException{
        DBOS.initialize(dbosConfig);
        dbos = DBOS.getInstance();
        DBOSExecutorTest.dataSource = DBUtils.createDataSource(dbosConfig) ;
        SystemDatabase.initialize(dataSource);
        systemDatabase = SystemDatabase.getInstance();
        dbosExecutor = new DBOSExecutor(dbosConfig, systemDatabase);
        dbos.setDbosExecutor(dbosExecutor);
        dbos.launch();
        systemDatabase.deleteOperations();
        systemDatabase.deleteWorkflowsTestHelper();
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dbos.shutdown();
    }

    @Test
    void executeWorkflowById() throws Exception {

       ExecutingService executingService = dbos.<ExecutingService>Workflow()
                .interfaceClass(ExecutingService.class)
                .implementation(new ExecutingServiceImpl())
                .build();

        String result = null ;

        String wfid = "wf-123";
        try (SetWorkflowID id = new SetWorkflowID(wfid)){
            result = executingService.workflowMethod("test-item");
        }

        assertEquals("test-itemtest-item", result);

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput()) ;
        assertEquals(wfs.get(0).getStatus(), WorkflowState.SUCCESS.name());

        setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());


        WorkflowHandle<String> handle = dbosExecutor.executeWorkflowById(wfid);

        result = handle.getResult();
        assertEquals("test-itemtest-item", result);
        assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus()) ;

        wfs = systemDatabase.listWorkflows(new ListWorkflowsInput()) ;
        assertEquals(wfs.get(0).getStatus(), WorkflowState.SUCCESS.name());



    }

    @Test
    void executeWorkflowByIdNonExistent() throws Exception {

        ExecutingService executingService = dbos.<ExecutingService>Workflow()
                .interfaceClass(ExecutingService.class)
                .implementation(new ExecutingServiceImpl())
                .build();

        String result = null ;

        String wfid = "wf-123";
        try (SetWorkflowID id = new SetWorkflowID(wfid)){
            result = executingService.workflowMethod("test-item");
        }

        assertEquals("test-itemtest-item", result);

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput()) ;
        assertEquals(wfs.get(0).getStatus(), WorkflowState.SUCCESS.name());



        boolean error = false;
        try {
            WorkflowHandle<String> handle = dbosExecutor.executeWorkflowById("wf-124");
        } catch (Exception e) {
            error = true;
            assert e instanceof NonExistentWorkflowException : "Expected NonExistentWorkflowException but got " + e.getClass().getName();
        }

        assertTrue(error);

    }

    @Test
    void workflowFunctionNotfound() throws Exception {

        ExecutingService executingService = dbos.<ExecutingService>Workflow()
                .interfaceClass(ExecutingService.class)
                .implementation(new ExecutingServiceImpl())
                .build();

        String result = null ;

        String wfid = "wf-123";
        try (SetWorkflowID id = new SetWorkflowID(wfid)){
            result = executingService.workflowMethod("test-item");
        }

        assertEquals("test-itemtest-item", result);

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput()) ;
        assertEquals(wfs.get(0).getStatus(), WorkflowState.SUCCESS.name());

        dbos.shutdown(); // clear out the registry
        startDBOS(); // restart dbos

        boolean error = false;
        try {
            WorkflowHandle<String> handle = dbosExecutor.executeWorkflowById(wfid);
        } catch (Exception e) {
            error = true;
            assert e instanceof WorkflowFunctionNotFoundException : "Expected WorkflowFunctionNotfoundException but got " + e.getClass().getName();
        }

        assertTrue(error);

    }


    private void setWorkflowState(DataSource ds, String workflowId, String newState) throws SQLException {

        String sql = "UPDATE dbos.workflow_status SET status = ?, updated_at = ? WHERE workflow_uuid = ?";

        try (Connection connection = ds.getConnection();
             PreparedStatement pstmt = connection.prepareStatement(sql)) {

            pstmt.setString(1, newState);
            pstmt.setLong(2, Instant.now().toEpochMilli());
            pstmt.setString(3, workflowId);

            // Execute the update and get the number of rows affected
            int rowsAffected = pstmt.executeUpdate();

            assertEquals(1, rowsAffected);

        }
    }

    void startDBOS() throws SQLException{
        DBOS.initialize(dbosConfig);
        dbos = DBOS.getInstance();
        DBOSExecutorTest.dataSource = DBUtils.createDataSource(dbosConfig) ;
        SystemDatabase.initialize(dataSource);
        systemDatabase = SystemDatabase.getInstance();
        dbosExecutor = new DBOSExecutor(dbosConfig, systemDatabase);
        dbos.setDbosExecutor(dbosExecutor);
        dbos.launch();
    }
}
