package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSOptions;
import dev.dbos.transact.context.SetDBOSOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.AwaitedWorkflowCancelledException;
import dev.dbos.transact.exceptions.WorkflowCancelledException;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.utils.DBUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class TimeoutTest {

    private static DBOSConfig dbosConfig;
    private static DataSource dataSource ;
    private DBOS dbos ;
    private static SystemDatabase systemDatabase ;
    private DBOSExecutor dbosExecutor;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        TimeoutTest.dbosConfig = new DBOSConfig
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
        TimeoutTest.dataSource = DBUtils.createDataSource(dbosConfig) ;
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
    public void async() throws Exception  {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .build();
        simpleService.setSimpleService(simpleService);

        // asynchronous

        String wfid1 = "wf-124";
        String result;

        DBOSOptions options = new DBOSOptions.Builder(wfid1).async().timeout(3).build();
        try (SetDBOSOptions id = new SetDBOSOptions(options)){
            result = simpleService.longWorkflow("12345");
        }
        assertNull(result);

        WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow(wfid1); ;
        result = handle.getResult();
        assertEquals("1234512345", result);
        assertEquals(wfid1, handle.getWorkflowId());
        assertEquals("SUCCESS", handle.getStatus().getStatus()) ;

    }

    @Test
    public void asyncTimedOut() {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .build();
        simpleService.setSimpleService(simpleService);

        // make it timeout
        String wfid1 = "wf-125";
        String result ;
        DBOSOptions options = new DBOSOptions.Builder(wfid1).async().timeout(1).build();
        try (SetDBOSOptions id = new SetDBOSOptions(options)){
            result = simpleService.longWorkflow("12345");
        }
        assertNull(result);
        WorkflowHandle handle = dbosExecutor.retrieveWorkflow(wfid1);

        try {
            handle.getResult();
            fail("Expected Exception to be thrown");
        } catch (Throwable t) {
            System.out.println(t.getClass().toString()) ;
            assertTrue(t instanceof AwaitedWorkflowCancelledException) ;
        }

        WorkflowStatus s = systemDatabase.getWorkflowStatus(wfid1);
        assertEquals(WorkflowState.CANCELLED.name(), s.getStatus()) ;
    }

    @Test
    public void queued() throws Exception  {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .build();
        simpleService.setSimpleService(simpleService);

        // queued

        String wfid1 = "wf-126";
        String result;

        Queue simpleQ = new DBOS.QueueBuilder("simpleQ")
                .build();

        DBOSOptions options = new DBOSOptions.Builder(wfid1).queue(simpleQ).timeout(3).build();
        try (SetDBOSOptions id = new SetDBOSOptions(options)){
            result = simpleService.longWorkflow("12345");
        }
        assertNull(result);

        WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow(wfid1); ;
        result = handle.getResult();
        assertEquals("1234512345", result);
        assertEquals(wfid1, handle.getWorkflowId());
        assertEquals("SUCCESS", handle.getStatus().getStatus()) ;

    }

    @Test
    public void queuedTimedOut() {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .build();
        simpleService.setSimpleService(simpleService);

        // make it timeout
        String wfid1 = "wf-127";
        String result ;

        Queue simpleQ = new DBOS.QueueBuilder("simpleQ")
                .build();

        DBOSOptions options = new DBOSOptions.Builder(wfid1).queue(simpleQ).timeout(1).build();
        try (SetDBOSOptions id = new SetDBOSOptions(options)){
            result = simpleService.longWorkflow("12345");
        }
        assertNull(result);
        WorkflowHandle handle = dbosExecutor.retrieveWorkflow(wfid1);

        try {
            handle.getResult();
            fail("Expected Exception to be thrown");
        } catch (Throwable t) {
            System.out.println(t.getClass().toString()) ;
            assertTrue(t instanceof AwaitedWorkflowCancelledException) ;
        }

        WorkflowStatus s = systemDatabase.getWorkflowStatus(wfid1);
        assertEquals(WorkflowState.CANCELLED.name(), s.getStatus()) ;
    }

    @Test
    public void sync() throws Exception  {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .build();
        simpleService.setSimpleService(simpleService);

        // synchronous

        String wfid1 = "wf-128";
        String result;

        DBOSOptions options = new DBOSOptions.Builder(wfid1).timeout(3).build();

        try (SetDBOSOptions id = new SetDBOSOptions(options)){
            result = simpleService.longWorkflow("12345");
        }
        assertEquals("1234512345", result);

        WorkflowStatus s = systemDatabase.getWorkflowStatus(wfid1);
        assertEquals(WorkflowState.SUCCESS.name(), s.getStatus()) ;

    }

    @Test
    public void syncTimeout() throws Exception  {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .build();
        simpleService.setSimpleService(simpleService);

        // synchronous

        String wfid1 = "wf-128";
        String result = null;

        DBOSOptions options = new DBOSOptions.Builder(wfid1).timeout(1).build();

        try {
            try (SetDBOSOptions id = new SetDBOSOptions(options)) {
                result = simpleService.longWorkflow("12345");
            }
        } catch (Throwable t) {
            assertNull(result);
            assertTrue(t instanceof AwaitedWorkflowCancelledException) ;
        }

        WorkflowStatus s = systemDatabase.getWorkflowStatus(wfid1);
        assertEquals(WorkflowState.CANCELLED.name(), s.getStatus()) ;

    }

    @Test
    public void recovery() throws Exception {

        SimpleService simpleService = dbos.<SimpleService>Workflow()
                .interfaceClass(SimpleService.class)
                .implementation(new SimpleServiceImpl())
                .build();
        simpleService.setSimpleService(simpleService);

        // synchronous

        String wfid1 = "wf-128";
        String result;

        DBOSOptions options = new DBOSOptions.Builder(wfid1).timeout(3).build();

        try (SetDBOSOptions id = new SetDBOSOptions(options)){
            result = simpleService.workWithString("12345");
        }

        setDelayEpoch(dataSource, wfid1);

        WorkflowHandle handle = dbosExecutor.executeWorkflowById(wfid1) ;
        assertEquals(WorkflowState.CANCELLED.name(), handle.getStatus().getStatus()) ;


    }

    private void setDelayEpoch(DataSource ds, String workflowId) throws SQLException {

        String sql = "UPDATE dbos.workflow_status SET status = ?, updated_at = ?, workflow_deadline_epoch_ms = ? WHERE workflow_uuid = ?";

        try (Connection connection = ds.getConnection();
             PreparedStatement pstmt = connection.prepareStatement(sql)) {

            pstmt.setString(1, WorkflowState.PENDING.name());
            pstmt.setLong(2, Instant.now().toEpochMilli());

            long newEpoch = System.currentTimeMillis() - 10000 ;
            pstmt.setLong(3, newEpoch);
            pstmt.setString(4, workflowId);

            // Execute the update and get the number of rows affected
            int rowsAffected = pstmt.executeUpdate();

            assertEquals(1, rowsAffected);

        }
    }

}
