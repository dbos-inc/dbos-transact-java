package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSOptions;
import dev.dbos.transact.context.SetDBOSOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.AwaitedWorkflowCancelledException;
import dev.dbos.transact.exceptions.WorkflowCancelledException;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.ExecutingService;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.utils.DBUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class WorkflowMgmtTest {

    Logger logger = LoggerFactory.getLogger(WorkflowMgmtTest.class);

    private static DBOSConfig dbosConfig;
    private static DataSource dataSource ;
    private DBOS dbos ;
    private static SystemDatabase systemDatabase ;
    private DBOSExecutor dbosExecutor;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        WorkflowMgmtTest.dbosConfig = new DBOSConfig
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
        WorkflowMgmtTest.dataSource = DBUtils.createDataSource(dbosConfig) ;
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
    public void asyncCancelResumeTest() throws Exception {

        CountDownLatch mainLatch = new CountDownLatch(1);
        CountDownLatch workLatch = new CountDownLatch(1);

        MgmtService mgmtService = dbos.<MgmtService>Workflow()
                .interfaceClass(MgmtService.class)
                .implementation(new MgmtServiceImpl(mainLatch, workLatch))
                .build();
        mgmtService.setMgmtService(mgmtService);

        String workflowId = "wfid1" ;
        DBOSOptions options = new DBOSOptions.Builder(workflowId).async().build();
        int result ;
        try (SetDBOSOptions o = new SetDBOSOptions(options)) {
            mgmtService.simpleWorkflow(23);
        }

        mainLatch.await();
        dbos.cancelWorkflow(workflowId);
        workLatch.countDown();

        assertEquals(1, mgmtService.getStepsExecuted()) ;
        WorkflowHandle h = dbosExecutor.retrieveWorkflow(workflowId) ;
        assertEquals(WorkflowState.CANCELLED.name(), h.getStatus().getStatus());

        WorkflowHandle<?> handle = dbos.resumeWorkflow(workflowId) ;

        result = (Integer) handle.getResult() ;
        assertEquals(23, result);
        assertEquals(3, mgmtService.getStepsExecuted()) ;

        // resume again

        handle = dbos.resumeWorkflow(workflowId) ;

        result = (Integer) handle.getResult() ;
        assertEquals(23, result);
        assertEquals(3, mgmtService.getStepsExecuted()) ;
        h = dbosExecutor.retrieveWorkflow(workflowId) ;
        assertEquals(WorkflowState.SUCCESS.name(), h.getStatus().getStatus());

        logger.info("Test completed");

    }

    @Test
    public void queuedCancelResumeTest() throws Exception {

        CountDownLatch mainLatch = new CountDownLatch(1);
        CountDownLatch workLatch = new CountDownLatch(1);

        MgmtService mgmtService = dbos.<MgmtService>Workflow()
                .interfaceClass(MgmtService.class)
                .implementation(new MgmtServiceImpl(mainLatch, workLatch))
                .build();
        mgmtService.setMgmtService(mgmtService);

        Queue myqueue = new DBOS.QueueBuilder("myqueue").build();

        String workflowId = "wfid1" ;
        DBOSOptions options = new DBOSOptions.Builder(workflowId).queue(myqueue).build();
        int result ;
        try (SetDBOSOptions o = new SetDBOSOptions(options)) {
            mgmtService.simpleWorkflow(23);
        }

        mainLatch.await();
        dbos.cancelWorkflow(workflowId);
        workLatch.countDown();

        assertEquals(1, mgmtService.getStepsExecuted()) ;
        WorkflowHandle h = dbosExecutor.retrieveWorkflow(workflowId) ;
        assertEquals(WorkflowState.CANCELLED.name(), h.getStatus().getStatus());

        WorkflowHandle<?> handle = dbos.resumeWorkflow(workflowId) ;

        result = (Integer) handle.getResult() ;
        assertEquals(23, result);
        assertEquals(3, mgmtService.getStepsExecuted()) ;

        // resume again

        handle = dbos.resumeWorkflow(workflowId) ;

        result = (Integer) handle.getResult() ;
        assertEquals(23, result);
        assertEquals(3, mgmtService.getStepsExecuted()) ;
        h = dbosExecutor.retrieveWorkflow(workflowId) ;
        assertEquals(WorkflowState.SUCCESS.name(), h.getStatus().getStatus());

        logger.info("Test completed");

    }


    @Test
    public void syncCancelResumeTest() throws Exception {

        CountDownLatch mainLatch = new CountDownLatch(1);
        CountDownLatch workLatch = new CountDownLatch(1);

        MgmtService mgmtService = dbos.<MgmtService>Workflow()
                .interfaceClass(MgmtService.class)
                .implementation(new MgmtServiceImpl(mainLatch, workLatch))
                .build();
        mgmtService.setMgmtService(mgmtService);


        ExecutorService e = Executors.newFixedThreadPool(2);
        String workflowId = "wfid1" ;

        CountDownLatch testLatch = new CountDownLatch(2) ;

        e.submit(() -> {

            DBOSOptions options = new DBOSOptions.Builder(workflowId).build();

            try {
                try (SetDBOSOptions o = new SetDBOSOptions(options)) {
                    mgmtService.simpleWorkflow(23);
                }
            } catch(Throwable t) {
                assertTrue(t instanceof AwaitedWorkflowCancelledException) ;
            }

            assertEquals(1, mgmtService.getStepsExecuted()) ;
            testLatch.countDown();
        }) ;

        e.submit(() -> {
            try {
                mainLatch.await();
                dbos.cancelWorkflow(workflowId);
                workLatch.countDown();
                testLatch.countDown();

            } catch(InterruptedException ie) {
                logger.error(ie.toString()) ;
            }
        }) ;

        testLatch.await();

        WorkflowHandle<?> handle = dbos.resumeWorkflow(workflowId) ;

        int result = (Integer) handle.getResult() ;
        assertEquals(23, result);
        assertEquals(3, mgmtService.getStepsExecuted()) ;

        // resume again

        handle = dbos.resumeWorkflow(workflowId) ;

        result = (Integer) handle.getResult() ;
        assertEquals(23, result);
        assertEquals(3, mgmtService.getStepsExecuted()) ;

        logger.info("Test completed");

    }


}
