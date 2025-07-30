package dev.dbos.transact.workflow;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSOptions;
import dev.dbos.transact.context.SetDBOSOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.AwaitedWorkflowCancelledException;
import dev.dbos.transact.exceptions.NonExistentWorkflowException;
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
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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



    @Test
    public void forkNonExistent() {

        try {
            WorkflowHandle<?> rstatHandle = dbos.forkWorkflow("12345", 2);
            fail("An exceptions should have been thrown");
        } catch (Throwable t) {
            logger.info(t.getClass().getName()) ;
            assertTrue(t instanceof NonExistentWorkflowException);
        }

    }

    @Test
    public void testFork() {

        ForkServiceImpl impl = new ForkServiceImpl();

        ForkService forkService = dbos.<ForkService>Workflow()
                .interfaceClass(ForkService.class)
                .implementation(impl)
                .build();
        forkService.setForkService(forkService);

        String workflowId = "wfid1" ;
        DBOSOptions options = new DBOSOptions.Builder(workflowId).build();
        String result ;
        try (SetDBOSOptions o = new SetDBOSOptions(options)) {
            result = forkService.simpleWorkflow("hello");
        }

        assertEquals("hellohello", result);
        WorkflowHandle<?> handle = dbosExecutor.retrieveWorkflow(workflowId);
        assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());

        assertEquals(1, impl.step1Count) ;
        assertEquals(1, impl.step2Count) ;
        assertEquals(1, impl.step3Count) ;
        assertEquals(1, impl.step4Count) ;
        assertEquals(1, impl.step5Count) ;

        logger.info("First execution done starting fork") ;

        WorkflowHandle<?> rstatHandle = dbos.forkWorkflow(workflowId, 0);
        result = (String) rstatHandle.getResult() ;
        assertEquals("hellohello", result);
        assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().getStatus());
        assertTrue(rstatHandle.getWorkflowId() != workflowId);

        assertEquals(2, impl.step1Count) ;
        assertEquals(2, impl.step2Count) ;
        assertEquals(2, impl.step3Count) ;
        assertEquals(2, impl.step4Count) ;
        assertEquals(2, impl.step5Count) ;

        List<StepInfo> steps = systemDatabase.listWorkflowSteps(rstatHandle.getWorkflowId()) ;
        assertEquals(5, steps.size()) ;

        logger.info("first fork done . starting 2nd fork ") ;

        rstatHandle = dbos.forkWorkflow(workflowId, 2);
        result = (String) rstatHandle.getResult() ;
        assertEquals("hellohello", result);
        assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().getStatus());
        assertTrue(rstatHandle.getWorkflowId() != workflowId);

        assertEquals(2, impl.step1Count) ;
        assertEquals(2, impl.step2Count) ;
        assertEquals(3, impl.step3Count) ;
        assertEquals(3, impl.step4Count) ;
        assertEquals(3, impl.step5Count) ;

        logger.info("Second fork done . starting 3rd fork ") ;

        rstatHandle = dbos.forkWorkflow(workflowId, 4);
        result = (String) rstatHandle.getResult() ;
        assertEquals("hellohello", result);
        assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().getStatus());
        assertTrue(rstatHandle.getWorkflowId() != workflowId);

        assertEquals(2, impl.step1Count) ;
        assertEquals(2, impl.step2Count) ;
        assertEquals(3, impl.step3Count) ;
        assertEquals(3, impl.step4Count) ;
        assertEquals(4, impl.step5Count) ;

    }

    @Test
    public void testParentChildFork() {

        ForkServiceImpl impl = new ForkServiceImpl();

        ForkService forkService = dbos.<ForkService>Workflow()
                .interfaceClass(ForkService.class)
                .implementation(impl)
                .build();
        forkService.setForkService(forkService);

        String workflowId = "wfid1" ;
        DBOSOptions options = new DBOSOptions.Builder(workflowId).build();
        String result ;
        try (SetDBOSOptions o = new SetDBOSOptions(options)) {
            result = forkService.parentChild("hello");
        }

        assertEquals("hellohello", result);
        WorkflowHandle<?> handle = dbosExecutor.retrieveWorkflow(workflowId);
        assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());

        assertEquals(1, impl.step1Count) ;
        assertEquals(1, impl.step2Count) ;
        assertEquals(1, impl.child1Count) ;
        assertEquals(1, impl.child2Count) ;
        assertEquals(1, impl.step5Count) ;

        List<StepInfo> stepsRun0 = systemDatabase.listWorkflowSteps(workflowId) ;
        assertEquals(5, stepsRun0.size()) ;

        logger.info("First execution done starting fork") ;

        WorkflowHandle<?> rstatHandle = dbos.forkWorkflow(workflowId, 0);
        result = (String) rstatHandle.getResult() ;
        assertEquals("hellohello", result);
        assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().getStatus());
        assertTrue(rstatHandle.getWorkflowId() != workflowId);

        assertEquals(2, impl.step1Count) ;
        assertEquals(2, impl.step2Count) ;
        assertEquals(1, impl.child1Count) ;
        assertEquals(1, impl.child2Count) ;
        assertEquals(2, impl.step5Count) ;

        List<StepInfo> steps = systemDatabase.listWorkflowSteps(rstatHandle.getWorkflowId()) ;
        assertEquals(5, steps.size()) ;

        assertTrue(stepsRun0.get(2).getChildWorkflowId().equals(steps.get(2).getChildWorkflowId()));
        assertTrue(stepsRun0.get(3).getChildWorkflowId().equals(steps.get(3).getChildWorkflowId()));

        logger.info("First execution done starting 2nd fork");

        rstatHandle = dbos.forkWorkflow(workflowId, 3);
        result = (String) rstatHandle.getResult() ;
        assertEquals("hellohello", result);
        assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().getStatus());
        assertTrue(rstatHandle.getWorkflowId() != workflowId);

        assertEquals(2, impl.step1Count) ;
        assertEquals(2, impl.step2Count) ;
        assertEquals(1, impl.child1Count) ;
        assertEquals(1, impl.child2Count) ;
        assertEquals(3, impl.step5Count) ;

        steps = systemDatabase.listWorkflowSteps(rstatHandle.getWorkflowId()) ;
        assertEquals(5, steps.size()) ;

        logger.info(stepsRun0.get(2).getChildWorkflowId() ) ;
        logger.info(steps.get(2).getChildWorkflowId()) ;
        assertTrue(stepsRun0.get(2).getChildWorkflowId().equals(steps.get(2).getChildWorkflowId()));
        assertTrue(stepsRun0.get(3).getChildWorkflowId().equals(steps.get(3).getChildWorkflowId()));

        logger.info("First execution done starting 2nd fork");

        rstatHandle = dbos.forkWorkflow(workflowId, 4);
        result = (String) rstatHandle.getResult() ;
        assertEquals("hellohello", result);
        assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().getStatus());
        assertTrue(rstatHandle.getWorkflowId() != workflowId);

        assertEquals(2, impl.step1Count) ;
        assertEquals(2, impl.step2Count) ;
        assertEquals(1, impl.child1Count) ;
        assertEquals(1, impl.child2Count) ;
        assertEquals(4, impl.step5Count) ;

        steps = systemDatabase.listWorkflowSteps(rstatHandle.getWorkflowId()) ;
        assertEquals(5, steps.size()) ;

        assertTrue(stepsRun0.get(2).getChildWorkflowId().equals(steps.get(2).getChildWorkflowId()));
        assertTrue(stepsRun0.get(3).getChildWorkflowId().equals(steps.get(3).getChildWorkflowId()));

        logger.info("First execution done starting 2nd fork");

    }

    @Test
    public void testParentChildAsyncFork() {

        ForkServiceImpl impl = new ForkServiceImpl();

        ForkService forkService = dbos.<ForkService>Workflow()
                .interfaceClass(ForkService.class)
                .implementation(impl)
                .build();
        forkService.setForkService(forkService);

        String workflowId = "wfid1";
        DBOSOptions options = new DBOSOptions.Builder(workflowId).build();
        String result;
        try (SetDBOSOptions o = new SetDBOSOptions(options)) {
            result = forkService.parentChildAsync("hello");
        }

        assertEquals("hellohello", result);
        WorkflowHandle<?> handle = dbosExecutor.retrieveWorkflow(workflowId);
        assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());

        assertEquals(1, impl.step1Count);
        assertEquals(1, impl.step2Count);
        assertEquals(1, impl.child1Count);
        assertEquals(1, impl.child2Count);
        assertEquals(1, impl.step5Count);

        List<StepInfo> stepsRun0 = systemDatabase.listWorkflowSteps(workflowId);
        assertEquals(5, stepsRun0.size());

        logger.info("First execution done starting fork");

        WorkflowHandle<?> rstatHandle = dbos.forkWorkflow(workflowId, 3);
        result = (String) rstatHandle.getResult();

        assertEquals("hellohello", result);
        assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().getStatus());
        assertTrue(rstatHandle.getWorkflowId() != workflowId);

        assertEquals(1, impl.step1Count);
        assertEquals(1, impl.step2Count);
        assertEquals(1, impl.child1Count);
        assertEquals(1, impl.child2Count); // 1 because the wf already executed even if we did not copy the step
        assertEquals(2, impl.step5Count);

        List<StepInfo> steps = systemDatabase.listWorkflowSteps(rstatHandle.getWorkflowId());
        assertEquals(5, steps.size());

        assertTrue(stepsRun0.get(2).getChildWorkflowId().equals(steps.get(2).getChildWorkflowId()));
        assertTrue(stepsRun0.get(3).getChildWorkflowId().equals(steps.get(3).getChildWorkflowId()));

    }

}
