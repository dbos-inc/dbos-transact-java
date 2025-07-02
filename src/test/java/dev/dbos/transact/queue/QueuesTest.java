package dev.dbos.transact.queue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.step.StepsTest;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.InsertWorkflowResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class QueuesTest {

    Logger logger = LoggerFactory.getLogger(QueuesTest.class);

    private static DBOSConfig dbosConfig;
    private static DataSource dataSource;
    private static DBOS dbos ;
    private static SystemDatabase systemDatabase ;
    private static DBOSExecutor dbosExecutor;
    private static QueueService queueService ;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        QueuesTest.dbosConfig = new DBOSConfig
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
        dataSource = DBUtils.createDataSource(dbosConfig);
        DBOS.initialize(dbosConfig);
        dbos = DBOS.getInstance();
        SystemDatabase.initialize(dataSource);
        systemDatabase = SystemDatabase.getInstance();
        dbosExecutor = new DBOSExecutor(dbosConfig, systemDatabase);
        dbos.setDbosExecutor(dbosExecutor);
        dbos.launch();
        // TODO: move before launch after launch has a wait
        queueService = new QueueService(systemDatabase);
        queueService.setDbosExecutor(dbosExecutor);
        dbos.setQueueService(queueService);
        queueService.start();


        DBUtils.clearTables(dataSource);
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        queueService.stop();
        dbos.shutdown();
    }

    @Test
    public void testQueuedWorkflow() throws Exception {

        Queue firstQ = new DBOS.QueueBuilder("firstQueue")
                .concurrency(1)
                .workerConcurrency(1)
                .build();


        ServiceQ serviceQ = new DBOS.WorkflowBuilder<ServiceQ>()
                .interfaceClass(ServiceQ.class)
                .implementation(new ServiceQImpl())
                .queue(firstQ)
                .build() ;

        String id = "q1234" ;

        try (SetWorkflowID ctx = new SetWorkflowID(id)) {
            serviceQ.simpleQWorkflow("inputq");
        }

        WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow(id);
        assertEquals(id, handle.getWorkflowId());
        String result = handle.getResult();
        assertEquals("inputqinputq",result) ;

    }

    @Test
    public void testQueuedMultipleWorkflows() throws Exception {

        queueService.stop();
        while(!queueService.isStopped()) {
            Thread.sleep(2000);
            logger.info("Waiting for queueService to stop") ;
        }

        Queue firstQ = new DBOS.QueueBuilder("firstQueue")
                .concurrency(1)
                .workerConcurrency(1)
                .build();

        ServiceQ serviceQ = new DBOS.WorkflowBuilder<ServiceQ>()
                .interfaceClass(ServiceQ.class)
                .implementation(new ServiceQImpl())
                .queue(firstQ)
                .build() ;


        for (int i = 0 ; i < 5 ; i++) {
            String id = "wfid"+i;

            try (SetWorkflowID ctx = new SetWorkflowID(id)) {
                serviceQ.simpleQWorkflow("inputq"+i);
            }

        }

        List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput()) ;

        for (int i = 0 ; i < 5 ; i++) {
            String id = "wfid"+i;

            assertEquals(id, wfs.get(i).getWorkflowId());
            assertEquals(WorkflowState.ENQUEUED.name(), wfs.get(i).getStatus());

        }

        queueService.start();

        for (int i = 0 ; i < 5 ; i++) {
            String id = "wfid"+i;

            WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow(id);
            assertEquals(id, handle.getWorkflowId());
            String result = handle.getResult();
            assertEquals("inputq"+i+"inputq"+i,result) ;
            assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());

        }

    }

    @Test
    public void multipleQueues() throws Exception{

        Queue firstQ = new DBOS.QueueBuilder("firstQueue")
                .concurrency(1)
                .workerConcurrency(1)
                .build();


        ServiceQ serviceQ1 = new DBOS.WorkflowBuilder<ServiceQ>()
                .interfaceClass(ServiceQ.class)
                .implementation(new ServiceQImpl())
                .queue(firstQ)
                .build() ;

        Queue secondQ = new DBOS.QueueBuilder("secondQueue")
                .concurrency(1)
                .workerConcurrency(1)
                .build();

        ServiceI serviceI = new DBOS.WorkflowBuilder<ServiceI>()
                .interfaceClass(ServiceI.class)
                .implementation(new ServiceIImpl())
                .queue(secondQ)
                .build() ;


        String id1 = "firstQ1234" ;
        String id2 = "second1234" ;

        try (SetWorkflowID ctx = new SetWorkflowID(id1)) {
            serviceQ1.simpleQWorkflow("firstinput");
        }

        try (SetWorkflowID ctx = new SetWorkflowID(id2)) {
            serviceI.workflowI(25);
        }

        WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow(id1);
        assertEquals(id1, handle.getWorkflowId());
        String result = handle.getResult();
        assertEquals("firstinputfirstinput",result) ;
        assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());

        WorkflowHandle<Integer> handle2 = dbosExecutor.retrieveWorkflow(id2);
        assertEquals(id2, handle2.getWorkflowId());
        Integer result2 = handle2.getResult();
        assertEquals(50,result2) ;
        assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().getStatus());
    }

    @Test
    public void testLimiter() throws Exception {

        int limit = 5;
        double period = 1.8; //

        Queue limitQ = new DBOS.QueueBuilder("limitQueue")
                .limit(limit, period)
                .concurrency(1)
                .workerConcurrency(1)
                .build();

        ServiceQ serviceQ = new DBOS.WorkflowBuilder<ServiceQ>()
                .interfaceClass(ServiceQ.class)
                .implementation(new ServiceQImpl())
                .queue(limitQ)
                .build() ;

        int numWaves = 3;
        int numTasks = numWaves * limit ;
        List<WorkflowHandle<Double>> handles = new ArrayList<>() ;
        List<Double> times = new ArrayList<>();

        for (int i = 0 ; i < numTasks ; i++) {
            String id = "id"+i ;
            try (SetWorkflowID ctx = new SetWorkflowID(id)) {
                serviceQ.limitWorkflow("abc","123");
            }
            handles.add(dbosExecutor.retrieveWorkflow(id));
        }

        for (WorkflowHandle<Double> h : handles) {
            double result = h.getResult() ;
            logger.info(String.valueOf(result));
            times.add(result);
        }

        double waveTolerance = 0.5;
        for (int wave = 0; wave < numWaves; wave++) {
            for (int i = wave * limit; i < (wave + 1) * limit - 1; i++) {
                double diff = times.get(i + 1) - times.get(i);
                logger.info(String.format("Wave %d, Task %d-%d: Time diff %.3f", wave, i, i+1, diff));
                assertTrue(diff < waveTolerance,
                        String.format("Wave %d: Tasks %d and %d should start close together. Diff: %.3f", wave, i, i + 1, diff));
            }
        }
        logger.info("Verified intra-wave timing.");

        double periodTolerance = 0.5;
        for (int wave = 0; wave < numWaves - 1; wave++) {
            double startOfNextWave = times.get(limit * (wave + 1));
            double startOfCurrentWave = times.get(limit * wave);
            double gap = startOfNextWave - startOfCurrentWave;
            logger.info(String.format("Gap between Wave %d and %d: %.3f", wave, wave+1, gap));
            assertTrue(gap > period - periodTolerance,
                    String.format("Gap between wave %d and %d should be at least %.3f. Actual: %.3f", wave, wave + 1, period - periodTolerance, gap));
            assertTrue(gap < period + periodTolerance,
                    String.format("Gap between wave %d and %d should be at most %.3f. Actual: %.3f", wave, wave + 1, period + periodTolerance, gap));
        }

        for (WorkflowHandle<Double> h : handles) {
            assertEquals(WorkflowState.SUCCESS.name(), h.getStatus().getStatus());
        }

    }

    @Test
    public void testWorkerConcurrency() throws Exception {

        queueService.stop();
        while(!queueService.isStopped()) {
            Thread.sleep(2000);
            logger.info("Waiting for queueService to stop") ;
        }

        Queue qwithWCLimit = new DBOS.QueueBuilder("QwithWCLimit")
                .concurrency(1)
                .workerConcurrency(2)
                .concurrency(3)
                .build();


        WorkflowStatusInternal wfStatusInternal = new WorkflowStatusInternal(
                "xxx",
                WorkflowState.SUCCESS,
                "OrderProcessingWorkflow",
                "com.example.workflows.OrderWorkflow",
                "prod-config",
                "user123@example.com",
                "admin",
                "admin,operator",
                "{\"result\":\"success\"}",
                null,
                System.currentTimeMillis() - 3600000,
                System.currentTimeMillis(),
                "QwithWCLimit",
                Constants.DEFAULT_EXECUTORID,
                Constants.DEFAULT_APP_VERSION,
                "order-app-123",
                0,
                300000,
                System.currentTimeMillis() + 2400000,
                "dedup-112233",
                1,
                "{\"orderId\":\"ORD-12345\"}"
        );



        for (int i = 0 ;  i < 4 ; i++) {

            try (Connection conn = dataSource.getConnection()) {

                String wfid = "id" + i;
                wfStatusInternal.setWorkflowUUID(wfid);
                wfStatusInternal.setStatus(WorkflowState.ENQUEUED);
                wfStatusInternal.setDeduplicationId("dedup"+i);
                InsertWorkflowResult result = systemDatabase.insertWorkflowStatus(conn, wfStatusInternal);

            }
        }

        List<String> idsToRun = systemDatabase.getAndStartQueuedWorkflows(qwithWCLimit, Constants.DEFAULT_EXECUTORID, Constants.DEFAULT_APP_VERSION) ;
        assertEquals(2, idsToRun.size()) ;

        // run the same above 2 are in Pending.
        // So no de queueing
        idsToRun = systemDatabase.getAndStartQueuedWorkflows(qwithWCLimit, Constants.DEFAULT_EXECUTORID, Constants.DEFAULT_APP_VERSION) ;
        assertEquals(0, idsToRun.size()) ;

        // mark the first 2 as success
        DBUtils.updateWorkflowState(dataSource, WorkflowState.PENDING.name(), WorkflowState.SUCCESS.name());

        // next 2 get dequeued
        idsToRun = systemDatabase.getAndStartQueuedWorkflows(qwithWCLimit, Constants.DEFAULT_EXECUTORID, Constants.DEFAULT_APP_VERSION) ;
        assertEquals(2, idsToRun.size()) ;

        DBUtils.updateWorkflowState(dataSource, WorkflowState.PENDING.name(), WorkflowState.SUCCESS.name());
        idsToRun = systemDatabase.getAndStartQueuedWorkflows(qwithWCLimit, Constants.DEFAULT_EXECUTORID, Constants.DEFAULT_APP_VERSION) ;
        assertEquals(0, idsToRun.size()) ;

    }

    @Test
    public void testGlobalConcurrency() throws Exception {

        queueService.stop();
        while (!queueService.isStopped()) {
            Thread.sleep(2000);
            logger.info("Waiting for queueService to stop");
        }


        Queue qwithWCLimit = new DBOS.QueueBuilder("QwithWCLimit")
                .concurrency(1)
                .workerConcurrency(2)
                .concurrency(3)
                .build();


        WorkflowStatusInternal wfStatusInternal = new WorkflowStatusInternal(
                "xxx",
                WorkflowState.SUCCESS,
                "OrderProcessingWorkflow",
                "com.example.workflows.OrderWorkflow",
                "prod-config",
                "user123@example.com",
                "admin",
                "admin,operator",
                "{\"result\":\"success\"}",
                null,
                System.currentTimeMillis() - 3600000,
                System.currentTimeMillis(),
                "QwithWCLimit",
                Constants.DEFAULT_EXECUTORID,
                Constants.DEFAULT_APP_VERSION,
                "order-app-123",
                0,
                300000,
                System.currentTimeMillis() + 2400000,
                "dedup-112233",
                1,
                "{\"orderId\":\"ORD-12345\"}"
        );


        // executor1
        for (int i = 0; i < 2; i++) {

            try (Connection conn = dataSource.getConnection()) {

                String wfid = "id" + i;
                wfStatusInternal.setWorkflowUUID(wfid);
                wfStatusInternal.setStatus(WorkflowState.ENQUEUED);
                wfStatusInternal.setDeduplicationId("dedup" + i);
                InsertWorkflowResult result = systemDatabase.insertWorkflowStatus(conn, wfStatusInternal);
            }
        }

        // executor2

        String executor2 = "remote";
        for (int i = 2; i < 5; i++) {

            try (Connection conn = dataSource.getConnection()) {

                String wfid = "id" + i;
                wfStatusInternal.setWorkflowUUID(wfid);
                wfStatusInternal.setStatus(WorkflowState.PENDING);
                wfStatusInternal.setDeduplicationId("dedup" + i);
                wfStatusInternal.setExecutorId(executor2);
                InsertWorkflowResult result = systemDatabase.insertWorkflowStatus(conn, wfStatusInternal);
            }
        }


        List<String> idsToRun = systemDatabase.getAndStartQueuedWorkflows(qwithWCLimit, Constants.DEFAULT_EXECUTORID, Constants.DEFAULT_APP_VERSION) ;
        // 0 because global concurrency limit is reached
        assertEquals(0, idsToRun.size()) ;

        DBUtils.updateWorkflowState(dataSource, WorkflowState.PENDING.name(), WorkflowState.SUCCESS.name());
        idsToRun = systemDatabase.getAndStartQueuedWorkflows(qwithWCLimit, Constants.DEFAULT_EXECUTORID, Constants.DEFAULT_APP_VERSION) ;
        assertEquals(2, idsToRun.size()) ;

    }
}

