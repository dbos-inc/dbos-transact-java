package dev.dbos.transact.queue;

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
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
        // queueService.start();


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

        queueService.start();

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

        while(!queueService.isStopped()) {
            Thread.sleep(2000);
            logger.info("Waiting for queueService to step") ;
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

        /* WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow(id);
        assertEquals(id, handle.getWorkflowId());
        String result = handle.getResult();
        assertEquals("inputqinputq",result) ; */

    }

}

