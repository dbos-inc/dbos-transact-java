package dev.dbos.transact.queue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.step.StepsTest;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.WorkflowHandle;
import org.junit.jupiter.api.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class QueuesTest {

    private static DBOSConfig dbosConfig;
    private static DataSource dataSource;
    private static DBOS dbos ;
    private static SystemDatabase systemDatabase ;
    private static DBOSExecutor dbosExecutor;

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

        DBUtils.clearTables(dataSource);
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dbos.shutdown();
    }

    @Test
    public void testQueuedWorkflow() {

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

    }

}
