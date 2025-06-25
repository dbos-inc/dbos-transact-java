package dev.dbos.transact.step;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.*;
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

public class StepsTest {

    private static DBOSConfig dbosConfig;
    private static DBOS dbos ;
    private static SystemDatabase systemDatabase ;
    private static DBOSExecutor dbosExecutor;

    @BeforeAll
    static void onetimeSetup() throws Exception {

        StepsTest.dbosConfig = new DBOSConfig
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
    public void workflowWithStepsSync() throws SQLException  {

        ServiceB serviceB = dbos.<ServiceB>Workflow()
                .interfaceClass(ServiceB.class)
                .implementation(new ServiceBImpl())
                .build();


        ServiceA serviceA = dbos.<ServiceA>Workflow()
                .interfaceClass(ServiceA.class)
                .implementation(new ServiceAImpl(serviceB))
                .build();

        String result = serviceA.workflowWithSteps("hello");
        assertEquals("hellohello", result);


    }

    @Test
    public void workflowWithStepsSyncError() throws SQLException  {

        ServiceB serviceB = dbos.<ServiceB>Workflow()
                .interfaceClass(ServiceB.class)
                .implementation(new ServiceBImpl())
                .build();

        ServiceA serviceA = dbos.<ServiceA>Workflow()
                .interfaceClass(ServiceA.class)
                .implementation(new ServiceAImpl(serviceB))
                .build();

        String result = serviceA.workflowWithStepError("hello");
        assertEquals("hellohello", result);

    }

    @Test
    public void AsyncworkflowWithSteps() throws Exception  {

        ServiceB serviceB = dbos.<ServiceB>Workflow()
                .interfaceClass(ServiceB.class)
                .implementation(new ServiceBImpl())
                .build();


        ServiceA serviceA = dbos.<ServiceA>Workflow()
                .interfaceClass(ServiceA.class)
                .implementation(new ServiceAImpl(serviceB))
                .async()
                .build();

        String workflowId = "wf-1234";

        try (SetWorkflowID id = new SetWorkflowID(workflowId)) {
            serviceA.workflowWithSteps("hello");
        }

        WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow(workflowId);
        assertEquals("hellohello", handle.getResult());


    }

}
