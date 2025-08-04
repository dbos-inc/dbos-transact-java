package dev.dbos.transact.step;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.*;
import org.junit.jupiter.api.*;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class StepsTest {

    private static DBOSConfig dbosConfig;
    private static DataSource dataSource;
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
    }

    @BeforeEach
    void beforeEachTest() throws SQLException {
        DBUtils.recreateDB(dbosConfig);
        dataSource = SystemDatabase.createDataSource(dbosConfig);
        systemDatabase = new SystemDatabase(dataSource);
        dbosExecutor = new DBOSExecutor(dbosConfig, systemDatabase);
        dbos = DBOS.initialize(dbosConfig, systemDatabase, dbosExecutor, null, null);
        dbos.launch();
    }

    @AfterEach
    void afterEachTest() throws SQLException {
        dbos.shutdown();
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

        String wid = "sync123";

        try (SetWorkflowID id = new SetWorkflowID(wid)) {
            String result = serviceA.workflowWithSteps("hello");
            assertEquals("hellohello", result);
        }

        List<StepInfo> stepInfos = systemDatabase.listWorkflowSteps(wid);
        assertEquals(5, stepInfos.size());

        assertEquals("step1",stepInfos.get(0).getFunctionName());
        assertEquals(0,stepInfos.get(0).getFunctionId());
        assertEquals("one",stepInfos.get(0).getOutput());
        assertNull(stepInfos.get(0).getError());
        assertEquals("step2",stepInfos.get(1).getFunctionName());
        assertEquals(1,stepInfos.get(1).getFunctionId());
        assertEquals("two",stepInfos.get(1).getOutput());
        assertEquals("step3",stepInfos.get(2).getFunctionName());
        assertEquals(2,stepInfos.get(2).getFunctionId());
        assertEquals("three",stepInfos.get(2).getOutput());
        assertEquals("step4",stepInfos.get(3).getFunctionName());
        assertEquals(3,stepInfos.get(3).getFunctionId());
        assertEquals("four",stepInfos.get(3).getOutput());
        assertEquals("step5",stepInfos.get(4).getFunctionName());
        assertEquals(4,stepInfos.get(4).getFunctionId());
        assertEquals("five",stepInfos.get(4).getOutput());

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

        String wid = "sync123er";
        try (SetWorkflowID id = new SetWorkflowID(wid)) {
            String result = serviceA.workflowWithStepError("hello");
            assertEquals("hellohello", result);
        }

        List<StepInfo> stepInfos = systemDatabase.listWorkflowSteps(wid);
        assertEquals(5, stepInfos.size());
        assertEquals("step3",stepInfos.get(2).getFunctionName());
        assertEquals(2,stepInfos.get(2).getFunctionId());
        Throwable error = stepInfos.get(2).getError();
        assertInstanceOf(Exception.class, error, "The error should be an Exception");
        assertEquals("step3 error", error.getMessage(), "Error message should match");
        assertNull(stepInfos.get(2).getOutput()) ;
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

        WorkflowHandle<?> handle = dbosExecutor.retrieveWorkflow(workflowId);
        assertEquals("hellohello", (String)handle.getResult());

        List<StepInfo> stepInfos = systemDatabase.listWorkflowSteps(workflowId);
        assertEquals(5, stepInfos.size());

        assertEquals("step1",stepInfos.get(0).getFunctionName());
        assertEquals(0,stepInfos.get(0).getFunctionId());
        assertEquals("one",stepInfos.get(0).getOutput());
        assertEquals("step2",stepInfos.get(1).getFunctionName());
        assertEquals(1,stepInfos.get(1).getFunctionId());
        assertEquals("two",stepInfos.get(1).getOutput());
        assertEquals("step3",stepInfos.get(2).getFunctionName());
        assertEquals(2,stepInfos.get(2).getFunctionId());
        assertEquals("three",stepInfos.get(2).getOutput());
        assertEquals("step4",stepInfos.get(3).getFunctionName());
        assertEquals(3,stepInfos.get(3).getFunctionId());
        assertEquals("four",stepInfos.get(3).getOutput());
        assertEquals("step5",stepInfos.get(4).getFunctionName());
        assertEquals(4,stepInfos.get(4).getFunctionId());
        assertEquals("five",stepInfos.get(4).getOutput());
        assertNull(stepInfos.get(4).getError());

    }

    @Test
    public void SameInterfaceWorkflowWithSteps() throws Exception  {

        ServiceWFAndStep service = dbos.<ServiceWFAndStep>Workflow()
                .interfaceClass(ServiceWFAndStep.class)
                .implementation(new ServiceWFAndStepImpl())
                .async()
                .build();

        service.setSelf(service);

        String workflowId = "wf-same-1234";

        try (SetWorkflowID id = new SetWorkflowID(workflowId)) {
            service.aWorkflow("hello");
        }

        WorkflowHandle<?> handle = dbosExecutor.retrieveWorkflow(workflowId);
        assertEquals("helloonetwo", (String)handle.getResult());

        List<StepInfo> stepInfos = systemDatabase.listWorkflowSteps(workflowId);
        assertEquals(2, stepInfos.size());

        assertEquals("step1",stepInfos.get(0).getFunctionName());
        assertEquals(0,stepInfos.get(0).getFunctionId());
        assertEquals("one",stepInfos.get(0).getOutput());
        assertEquals("step2",stepInfos.get(1).getFunctionName());
        assertEquals(1,stepInfos.get(1).getFunctionId());
        assertEquals("two",stepInfos.get(1).getOutput());
        assertNull(stepInfos.get(1).getError());

    }

}
