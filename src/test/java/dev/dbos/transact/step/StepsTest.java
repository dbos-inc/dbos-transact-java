package dev.dbos.transact.step;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.*;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.*;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class StepsTest {

  private static DBOSConfig dbosConfig;
  private DBOS dbos;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    StepsTest.dbosConfig =
        new DBOSConfig.Builder()
            .appName("systemdbtest")
            .databaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .dbUser("postgres")
            .maximumPoolSize(2)
            .build();
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);

    dbos = DBOS.initialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws SQLException, Exception {
    dbos.shutdown();
  }

  @Test
  public void workflowWithStepsSync() throws SQLException {
    ServiceB serviceB =
        dbos.<ServiceB>Workflow()
            .interfaceClass(ServiceB.class)
            .implementation(new ServiceBImpl())
            .build();

    ServiceA serviceA =
        dbos.<ServiceA>Workflow()
            .interfaceClass(ServiceA.class)
            .implementation(new ServiceAImpl(serviceB))
            .build();

    dbos.launch();

    String wid = "sync123";

    try (var id = new WorkflowOptions(wid).setContext()) {
      String result = serviceA.workflowWithSteps("hello");
      assertEquals("hellohello", result);
    }

    List<StepInfo> stepInfos = dbos.listWorkflowSteps(wid);
    assertEquals(5, stepInfos.size());

    assertEquals("step1", stepInfos.get(0).functionName());
    assertEquals(0, stepInfos.get(0).functionId());
    assertEquals("one", stepInfos.get(0).output());
    assertNull(stepInfos.get(0).error());
    assertEquals("step2", stepInfos.get(1).functionName());
    assertEquals(1, stepInfos.get(1).functionId());
    assertEquals("two", stepInfos.get(1).output());
    assertEquals("step3", stepInfos.get(2).functionName());
    assertEquals(2, stepInfos.get(2).functionId());
    assertEquals("three", stepInfos.get(2).output());
    assertEquals("step4", stepInfos.get(3).functionName());
    assertEquals(3, stepInfos.get(3).functionId());
    assertEquals("four", stepInfos.get(3).output());
    assertEquals("step5", stepInfos.get(4).functionName());
    assertEquals(4, stepInfos.get(4).functionId());
    assertEquals("five", stepInfos.get(4).output());
  }

  @Test
  public void workflowWithStepsSyncError() throws SQLException {
    ServiceB serviceB =
        dbos.<ServiceB>Workflow()
            .interfaceClass(ServiceB.class)
            .implementation(new ServiceBImpl())
            .build();

    ServiceA serviceA =
        dbos.<ServiceA>Workflow()
            .interfaceClass(ServiceA.class)
            .implementation(new ServiceAImpl(serviceB))
            .build();

    dbos.launch();

    String wid = "sync123er";
    try (var id = new WorkflowOptions(wid).setContext()) {
      String result = serviceA.workflowWithStepError("hello");
      assertEquals("hellohello", result);
    }

    List<StepInfo> stepInfos = dbos.listWorkflowSteps(wid);
    assertEquals(5, stepInfos.size());
    assertEquals("step3", stepInfos.get(2).functionName());
    assertEquals(2, stepInfos.get(2).functionId());
    var error = stepInfos.get(2).error().throwable();
    assertInstanceOf(Exception.class, error, "The error should be an Exception");
    assertEquals("step3 error", error.getMessage(), "Error message should match");
    assertEquals("step3 error", stepInfos.get(2).error().message());
    assertNull(stepInfos.get(2).output());
  }

  @Test
  public void workflowWithInlineSteps() throws SQLException {
    ServiceWFAndStep service =
        dbos.<ServiceWFAndStep>Workflow()
            .interfaceClass(ServiceWFAndStep.class)
            .implementation(new ServiceWFAndStepImpl())
            .build();

    dbos.launch();

    String wid = "wfWISwww123";
    try (var id = new WorkflowOptions(wid).setContext()) {
      service.aWorkflowWithInlineSteps("input");
    }

    WorkflowHandle<String, RuntimeException> handle = dbos.retrieveWorkflow(wid);
    assertEquals("input5", (String) handle.getResult());

    List<StepInfo> stepInfos = dbos.listWorkflowSteps(wid);
    assertEquals(1, stepInfos.size());

    assertEquals("stringLength", stepInfos.get(0).functionName());
    assertEquals(0, stepInfos.get(0).functionId());
    assertEquals(5, stepInfos.get(0).output());
    assertNull(stepInfos.get(0).error());
  }

  @Test
  public void asyncworkflowWithSteps() throws Exception {
    ServiceB serviceB =
        dbos.<ServiceB>Workflow()
            .interfaceClass(ServiceB.class)
            .implementation(new ServiceBImpl())
            .build();

    ServiceA serviceA =
        dbos.<ServiceA>Workflow()
            .interfaceClass(ServiceA.class)
            .implementation(new ServiceAImpl(serviceB))
            .build();

    dbos.launch();

    String workflowId = "wf-1234";

    try (var id = new WorkflowOptions(workflowId).setContext()) {
      serviceA.workflowWithSteps("hello");
    }

    var handle = dbos.retrieveWorkflow(workflowId);
    assertEquals("hellohello", (String) handle.getResult());

    List<StepInfo> stepInfos = dbos.listWorkflowSteps(workflowId);
    assertEquals(5, stepInfos.size());

    assertEquals("step1", stepInfos.get(0).functionName());
    assertEquals(0, stepInfos.get(0).functionId());
    assertEquals("one", stepInfos.get(0).output());
    assertEquals("step2", stepInfos.get(1).functionName());
    assertEquals(1, stepInfos.get(1).functionId());
    assertEquals("two", stepInfos.get(1).output());
    assertEquals("step3", stepInfos.get(2).functionName());
    assertEquals(2, stepInfos.get(2).functionId());
    assertEquals("three", stepInfos.get(2).output());
    assertEquals("step4", stepInfos.get(3).functionName());
    assertEquals(3, stepInfos.get(3).functionId());
    assertEquals("four", stepInfos.get(3).output());
    assertEquals("step5", stepInfos.get(4).functionName());
    assertEquals(4, stepInfos.get(4).functionId());
    assertEquals("five", stepInfos.get(4).output());
    assertNull(stepInfos.get(4).error());
  }

  @Test
  public void sameInterfaceWorkflowWithSteps() throws Exception {
    ServiceWFAndStep service =
        dbos.<ServiceWFAndStep>Workflow()
            .interfaceClass(ServiceWFAndStep.class)
            .implementation(new ServiceWFAndStepImpl())
            .build();

    dbos.launch();

    service.setSelf(service);

    String workflowId = "wf-same-1234";

    try (var id = new WorkflowOptions(workflowId).setContext()) {
      service.aWorkflow("hello");
    }

    var handle = dbos.retrieveWorkflow(workflowId);
    assertEquals("helloonetwo", (String) handle.getResult());

    List<StepInfo> stepInfos = dbos.listWorkflowSteps(workflowId);
    assertEquals(2, stepInfos.size());

    assertEquals("step1", stepInfos.get(0).functionName());
    assertEquals(0, stepInfos.get(0).functionId());
    assertEquals("one", stepInfos.get(0).output());
    assertEquals("step2", stepInfos.get(1).functionName());
    assertEquals(1, stepInfos.get(1).functionId());
    assertEquals("two", stepInfos.get(1).output());
    assertNull(stepInfos.get(1).error());
  }

  @Test
  public void stepOutsideWorkflow() throws Exception {

    ServiceB serviceB =
        dbos.<ServiceB>Workflow()
            .interfaceClass(ServiceB.class)
            .implementation(new ServiceBImpl())
            .build();

    dbos.launch();

    String result = serviceB.step2("abcde");
    assertEquals("abcde", result);

    serviceB = new ServiceBImpl();
    result = serviceB.step2("hello");
    assertEquals("hello", result);

    dbos.shutdown();

    result = serviceB.step2("pqrstu");
    assertEquals("pqrstu", result);
  }

  @Test
  public void stepRetryLogic() throws Exception {
    ServiceWFAndStep service =
        dbos.<ServiceWFAndStep>Workflow()
            .interfaceClass(ServiceWFAndStep.class)
            .implementation(new ServiceWFAndStepImpl())
            .build();

    dbos.launch();

    service.setSelf(service);

    String workflowId = "wf-stepretrytest-1234";

    try (var id = new WorkflowOptions(workflowId).setContext()) {
      service.stepRetryWorkflow("hello");
    }

    var handle = dbos.retrieveWorkflow(workflowId);
    String expectedRes = "2 Retries: 2.  No retry: 1.  Backoff timeout: 2.";
    if (expectedRes != handle.getResult()) {
      System.out.println(handle.getResult());
    }
    assertEquals(expectedRes, (String) handle.getResult());

    List<StepInfo> stepInfos = dbos.listWorkflowSteps(workflowId);
    assertEquals(3, stepInfos.size());

    assertEquals("stepWith2Retries", stepInfos.get(0).functionName());
    assertEquals(0, stepInfos.get(0).functionId());
    assertNotNull(stepInfos.get(0).error());
    assertEquals("stepWithNoRetriesAllowed", stepInfos.get(1).functionName());
    assertEquals(1, stepInfos.get(1).functionId());
    assertNotNull(stepInfos.get(1).error());
    assertEquals("stepWithLongRetry", stepInfos.get(2).functionName());
    assertEquals(2, stepInfos.get(2).functionId());
    assertNull(stepInfos.get(2).error());
  }
}
