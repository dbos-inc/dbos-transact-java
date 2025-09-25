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
import org.junit.jupiter.api.Timeout;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class StepsTest {

  private static DBOSConfig dbosConfig;
  private DBOS dbos;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    StepsTest.dbosConfig =
        new DBOSConfig.Builder()
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

    assertEquals("step1", stepInfos.get(0).getFunctionName());
    assertEquals(0, stepInfos.get(0).getFunctionId());
    assertEquals("one", stepInfos.get(0).getOutput());
    assertNull(stepInfos.get(0).getError());
    assertEquals("step2", stepInfos.get(1).getFunctionName());
    assertEquals(1, stepInfos.get(1).getFunctionId());
    assertEquals("two", stepInfos.get(1).getOutput());
    assertEquals("step3", stepInfos.get(2).getFunctionName());
    assertEquals(2, stepInfos.get(2).getFunctionId());
    assertEquals("three", stepInfos.get(2).getOutput());
    assertEquals("step4", stepInfos.get(3).getFunctionName());
    assertEquals(3, stepInfos.get(3).getFunctionId());
    assertEquals("four", stepInfos.get(3).getOutput());
    assertEquals("step5", stepInfos.get(4).getFunctionName());
    assertEquals(4, stepInfos.get(4).getFunctionId());
    assertEquals("five", stepInfos.get(4).getOutput());
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
    assertEquals("step3", stepInfos.get(2).getFunctionName());
    assertEquals(2, stepInfos.get(2).getFunctionId());
    var error = stepInfos.get(2).getError().throwable();
    assertInstanceOf(Exception.class, error, "The error should be an Exception");
    assertEquals("step3 error", error.getMessage(), "Error message should match");
    assertEquals("step3 error", stepInfos.get(2).getError().message());
    assertNull(stepInfos.get(2).getOutput());
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

    assertEquals("stringLength", stepInfos.get(0).getFunctionName());
    assertEquals(0, stepInfos.get(0).getFunctionId());
    assertEquals(5, stepInfos.get(0).getOutput());
    assertNull(stepInfos.get(0).getError());
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

    assertEquals("step1", stepInfos.get(0).getFunctionName());
    assertEquals(0, stepInfos.get(0).getFunctionId());
    assertEquals("one", stepInfos.get(0).getOutput());
    assertEquals("step2", stepInfos.get(1).getFunctionName());
    assertEquals(1, stepInfos.get(1).getFunctionId());
    assertEquals("two", stepInfos.get(1).getOutput());
    assertEquals("step3", stepInfos.get(2).getFunctionName());
    assertEquals(2, stepInfos.get(2).getFunctionId());
    assertEquals("three", stepInfos.get(2).getOutput());
    assertEquals("step4", stepInfos.get(3).getFunctionName());
    assertEquals(3, stepInfos.get(3).getFunctionId());
    assertEquals("four", stepInfos.get(3).getOutput());
    assertEquals("step5", stepInfos.get(4).getFunctionName());
    assertEquals(4, stepInfos.get(4).getFunctionId());
    assertEquals("five", stepInfos.get(4).getOutput());
    assertNull(stepInfos.get(4).getError());
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

    assertEquals("step1", stepInfos.get(0).getFunctionName());
    assertEquals(0, stepInfos.get(0).getFunctionId());
    assertEquals("one", stepInfos.get(0).getOutput());
    assertEquals("step2", stepInfos.get(1).getFunctionName());
    assertEquals(1, stepInfos.get(1).getFunctionId());
    assertEquals("two", stepInfos.get(1).getOutput());
    assertNull(stepInfos.get(1).getError());
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

    assertEquals("stepWith2Retries", stepInfos.get(0).getFunctionName());
    assertEquals(0, stepInfos.get(0).getFunctionId());
    assertNotNull(stepInfos.get(0).getError());
    assertEquals("stepWithNoRetriesAllowed", stepInfos.get(1).getFunctionName());
    assertEquals(1, stepInfos.get(1).getFunctionId());
    assertNotNull(stepInfos.get(1).getError());
    assertEquals("stepWithLongRetry", stepInfos.get(2).getFunctionName());
    assertEquals(2, stepInfos.get(2).getFunctionId());
    assertNull(stepInfos.get(2).getError());
  }
}
