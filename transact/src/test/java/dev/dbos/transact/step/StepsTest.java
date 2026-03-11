package dev.dbos.transact.step;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.*;

import java.util.List;

import org.junit.jupiter.api.*;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT)
public class StepsTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS.Instance dbos;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS.Instance(dbosConfig);
  }

  @Test
  public void workflowWithStepsSync() throws Exception {
    ServiceB serviceB = dbos.registerWorkflows(ServiceB.class, new ServiceBImpl());
    ServiceA serviceA = dbos.registerWorkflows(ServiceA.class, new ServiceAImpl(serviceB));
    dbos.launch();

    String wid = "sync123";

    var before = System.currentTimeMillis();
    try (var id = new WorkflowOptions(wid).setContext()) {
      String result = serviceA.workflowWithSteps("hello");
      assertEquals("hellohello", result);
    }

    List<StepInfo> stepInfos = dbos.listWorkflowSteps(wid);
    assertEquals(5, stepInfos.size());

    String[] names = {"step1", "step2", "step3", "step4", "step5"};
    String[] output = {"one", "two", "three", "four", "five"};

    for (var i = 0; i < stepInfos.size(); i++) {
      var step = stepInfos.get(i);
      assertEquals(names[i], step.functionName());
      assertEquals(output[i], step.output());
      assertNull(step.error());
      assertNotNull(step.startedAtEpochMs());
      assertTrue(before <= step.startedAtEpochMs());
      assertNotNull(step.completedAtEpochMs());
      assertTrue(step.startedAtEpochMs() <= step.completedAtEpochMs());
      before = step.completedAtEpochMs();
    }
  }

  @Test
  public void workflowWithStepsSyncError() throws Exception {
    ServiceB serviceB = dbos.registerWorkflows(ServiceB.class, new ServiceBImpl());
    ServiceA serviceA = dbos.registerWorkflows(ServiceA.class, new ServiceAImpl(serviceB));
    dbos.launch();

    var before = System.currentTimeMillis();
    String wid = "sync123er";
    try (var id = new WorkflowOptions(wid).setContext()) {
      String result = serviceA.workflowWithStepError("hello");
      assertEquals("hellohello", result);
    }

    List<StepInfo> stepInfos = dbos.listWorkflowSteps(wid);
    assertEquals(5, stepInfos.size());

    for (var i = 0; i < stepInfos.size(); i++) {
      var step = stepInfos.get(i);
      assertNotNull(step.startedAtEpochMs());
      assertTrue(before <= step.startedAtEpochMs());
      assertNotNull(step.completedAtEpochMs());
      assertTrue(step.startedAtEpochMs() <= step.completedAtEpochMs());
      before = step.completedAtEpochMs();
    }

    var step3 = stepInfos.get(2);
    assertEquals("step3", step3.functionName());
    assertEquals(2, step3.functionId());
    var error = step3.error().throwable();
    assertInstanceOf(Exception.class, error, "The error should be an Exception");
    assertEquals("step3 error", error.getMessage(), "Error message should match");
    assertEquals("step3 error", step3.error().message());
    assertNull(step3.output());
  }

  @Test
  public void workflowWithInlineSteps() throws Exception {
    ServiceWFAndStep service =
        dbos.registerWorkflows(ServiceWFAndStep.class, new ServiceWFAndStepImpl(dbos));
    dbos.launch();

    var before = System.currentTimeMillis();
    String wid = "wfWISwww123";
    try (var id = new WorkflowOptions(wid).setContext()) {
      service.aWorkflowWithInlineSteps("input");
    }

    WorkflowHandle<String, RuntimeException> handle = dbos.retrieveWorkflow(wid);
    assertEquals("input5", (String) handle.getResult());

    List<StepInfo> stepInfos = dbos.listWorkflowSteps(wid);
    assertEquals(1, stepInfos.size());

    for (var i = 0; i < stepInfos.size(); i++) {
      var step = stepInfos.get(i);
      assertNotNull(step.startedAtEpochMs());
      assertTrue(before <= step.startedAtEpochMs());
      assertNotNull(step.completedAtEpochMs());
      assertTrue(step.startedAtEpochMs() <= step.completedAtEpochMs());
      before = step.completedAtEpochMs();
    }

    assertEquals("stringLength", stepInfos.get(0).functionName());
    assertEquals(0, stepInfos.get(0).functionId());
    assertEquals(5, stepInfos.get(0).output());
    assertNull(stepInfos.get(0).error());
  }

  @Test
  public void asyncworkflowWithSteps() throws Exception {
    ServiceB serviceB = dbos.registerWorkflows(ServiceB.class, new ServiceBImpl());
    ServiceA serviceA = dbos.registerWorkflows(ServiceA.class, new ServiceAImpl(serviceB));
    dbos.launch();

    var before = System.currentTimeMillis();
    String workflowId = "wf-1234";
    try (var id = new WorkflowOptions(workflowId).setContext()) {
      serviceA.workflowWithSteps("hello");
    }

    var handle = dbos.retrieveWorkflow(workflowId);
    assertEquals("hellohello", (String) handle.getResult());

    List<StepInfo> stepInfos = dbos.listWorkflowSteps(workflowId);
    assertEquals(5, stepInfos.size());

    String[] names = {"step1", "step2", "step3", "step4", "step5"};
    String[] output = {"one", "two", "three", "four", "five"};

    for (var i = 0; i < stepInfos.size(); i++) {
      var step = stepInfos.get(i);
      assertEquals(names[i], step.functionName());
      assertEquals(output[i], step.output());
      assertNull(step.error());
      assertNotNull(step.startedAtEpochMs());
      assertTrue(before <= step.startedAtEpochMs());
      assertNotNull(step.completedAtEpochMs());
      assertTrue(step.startedAtEpochMs() <= step.completedAtEpochMs());
      before = step.completedAtEpochMs();
    }
  }

  @Test
  public void sameInterfaceWorkflowWithSteps() throws Exception {
    ServiceWFAndStepImpl impl = new ServiceWFAndStepImpl(dbos);
    ServiceWFAndStep service = dbos.registerWorkflows(ServiceWFAndStep.class, impl);
    dbos.launch();

    impl.setSelf(service);

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
    ServiceB serviceB = dbos.registerWorkflows(ServiceB.class, new ServiceBImpl());
    dbos.launch();

    String result = serviceB.step2("abcde");
    assertEquals("abcde", result);

    result = new ServiceBImpl().step2("hello");
    assertEquals("hello", result);

    result = new ServiceBImpl().step2("pqrstu");
    assertEquals("pqrstu", result);
  }

  @Test
  public void stepRetryLogic() throws Exception {
    ServiceWFAndStepImpl impl = new ServiceWFAndStepImpl(dbos);
    ServiceWFAndStep service = dbos.registerWorkflows(ServiceWFAndStep.class, impl);
    dbos.launch();

    impl.setSelf(service);

    var before = System.currentTimeMillis();
    String workflowId = "wf-stepretrytest-1234";
    try (var id = new WorkflowOptions(workflowId).setContext()) {
      service.stepRetryWorkflow("hello");
    }

    var handle = dbos.retrieveWorkflow(workflowId);
    String expectedRes = "2 Retries: 2.  No retry: 1.  Backoff timeout: 2.";
    assertEquals(expectedRes, (String) handle.getResult());

    List<StepInfo> stepInfos = dbos.listWorkflowSteps(workflowId);
    assertEquals(3, stepInfos.size());

    for (var i = 0; i < stepInfos.size(); i++) {
      var step = stepInfos.get(i);
      assertNotNull(step.startedAtEpochMs());
      assertTrue(before <= step.startedAtEpochMs());
      assertNotNull(step.completedAtEpochMs());
      assertTrue(step.startedAtEpochMs() <= step.completedAtEpochMs());
      before = step.completedAtEpochMs();
    }

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

  @Test
  public void inlineStepRetryLogic() throws Exception {
    ServiceWFAndStep service =
        dbos.registerWorkflows(ServiceWFAndStep.class, new ServiceWFAndStepImpl(dbos));
    dbos.launch();

    String workflowId = "wf-inlinestepretrytest-1234";
    try (var id = new WorkflowOptions(workflowId).setContext()) {
      service.inlineStepRetryWorkflow("input");
    }

    var handle = dbos.retrieveWorkflow(workflowId);
    String expectedRes = "2 Retries: 2.";
    assertEquals(expectedRes, (String) handle.getResult());
  }
}
