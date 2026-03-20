package dev.dbos.transact.step;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.*;

import java.util.List;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface ServiceA {
  String workflowWithSteps(String input);

  String workflowWithStepError(String input);
}

class ServiceAImpl implements ServiceA {

  private static final Logger logger = LoggerFactory.getLogger(ServiceAImpl.class);

  private final ServiceB serviceBproxy;

  ServiceAImpl(ServiceB b) {
    this.serviceBproxy = b;
  }

  @Override
  @Workflow(name = "workflowWithSteps")
  public String workflowWithSteps(String input) {
    serviceBproxy.step1("one");
    serviceBproxy.step2("two");
    try {
      serviceBproxy.step3("three", false);
    } catch (Exception e) {
      logger.info(e.getMessage());
    }
    serviceBproxy.step4("four");
    serviceBproxy.step5("five");

    return input + input;
  }

  @Override
  @Workflow(name = "workflowWithStepsError")
  public String workflowWithStepError(String input) {
    serviceBproxy.step1("one");
    serviceBproxy.step2("two");
    try {
      serviceBproxy.step3("three", true);
    } catch (Exception e) {
      logger.info(e.getMessage());
    }
    serviceBproxy.step4("four");
    serviceBproxy.step5("five");

    return input + input;
  }
}

interface ServiceB {
  String step1(String input);

  String step2(String input);

  String step3(String input, boolean throwError) throws Exception;

  String step4(String input);

  String step5(String input);
}

class ServiceBImpl implements ServiceB {

  @Override
  @Step(name = "step1")
  public String step1(String input) {
    return input;
  }

  @Override
  @Step(name = "step2")
  public String step2(String input) {
    return input;
  }

  @Override
  @Step(name = "step3")
  public String step3(String input, boolean throwError) throws Exception {
    if (throwError) {
      throw new Exception("step3 error");
    }
    return input;
  }

  @Override
  @Step(name = "step4")
  public String step4(String input) {
    return input;
  }

  @Override
  @Step(name = "step5")
  public String step5(String input) {
    return input;
  }
}

interface ServiceWFAndStep {
  String aWorkflow(String input);

  String stepOne(String input);

  String stepTwo(String input);

  String aWorkflowWithInlineSteps(String input);

  String stepWith2Retries(String input) throws Exception;

  String stepWithNoRetriesAllowed(String input) throws Exception;

  String stepWithLongRetry(String input) throws Exception;

  String stepRetryWorkflow(String input);

  String inlineStepRetryWorkflow(String input);
}

class ServiceWFAndStepImpl implements ServiceWFAndStep {

  private final DBOS dbos;

  private ServiceWFAndStep self;

  public ServiceWFAndStepImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  public void setSelf(ServiceWFAndStep serviceWFAndStep) {
    self = serviceWFAndStep;
  }

  @Override
  @Workflow(name = "myworkflow")
  public String aWorkflow(String input) {
    String s1 = self.stepOne("one");
    String s2 = self.stepTwo("two");
    return input + s1 + s2;
  }

  @Override
  @Step(name = "step1")
  public String stepOne(String input) {
    return input;
  }

  @Override
  @Step(name = "step2")
  public String stepTwo(String input) {
    return input;
  }

  @Override
  @Workflow(name = "aWorkflowWithInlineSteps")
  public String aWorkflowWithInlineSteps(String input) {
    var len = dbos.runStep(() -> input.length(), new StepOptions("stringLength"));
    return (input + len);
  }

  int stepWithRetryRuns = 0;

  @Override
  @Step(
      name = "stepWith2Retries",
      retriesAllowed = true,
      maxAttempts = 2,
      intervalSeconds = .01,
      backOffRate = 1)
  public String stepWith2Retries(String input) throws Exception {
    ++this.stepWithRetryRuns;
    throw new Exception("Will not ever run");
  }

  int stepWithNoRetryRuns = 0;

  @Override
  @Step(name = "stepWithNoRetriesAllowed", retriesAllowed = false)
  public String stepWithNoRetriesAllowed(String input) throws Exception {
    ++stepWithNoRetryRuns;
    throw new Exception("No retries");
  }

  long startedTime = 0;
  int stepWithLongRetryRuns = 0;

  @Override
  @Step(
      name = "stepWithLongRetry",
      maxAttempts = 3,
      retriesAllowed = true,
      intervalSeconds = 1,
      backOffRate = 10)
  public String stepWithLongRetry(String input) throws Exception {
    ++stepWithLongRetryRuns;
    if (startedTime == 0) {
      startedTime = System.currentTimeMillis();
      throw new Exception("First try");
    }
    if (System.currentTimeMillis() - startedTime > 500) {
      var rv = Integer.valueOf(this.stepWithLongRetryRuns).toString();
      startedTime = 0;
      return rv;
    }
    throw new Exception("Not enough time passed yet");
  }

  @Override
  @Workflow(name = "retryTestWorkflow")
  public String stepRetryWorkflow(String input) {
    long ctime = System.currentTimeMillis();
    boolean caught = false;
    String result = "2 Retries: ";
    try {
      result = result + self.stepWith2Retries(input);
    } catch (Exception e) {
      caught = true;
    }
    if (!caught) {
      result += "<Step with retries should have thrown>";
    }
    if (System.currentTimeMillis() - ctime > 1000) {
      result += "<Retry took too long>";
    }
    result += this.stepWithRetryRuns;
    result += ".";

    result += "  No retry: ";
    caught = false;
    try {
      self.stepWithNoRetriesAllowed("");
    } catch (Exception e) {
      caught = true;
    }
    if (!caught) {
      result += "<Step with no retries should have thrown>";
    }
    result += this.stepWithNoRetryRuns;
    result += ".";

    ctime = System.currentTimeMillis();
    result += "  Backoff timeout: ";
    caught = false;
    try {
      self.stepWithLongRetry("");
    } catch (Exception e) {
      caught = true;
    }
    if (caught) {
      result += "<Step with long retry should have completed>";
    }
    if (System.currentTimeMillis() - ctime > 2000) {
      result += "<Step with long retry should have finished faster>";
    }
    if (System.currentTimeMillis() - ctime < 500) {
      result += "<Step with long retry should does not appear to have slept>";
    }
    result += this.stepWithLongRetryRuns;
    result += ".";

    return result;
  }

  @Override
  @Workflow(name = "inlineStepRetryTestWorkflow")
  public String inlineStepRetryWorkflow(String input) {
    long ctime = System.currentTimeMillis();
    boolean caught = false;
    String result = "2 Retries: ";
    try {
      result =
          result
              + dbos.runStep(
                  () -> {
                    ++this.stepWithRetryRuns;
                    throw new Exception("Will not ever run");
                  },
                  new StepOptions("inlineStepWithRetries")
                      .withRetriesAllowed(true)
                      .withMaxAttempts(2)
                      .withIntervalSeconds(0.01)
                      .withBackoffRate(2.0));
      ;
    } catch (Exception e) {
      caught = true;
    }
    if (!caught) {
      result += "<Step with retries should have thrown>";
    }
    if (System.currentTimeMillis() - ctime > 1000) {
      result += "<Retry took too long>";
    }
    result += this.stepWithRetryRuns;
    result += ".";

    return result;
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT)
public class StepsTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
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
