package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface ForkTestService {

  String simpleWorkflow(String input);

  String parentChild(String input);

  String parentChildAsync(String input);

  String stepOne(String input);

  int stepTwo(Integer input);

  float stepThree(Float input);

  double stepFour(Double input);

  void stepFive(boolean b);

  String child1(Integer number);

  String child2(Float number);

  void setEventWorkflow(String key) throws InterruptedException;
}

class ForkTestServiceImpl implements ForkTestService {

  private final DBOS.Instance dbos;
  private ForkTestService proxy;

  public int step1Count;
  public int step2Count;
  public int step3Count;
  public int step4Count;
  public int step5Count;
  public int child1Count;
  public int child2Count;

  public ForkTestServiceImpl(DBOS.Instance dbos) {
    this.dbos = dbos;
  }

  public void setProxy(ForkTestService proxy) {
    this.proxy = proxy;
  }

  @Override
  @Workflow
  public String simpleWorkflow(String input) {
    proxy.stepOne("one");
    proxy.stepTwo(2);
    proxy.stepThree(3.3f);
    proxy.stepFour(4.4);
    proxy.stepFive(false);
    return input + input;
  }

  @Override
  @Workflow
  public String parentChild(String input) {

    proxy.stepOne("one");
    proxy.stepTwo(2);
    try (var o = new WorkflowOptions("child1").setContext()) {
      proxy.child1(3);
    }
    try (var o = new WorkflowOptions("child2").setContext()) {
      proxy.child2(4.4f);
    }
    proxy.stepFive(false);
    return input + input;
  }

  @Override
  @Workflow
  public String parentChildAsync(String input) {
    proxy.stepOne("one");
    proxy.stepTwo(2);
    dbos.startWorkflow(() -> proxy.child1(25), new StartWorkflowOptions("child1"));
    dbos.startWorkflow(() -> proxy.child2(25.75f), new StartWorkflowOptions("child2"));
    proxy.stepFive(false);
    return input + input;
  }

  @Override
  @Step
  public String stepOne(String input) {
    ++step1Count;
    return input;
  }

  @Override
  @Step
  public int stepTwo(Integer input) {
    ++step2Count;
    return input;
  }

  @Override
  @Step
  public float stepThree(Float input) {
    ++step3Count;
    return input;
  }

  @Override
  @Step
  public double stepFour(Double input) {
    ++step4Count;
    return input;
  }

  @Override
  @Step
  public void stepFive(boolean b) {
    ++step5Count;
  }

  @Override
  @Workflow
  public String child1(Integer number) {
    ++child1Count;
    return String.valueOf(number);
  }

  @Override
  @Workflow
  public String child2(Float number) {
    ++child2Count;
    return String.valueOf(number);
  }

  @Override
  @Workflow
  public void setEventWorkflow(String key) throws InterruptedException {
    for (int i = 1; i <= 5; i++) {
      dbos.setEvent(key, "event-%d".formatted(i));
      Thread.sleep(100);
    }
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT)
public class ForkTest {

  private static final Logger logger = LoggerFactory.getLogger(ForkTest.class);

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS.Instance dbos;
  @AutoClose HikariDataSource dataSource;

  private ForkTestServiceImpl impl;
  private ForkTestService proxy;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS.Instance(dbosConfig);
    dataSource = pgContainer.dataSource();

    impl = new ForkTestServiceImpl(dbos);
    proxy = dbos.registerWorkflows(ForkTestService.class, impl);
    impl.setProxy(proxy);

    dbos.launch();
  }

  @Test
  public void forkNonExistent() {
    var wfid = UUID.randomUUID().toString();
    assertThrows(DBOSNonExistentWorkflowException.class, () -> dbos.forkWorkflow(wfid, 2));
  }

  @Test
  public void testFork() throws Exception {

    String workflowId = "testFork-%d".formatted(System.currentTimeMillis());
    String result;
    try (var o = new WorkflowOptions(workflowId).setContext()) {
      result = proxy.simpleWorkflow("hello");
    }

    assertEquals("hellohello", result);
    var handle = dbos.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());
    assertNull(handle.getStatus().forkedFrom());

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.step3Count);
    assertEquals(1, impl.step4Count);
    assertEquals(1, impl.step5Count);

    logger.info("First execution done starting fork");

    var handle1 = dbos.forkWorkflow(workflowId, 0);
    assertEquals("hellohello", handle1.getResult());
    assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().status());
    assertNotEquals(handle1.workflowId(), workflowId);
    assertEquals(workflowId, handle1.getStatus().forkedFrom());

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(2, impl.step3Count);
    assertEquals(2, impl.step4Count);
    assertEquals(2, impl.step5Count);

    List<StepInfo> steps = dbos.listWorkflowSteps(handle1.workflowId());
    assertEquals(5, steps.size());

    logger.info("first fork done . starting 2nd fork ");

    var handle2 = dbos.forkWorkflow(workflowId, 2);
    assertEquals("hellohello", handle2.getResult());
    assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().status());
    assertNotEquals(handle2.workflowId(), workflowId);
    assertEquals(workflowId, handle2.getStatus().forkedFrom());

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(3, impl.step3Count);
    assertEquals(3, impl.step4Count);
    assertEquals(3, impl.step5Count);

    logger.info("Second fork done . starting 3rd fork ");

    var handle3 = dbos.forkWorkflow(workflowId, 4);
    assertEquals("hellohello", handle3.getResult());
    assertEquals(WorkflowState.SUCCESS.name(), handle3.getStatus().status());
    assertNotEquals(handle3.workflowId(), workflowId);
    assertEquals(workflowId, handle3.getStatus().forkedFrom());

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(3, impl.step3Count);
    assertEquals(3, impl.step4Count);
    assertEquals(4, impl.step5Count);
  }

  @Test
  public void testForkWorkflowId() throws Exception {

    var workflowId = "testForkWorkflowId-%d".formatted(System.currentTimeMillis());
    String result;
    try (var o = new WorkflowOptions(workflowId).setContext()) {
      result = proxy.simpleWorkflow("hello");
    }

    assertEquals("hellohello", result);
    var handle = dbos.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());
    assertNull(handle.getStatus().forkedFrom());

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.step3Count);
    assertEquals(1, impl.step4Count);
    assertEquals(1, impl.step5Count);

    logger.info("First execution done starting fork");

    var forkedWorkflowId = "forked-testForkWorkflowId-%d".formatted(System.currentTimeMillis());
    var options = new ForkOptions().withForkedWorkflowId(forkedWorkflowId);

    var handle1 = dbos.forkWorkflow(workflowId, 0, options);
    assertEquals("hellohello", handle1.getResult());
    assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().status());
    assertEquals(forkedWorkflowId, handle1.workflowId());
    assertEquals(workflowId, handle1.getStatus().forkedFrom());

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(2, impl.step3Count);
    assertEquals(2, impl.step4Count);
    assertEquals(2, impl.step5Count);

    List<StepInfo> steps = dbos.listWorkflowSteps(handle1.workflowId());
    assertEquals(5, steps.size());
  }

  @Test
  public void testForkAppVersion() throws Exception {

    var workflowId = "testForkWorkflowId-%d".formatted(System.currentTimeMillis());
    String result;
    try (var o = new WorkflowOptions(workflowId).setContext()) {
      result = proxy.simpleWorkflow("hello");
    }

    assertEquals("hellohello", result);
    var handle = dbos.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());
    assertNull(handle.getStatus().forkedFrom());
    assertNotNull(handle.getStatus().appVersion());

    DBOSTestAccess.getQueueService(dbos).pause();

    var handle1 = dbos.forkWorkflow(workflowId, 0);
    assertNotEquals(workflowId, handle1.workflowId());
    assertNull(handle1.getStatus().appVersion());
    assertEquals(workflowId, handle1.getStatus().forkedFrom());

    var appVersion = UUID.randomUUID().toString();
    var options = new ForkOptions().withApplicationVersion(appVersion);
    var handle2 = dbos.forkWorkflow(workflowId, 0, options);
    assertNotEquals(workflowId, handle2.workflowId());
    assertEquals(appVersion, handle2.getStatus().appVersion());
    assertEquals(workflowId, handle2.getStatus().forkedFrom());
  }

  @Test
  public void testForkTimeout() throws Exception {

    var workflowId = "testForkTimeout-%d".formatted(System.currentTimeMillis());
    String result;
    try (var o = new WorkflowOptions(workflowId).setContext()) {
      result = proxy.simpleWorkflow("hello");
    }

    assertEquals("hellohello", result);
    var handle = dbos.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());
    assertNull(handle.getStatus().forkedFrom());
    assertNotNull(handle.getStatus().appVersion());

    DBOSTestAccess.getQueueService(dbos).pause();

    var handle1 = dbos.forkWorkflow(workflowId, 0);
    assertNotEquals(workflowId, handle1.workflowId());
    assertNull(handle1.getStatus().timeoutMs());
    assertNull(handle1.getStatus().deadlineEpochMs());

    var options = new ForkOptions().withTimeout(Duration.ofSeconds(1));
    var handle2 = dbos.forkWorkflow(workflowId, 0, options);
    assertNotEquals(workflowId, handle2.workflowId());
    assertEquals(workflowId, handle2.getStatus().forkedFrom());
    assertEquals(1000, handle2.getStatus().timeoutMs());
    assertNull(handle2.getStatus().deadlineEpochMs());
  }

  @Test
  public void testForkTimeoutOriginal() throws Exception {

    var workflowId = "testForkTimeoutOriginal-%d".formatted(System.currentTimeMillis());
    String result;
    var options = new WorkflowOptions(workflowId).withTimeout(1, TimeUnit.SECONDS);
    try (var o = options.setContext()) {
      result = proxy.simpleWorkflow("hello");
    }

    assertEquals("hellohello", result);
    var handle = dbos.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());
    assertNull(handle.getStatus().forkedFrom());
    assertEquals(1000, handle.getStatus().timeoutMs());

    DBOSTestAccess.getQueueService(dbos).pause();

    var handle1 = dbos.forkWorkflow(workflowId, 0);
    assertNotEquals(workflowId, handle1.workflowId());
    assertEquals(1000, handle1.getStatus().timeoutMs());
    assertNull(handle1.getStatus().deadlineEpochMs());

    var forkOptions = new ForkOptions().withTimeout(Duration.ofSeconds(2));
    var handle2 = dbos.forkWorkflow(workflowId, 0, forkOptions);
    assertNotEquals(workflowId, handle2.workflowId());
    assertEquals(workflowId, handle2.getStatus().forkedFrom());
    assertEquals(2000, handle2.getStatus().timeoutMs());
    assertNull(handle2.getStatus().deadlineEpochMs());

    forkOptions = new ForkOptions().withNoTimeout();
    var handle3 = dbos.forkWorkflow(workflowId, 0, forkOptions);
    assertNotEquals(workflowId, handle3.workflowId());
    assertEquals(workflowId, handle3.getStatus().forkedFrom());
    assertNull(handle3.getStatus().timeoutMs());
    assertNull(handle2.getStatus().deadlineEpochMs());
  }

  @Test
  public void testParentChildFork() throws Exception {

    String workflowId = "testParentChildFork-%d".formatted(System.currentTimeMillis());
    WorkflowOptions options = new WorkflowOptions(workflowId);
    String result;
    try (var o = options.setContext()) {
      result = proxy.parentChild("hello");
    }

    assertEquals("hellohello", result);
    var handle = dbos.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());
    assertNull(handle.getStatus().forkedFrom());

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.child1Count);
    assertEquals(1, impl.child2Count);
    assertEquals(1, impl.step5Count);

    logger.info("First execution done starting fork");

    var handle1 = dbos.forkWorkflow(workflowId, 3);
    assertEquals("hellohello", handle1.getResult());
    assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().status());
    assertNotEquals(handle1.workflowId(), workflowId);
    assertEquals(workflowId, handle1.getStatus().forkedFrom());

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.child1Count);
    // child2count is 1 because the wf already executed even if fork doesn't copy the step
    assertEquals(1, impl.child2Count);
    assertEquals(2, impl.step5Count);

    List<StepInfo> steps = dbos.listWorkflowSteps(handle1.workflowId());
    assertEquals(5, steps.size());
  }

  @Test
  public void testParentChildAsyncFork() throws Exception {

    String workflowId = "testParentChildAsyncFork-%d".formatted(System.currentTimeMillis());
    WorkflowOptions options = new WorkflowOptions(workflowId);
    String result;
    try (var o = options.setContext()) {
      result = proxy.parentChildAsync("hello");
    }

    assertEquals("hellohello", result);
    var handle = dbos.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());
    assertNull(handle.getStatus().forkedFrom());

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.child1Count);
    assertEquals(1, impl.child2Count);
    assertEquals(1, impl.step5Count);

    logger.info("First execution done starting fork");

    var handle1 = dbos.forkWorkflow(workflowId, 3);
    assertEquals("hellohello", handle1.getResult());
    assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().status());
    assertNotEquals(handle1.workflowId(), workflowId);
    assertEquals(workflowId, handle1.getStatus().forkedFrom());

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.child1Count);
    // child2count is 1 because the wf already executed even if fork doesn't copy the step
    assertEquals(1, impl.child2Count);
    assertEquals(2, impl.step5Count);

    List<StepInfo> steps = dbos.listWorkflowSteps(handle1.workflowId());
    assertEquals(5, steps.size());
  }

  @Test
  public void forkEventHistory() throws Exception {
    DBOSTestAccess.getQueueService(dbos).pause();

    // Verify the workflow runs and the event's final value is correct
    var wfid = "forkEventHistory-%d".formatted(System.currentTimeMillis());
    var key = "event-key";
    var timeout = Duration.ofSeconds(1);
    var options = new StartWorkflowOptions(wfid);
    var handle = dbos.startWorkflow(() -> proxy.setEventWorkflow(key), options);
    assertDoesNotThrow(() -> handle.getResult());
    assertEquals("event-5", dbos.getEvent(handle.workflowId(), key, timeout));

    var events = DBUtils.getWorkflowEvents(dataSource, handle.workflowId());
    var eventHistory = DBUtils.getWorkflowEventHistory(dataSource, handle.workflowId());
    assertEquals(1, events.size());
    assertEquals(5, eventHistory.size());

    // Block the workflow so forked workflows cannot advance
    DBOSTestAccess.getQueueService(dbos).pause();

    // Fork the workflow from each step, verify the event is set to the appropriate value
    var forkZero = dbos.forkWorkflow(wfid, 0);
    assertNull(dbos.getEvent(forkZero.workflowId(), key, timeout));

    var forkOne = dbos.forkWorkflow(wfid, 1);
    assertEquals("event-1", dbos.getEvent(forkOne.workflowId(), key, timeout));

    var forkTwo = dbos.forkWorkflow(wfid, 2);
    assertEquals("event-2", dbos.getEvent(forkTwo.workflowId(), key, timeout));

    var forkThree = dbos.forkWorkflow(wfid, 3);
    assertEquals("event-3", dbos.getEvent(forkThree.workflowId(), key, timeout));

    var forkFour = dbos.forkWorkflow(wfid, 4);
    assertEquals("event-4", dbos.getEvent(forkFour.workflowId(), key, timeout));

    // Fork from a fork
    var forkFive = dbos.forkWorkflow(forkFour.workflowId(), 4);
    assertEquals("event-4", dbos.getEvent(forkFive.workflowId(), key, timeout));

    events = DBUtils.getWorkflowEvents(dataSource, forkThree.workflowId());
    eventHistory = DBUtils.getWorkflowEventHistory(dataSource, forkThree.workflowId());
    assertEquals(1, events.size());
    assertEquals(3, eventHistory.size());

    assertEquals(events.get(0).value(), eventHistory.get(2).value());

    // Unblock the forked workflows, verify they successfully complete
    DBOSTestAccess.getQueueService(dbos).unpause();
    for (var h : List.of(forkOne, forkTwo, forkThree, forkFour, forkFive)) {
      assertDoesNotThrow(() -> h.getResult());
      assertEquals("event-5", dbos.getEvent(h.workflowId(), key, timeout));
    }
  }
}
