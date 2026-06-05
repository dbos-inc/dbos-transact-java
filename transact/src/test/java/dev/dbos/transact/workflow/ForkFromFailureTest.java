package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.PgContainer;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForkFromFailureTest {

  private static final Logger logger = LoggerFactory.getLogger(ForkFromFailureTest.class);

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;

  private ForkFromFailureServiceImpl impl;
  private ForkFromFailureService proxy;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);

    impl = new ForkFromFailureServiceImpl(dbos);
    proxy = dbos.registerProxy(ForkFromFailureService.class, impl);
    impl.setProxy(proxy);

    dbos.launch();
  }

  @Test
  public void testFromLastFailure() throws Exception {
    var workflowId = "testFromLastFailure-%d".formatted(System.currentTimeMillis());

    // First run: step2 fails on call #1
    impl.step2FailCount.set(1);
    assertThrows(
        Exception.class,
        () -> {
          try (var o = new WorkflowOptions(workflowId).setContext()) {
            proxy.threeStepWorkflow("hello");
          }
        });

    var original = dbos.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.ERROR, original.getStatus().status());
    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(0, impl.step3Count);

    // Fork from failure: should re-execute from step2
    var handle =
        dbos.forkFromFailure(workflowId, new ForkFromFailureOptions().withFromLastFailure());
    assertEquals("hellohello", handle.getResult());
    assertNotEquals(workflowId, handle.workflowId());
    assertEquals(workflowId, handle.getStatus().forkedFrom());
    assertTrue(dbos.retrieveWorkflow(workflowId).getStatus().wasForkedFrom());

    // step1 replayed from cache (not re-executed), step2 and step3 re-executed
    assertEquals(1, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(1, impl.step3Count);
  }

  @Test
  public void testFromLastFailureFallsBackToLastStep() throws Exception {
    // When no step has an error, fromLastFailure falls back to MAX(function_id)
    var workflowId = "testFromLastFailureFallback-%d".formatted(System.currentTimeMillis());

    try (var o = new WorkflowOptions(workflowId).setContext()) {
      proxy.threeStepWorkflow("hello");
    }
    assertEquals(WorkflowState.SUCCESS, dbos.retrieveWorkflow(workflowId).getStatus().status());
    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.step3Count);

    // No failure — should fall back to re-executing the last step (step3)
    var handle =
        dbos.forkFromFailure(workflowId, new ForkFromFailureOptions().withFromLastFailure());
    assertEquals("hellohello", handle.getResult());
    assertEquals(workflowId, handle.getStatus().forkedFrom());

    // step1 and step2 replayed from cache, step3 re-executed
    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(2, impl.step3Count);
  }

  @Test
  public void testFromLastStep() throws Exception {
    var workflowId = "testFromLastStep-%d".formatted(System.currentTimeMillis());

    try (var o = new WorkflowOptions(workflowId).setContext()) {
      proxy.threeStepWorkflow("hello");
    }
    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.step3Count);

    var handle = dbos.forkFromFailure(workflowId, new ForkFromFailureOptions().withFromLastStep());
    assertEquals("hellohello", handle.getResult());
    assertNotEquals(workflowId, handle.workflowId());
    assertEquals(workflowId, handle.getStatus().forkedFrom());

    // step1 and step2 replayed from cache, step3 re-executed
    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(2, impl.step3Count);
  }

  @Test
  public void testFromStep() throws Exception {
    var workflowId = "testFromStep-%d".formatted(System.currentTimeMillis());

    try (var o = new WorkflowOptions(workflowId).setContext()) {
      proxy.threeStepWorkflow("hello");
    }
    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.step3Count);

    // Fork from function_id=1 (step2): step1 copied, step2 and step3 re-executed
    var handle = dbos.forkFromFailure(workflowId, new ForkFromFailureOptions().withFromStep(1));
    assertEquals("hellohello", handle.getResult());
    assertNotEquals(workflowId, handle.workflowId());
    assertEquals(workflowId, handle.getStatus().forkedFrom());

    assertEquals(1, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(2, impl.step3Count);
  }

  @Test
  public void testFromStepName() throws Exception {
    var workflowId = "testFromStepName-%d".formatted(System.currentTimeMillis());

    try (var o = new WorkflowOptions(workflowId).setContext()) {
      proxy.threeStepWorkflow("hello");
    }
    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.step3Count);

    // Fork from the step named "stepThree": step1 and step2 copied, step3 re-executed
    var handle =
        dbos.forkFromFailure(
            workflowId, new ForkFromFailureOptions().withFromStepName("stepThree"));
    assertEquals("hellohello", handle.getResult());
    assertEquals(workflowId, handle.getStatus().forkedFrom());

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(2, impl.step3Count);
  }

  @Test
  public void testBatchForkFromFailure() throws Exception {
    var wfid1 = "testBatch-1-%d".formatted(System.currentTimeMillis());
    var wfid2 = "testBatch-2-%d".formatted(System.currentTimeMillis());

    try (var o = new WorkflowOptions(wfid1).setContext()) {
      proxy.threeStepWorkflow("hello");
    }
    try (var o = new WorkflowOptions(wfid2).setContext()) {
      proxy.threeStepWorkflow("world");
    }

    var handles =
        dbos.forkFromFailure(
            List.of(wfid1, wfid2), new ForkFromFailureOptions().withFromLastStep());
    assertEquals(2, handles.size());
    for (var h : handles) {
      assertNotNull(h.getResult());
    }
  }

  @Test
  public void testNoModeThrows() throws Exception {
    var workflowId = "testNoMode-%d".formatted(System.currentTimeMillis());
    try (var o = new WorkflowOptions(workflowId).setContext()) {
      proxy.threeStepWorkflow("hello");
    }
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.forkFromFailure(workflowId, new ForkFromFailureOptions()));
  }

  @Test
  public void testMultipleModesThrows() throws Exception {
    var workflowId = "testMultiModes-%d".formatted(System.currentTimeMillis());
    try (var o = new WorkflowOptions(workflowId).setContext()) {
      proxy.threeStepWorkflow("hello");
    }
    assertThrows(
        IllegalArgumentException.class,
        () ->
            dbos.forkFromFailure(
                workflowId, new ForkFromFailureOptions().withFromLastFailure().withFromLastStep()));
  }

  @Test
  public void testStepNameNotFoundThrows() throws Exception {
    var workflowId = "testStepNameNotFound-%d".formatted(System.currentTimeMillis());
    try (var o = new WorkflowOptions(workflowId).setContext()) {
      proxy.threeStepWorkflow("hello");
    }
    assertThrows(
        Exception.class,
        () ->
            dbos.forkFromFailure(
                workflowId, new ForkFromFailureOptions().withFromStepName("nonExistentStep")));
  }

  @Test
  public void testForkFromFailureWithAppVersion() throws Exception {
    var workflowId = "testForkFromFailureAppVersion-%d".formatted(System.currentTimeMillis());
    try (var o = new WorkflowOptions(workflowId).setContext()) {
      proxy.threeStepWorkflow("hello");
    }

    DBOSTestAccess.getQueueService(dbos).pause();

    var appVersion = UUID.randomUUID().toString();
    var handle =
        dbos.forkFromFailure(
            workflowId,
            new ForkFromFailureOptions().withFromLastStep().withApplicationVersion(appVersion));
    assertEquals(appVersion, handle.getStatus().appVersion());
    assertEquals(workflowId, handle.getStatus().forkedFrom());
  }
}

interface ForkFromFailureService {
  String threeStepWorkflow(String input);

  String stepOne(String input);

  String stepTwo(String input);

  String stepThree(String input);
}

class ForkFromFailureServiceImpl implements ForkFromFailureService {

  private final DBOS dbos;
  private ForkFromFailureService proxy;

  public int step1Count;
  public int step2Count;
  public int step3Count;
  public AtomicInteger step2FailCount = new AtomicInteger(0);

  public ForkFromFailureServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  public void setProxy(ForkFromFailureService proxy) {
    this.proxy = proxy;
  }

  @Override
  @Workflow
  public String threeStepWorkflow(String input) {
    proxy.stepOne(input);
    proxy.stepTwo(input);
    proxy.stepThree(input);
    return input + input;
  }

  @Override
  @Step(name = "stepOne")
  public String stepOne(String input) {
    ++step1Count;
    return input;
  }

  @Override
  @Step(name = "stepTwo")
  public String stepTwo(String input) {
    ++step2Count;
    if (step2FailCount.getAndDecrement() > 0) {
      throw new RuntimeException("step2 failing on purpose");
    }
    return input;
  }

  @Override
  @Step(name = "stepThree")
  public String stepThree(String input) {
    ++step3Count;
    return input;
  }
}
