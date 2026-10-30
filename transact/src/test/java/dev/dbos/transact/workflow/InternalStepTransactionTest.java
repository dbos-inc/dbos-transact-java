package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

/**
 * A DBOS operation called from a workflow commits its effect and its checkpoint together. A failure
 * before the commit leaves neither, so the replay does the operation once; a commit whose
 * acknowledgement is lost is found on retry and replayed, not done again.
 */
@ResourceLock(DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT)
@ResourceLock(DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMITTED)
public class InternalStepTransactionTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @AutoClose DBOS dbos;

  private InternalStepService proxy;
  private InternalStepServiceImpl impl;

  @BeforeEach
  void beforeEach() {
    dbos = new DBOS(pgContainer.dbosConfig());
    impl = new InternalStepServiceImpl(dbos);
    proxy = dbos.registerProxy(InternalStepService.class, impl);
    dbos.launch();
  }

  @AfterEach
  void afterEach() {
    DebugTriggers.clearDebugTrigger(DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT);
    DebugTriggers.clearDebugTrigger(DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMITTED);
    impl.release.countDown();
  }

  private String startBlocked(String prefix) throws Exception {
    var id = "%s-%d".formatted(prefix, System.nanoTime());
    dbos.startWorkflow(proxy::blocked, new StartWorkflowOptions(id));
    assertTrue(impl.blockedStarted.await(10, TimeUnit.SECONDS));
    return id;
  }

  private List<StepInfo> stepsNamed(String workflowId, String name) {
    return dbos.listWorkflowSteps(workflowId).stream()
        .filter(step -> step.functionName().equals(name))
        .toList();
  }

  private WorkflowState statusOf(String workflowId) {
    return dbos.retrieveWorkflow(workflowId).getStatus().status();
  }

  @Test
  public void aFailureBeforeCommitLeavesNeitherEffectNorCheckpoint() throws Exception {
    var targetId = startBlocked("target");
    DebugTriggers.setDebugTrigger(
        DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT,
        new DebugTriggers.DebugAction().setSqlExceptionToThrow(new SQLException("crash", "XX000")));

    // The workflow catches the failed cancel, looks at the target, and cancels again.
    var callerId = "caller-%d".formatted(System.nanoTime());
    var caller =
        dbos.startWorkflow(
            () -> proxy.cancelsAfterAFailure(targetId), new StartWorkflowOptions(callerId));

    assertEquals("first failed, target PENDING", caller.getResult(), "the cancel rolled back");
    assertEquals(WorkflowState.CANCELLED, statusOf(targetId));
    // Neither the failure, which the rollback undid, nor its checkpoint was recorded: only the
    // second cancel's.
    var recorded = stepsNamed(callerId, "DBOS.cancelWorkflow");
    assertEquals(1, recorded.size());
    assertNull(recorded.get(0).error());
  }

  @Test
  public void aTransientFailureBeforeCommitIsRetriedNotRecorded() throws Exception {
    var targetId = startBlocked("target");
    DebugTriggers.setDebugTrigger(
        DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT,
        new DebugTriggers.DebugAction().setSqlExceptionToThrow(new SQLTransientException()));

    var callerId = "caller-%d".formatted(System.nanoTime());
    var caller =
        dbos.startWorkflow(() -> proxy.cancels(targetId), new StartWorkflowOptions(callerId));

    assertEquals("done", caller.getResult());
    assertEquals(WorkflowState.CANCELLED, statusOf(targetId));
    var recorded = stepsNamed(callerId, "DBOS.cancelWorkflow");
    assertEquals(1, recorded.size());
    assertNull(recorded.get(0).error(), "the transient failure is not the step's answer");
  }

  @Test
  public void aLostCommitAcknowledgementReplaysTheCheckpoint() throws Exception {
    var targetId = startBlocked("target");
    var deletedId = "deleted-%d".formatted(System.nanoTime());
    dbos.startWorkflow(proxy::quick, new StartWorkflowOptions(deletedId)).getResult();
    DebugTriggers.setDebugTrigger(
        DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMITTED,
        new DebugTriggers.DebugAction().setSqlExceptionToThrow(new SQLTransientException()));

    // The cancel commits, its acknowledgement is lost, and the retry finds the checkpoint it wrote.
    var callerId = "caller-%d".formatted(System.nanoTime());
    var caller =
        dbos.startWorkflow(
            () -> proxy.cancelsThenDeletes(targetId, deletedId),
            new StartWorkflowOptions(callerId));

    assertEquals("done", caller.getResult());
    assertEquals(WorkflowState.CANCELLED, statusOf(targetId));
    assertEquals(1, stepsNamed(callerId, "DBOS.cancelWorkflow").size());
    assertEquals(1, stepsNamed(callerId, "DBOS.deleteWorkflow").size());
    assertTrue(dbos.getWorkflowStatus(deletedId).isEmpty(), "deleted");
  }

  @Test
  public void aCallFromInsideAStepRecordsNoCheckpointOfItsOwn() throws Exception {
    var targetId = startBlocked("target");
    var callerId = "caller-%d".formatted(System.nanoTime());
    var caller =
        dbos.startWorkflow(
            () -> proxy.cancelsFromStep(targetId), new StartWorkflowOptions(callerId));

    assertEquals("done", caller.getResult());
    assertEquals(WorkflowState.CANCELLED, statusOf(targetId));
    assertTrue(stepsNamed(callerId, "DBOS.cancelWorkflow").isEmpty());
    assertEquals(1, stepsNamed(callerId, "cancelStep").size());
  }
}

interface InternalStepService {
  String blocked() throws InterruptedException;

  String quick();

  String cancels(String targetId);

  String cancelsAfterAFailure(String targetId);

  String cancelsThenDeletes(String targetId, String deletedId);

  String cancelsFromStep(String targetId);
}

class InternalStepServiceImpl implements InternalStepService {
  private final DBOS dbos;
  final CountDownLatch blockedStarted = new CountDownLatch(1);
  final CountDownLatch release = new CountDownLatch(1);

  InternalStepServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public String blocked() throws InterruptedException {
    blockedStarted.countDown();
    release.await();
    return DBOSContext.workflowId();
  }

  @Override
  @Workflow
  public String quick() {
    return "quick";
  }

  @Override
  @Workflow
  public String cancels(String targetId) {
    dbos.cancelWorkflow(targetId);
    return "done";
  }

  @Override
  @Workflow
  public String cancelsAfterAFailure(String targetId) {
    String first;
    try {
      dbos.cancelWorkflow(targetId);
      first = "first committed";
    } catch (RuntimeException e) {
      first = "first failed";
    }
    var between = dbos.getWorkflowStatus(targetId).orElseThrow().status();
    dbos.cancelWorkflow(targetId);
    return first + ", target " + between;
  }

  @Override
  @Workflow
  public String cancelsThenDeletes(String targetId, String deletedId) {
    dbos.cancelWorkflow(targetId);
    dbos.deleteWorkflow(deletedId);
    return "done";
  }

  @Override
  @Workflow
  public String cancelsFromStep(String targetId) {
    dbos.runStep(() -> dbos.cancelWorkflow(targetId), "cancelStep");
    return "done";
  }
}
