package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.utils.PgContainer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A workflow may not cancel, resume or delete itself, from its own body or from one of its steps,
 * nor cancel or delete a tree it belongs to. Each clears or removes its own row, so the ownership
 * check on the call's checkpoint, in the same transaction, would roll the call back and leave the
 * workflow waiting on an outcome nothing writes. The call is refused instead, and the workflow can
 * catch the refusal and carry on. Targeting other workflows, its own children's trees included, is
 * unaffected.
 */
public class SelfTargetTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @AutoClose DBOS dbos;

  private SelfTargetService proxy;
  private SelfTargetServiceImpl impl;

  @BeforeEach
  void beforeEach() {
    dbos = new DBOS(pgContainer.dbosConfig());
    impl = new SelfTargetServiceImpl(dbos);
    proxy = dbos.registerProxy(SelfTargetService.class, impl);
    impl.self = proxy;
    dbos.launch();
  }

  private StepInfo stepNamed(String workflowId, String name) {
    return dbos.listWorkflowSteps(workflowId).stream()
        .filter(step -> step.functionName().equals(name))
        .findFirst()
        .orElseThrow(() -> new AssertionError("no step " + name + " in " + workflowId));
  }

  @Test
  public void aWorkflowCannotCancelItself() throws Exception {
    var workflowId = "self-cancel-%d".formatted(System.nanoTime());
    var handle = dbos.startWorkflow(proxy::cancelSelf, new StartWorkflowOptions(workflowId));

    assertEquals(
        "refused: workflow %s cannot cancel itself".formatted(workflowId), handle.getResult());
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
    assertEquals(1, impl.afterRefusal.get(), "the workflow carries on after the refusal");
    // Recorded as the call's step, so a replay is refused the same way.
    var step = stepNamed(workflowId, "DBOS.cancelWorkflow");
    assertEquals(IllegalArgumentException.class.getName(), step.error().className());
  }

  @Test
  public void aRecordedRefusalReplaysRatherThanBeingDecidedAgain() throws Exception {
    var workflowId = "self-cancel-replay-%d".formatted(System.nanoTime());
    var original = dbos.startWorkflow(proxy::cancelSelf, new StartWorkflowOptions(workflowId));
    var refusal = original.getResult();

    // A fork from the step after the cancel copies the cancel's recorded refusal and replays it.
    // Decided again, the refusal would name the fork, not the original.
    var cancelStep = stepNamed(workflowId, "DBOS.cancelWorkflow").functionId();
    var fork = dbos.<String, RuntimeException>forkWorkflow(workflowId, cancelStep + 1);
    assertEquals(refusal, fork.getResult());
    assertTrue(refusal.contains(workflowId), refusal);
  }

  @Test
  public void aWorkflowCannotResumeItself() throws Exception {
    var workflowId = "self-resume-%d".formatted(System.nanoTime());
    var handle = dbos.startWorkflow(proxy::resumeSelf, new StartWorkflowOptions(workflowId));

    assertEquals(
        "refused: workflow %s cannot resume itself".formatted(workflowId), handle.getResult());
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
    assertEquals(1, impl.afterRefusal.get(), "dispatched once, not again and again");
    var step = stepNamed(workflowId, "DBOS.resumeWorkflow");
    assertEquals(IllegalArgumentException.class.getName(), step.error().className());
  }

  @Test
  public void aWorkflowCannotDeleteItself() throws Exception {
    var workflowId = "self-delete-%d".formatted(System.nanoTime());
    var handle = dbos.startWorkflow(proxy::deleteSelf, new StartWorkflowOptions(workflowId));

    assertEquals(
        "refused: workflow %s cannot delete itself".formatted(workflowId), handle.getResult());
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
    var step = stepNamed(workflowId, "DBOS.deleteWorkflow");
    assertEquals(IllegalArgumentException.class.getName(), step.error().className());
  }

  @Test
  public void aStepCannotCancelOrResumeItsOwnWorkflow() throws Exception {
    var cancelId = "step-self-cancel-%d".formatted(System.nanoTime());
    var cancel = dbos.startWorkflow(proxy::cancelSelfInStep, new StartWorkflowOptions(cancelId));
    assertEquals(
        "refused: workflow %s cannot cancel itself".formatted(cancelId), cancel.getResult());
    assertEquals(WorkflowState.SUCCESS, cancel.getStatus().status());

    var resumeId = "step-self-resume-%d".formatted(System.nanoTime());
    var resume = dbos.startWorkflow(proxy::resumeSelfInStep, new StartWorkflowOptions(resumeId));
    assertEquals(
        "refused: workflow %s cannot resume itself".formatted(resumeId), resume.getResult());
    assertEquals(WorkflowState.SUCCESS, resume.getStatus().status());

    var deleteId = "step-self-delete-%d".formatted(System.nanoTime());
    var delete = dbos.startWorkflow(proxy::deleteSelfInStep, new StartWorkflowOptions(deleteId));
    assertEquals(
        "refused: workflow %s cannot delete itself".formatted(deleteId), delete.getResult());
    assertEquals(WorkflowState.SUCCESS, delete.getStatus().status());
  }

  @Test
  public void aRefusalAStepDoesNotCatchIsThatStepsError() throws Exception {
    var workflowId = "step-uncaught-%d".formatted(System.nanoTime());
    var handle =
        dbos.startWorkflow(proxy::cancelSelfInUncaughtStep, new StartWorkflowOptions(workflowId));

    assertEquals(
        "step failed: workflow %s cannot cancel itself".formatted(workflowId), handle.getResult());
    // Nothing of the cancel's own is recorded inside a step; the refusal is the step's result.
    var step = stepNamed(workflowId, "uncaughtCancel");
    assertEquals(IllegalArgumentException.class.getName(), step.error().className());
  }

  @Test
  public void aChildCannotCancelATreeItBelongsTo() throws Exception {
    var parentId = "tree-parent-%d".formatted(System.nanoTime());
    var handle = dbos.startWorkflow(proxy::parentOfCanceller, new StartWorkflowOptions(parentId));

    var result = handle.getResult();
    assertTrue(result.startsWith("refused: workflow "), result);
    assertTrue(
        result.endsWith("cannot cancel the tree of %s, which contains it".formatted(parentId)),
        result);
    // The walk cancelled the parent before reaching the child, and the refusal rolled it back.
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
  }

  @Test
  public void aChildCannotDeleteATreeItBelongsTo() throws Exception {
    var parentId = "delete-tree-parent-%d".formatted(System.nanoTime());
    var handle = dbos.startWorkflow(proxy::parentOfDeleter, new StartWorkflowOptions(parentId));

    var result = handle.getResult();
    assertTrue(
        result.endsWith("cannot delete the tree of %s, which contains it".formatted(parentId)),
        result);
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
  }

  @Test
  public void aParentCanCancelItsOwnChildsTree() throws Exception {
    var parentId = "cancels-child-%d".formatted(System.nanoTime());
    var childId = parentId + "-child";
    var handle =
        dbos.startWorkflow(
            () -> proxy.cancelsChildTree(childId), new StartWorkflowOptions(parentId));

    assertEquals("cancelled " + childId, handle.getResult());
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
    assertEquals(WorkflowState.CANCELLED, dbos.retrieveWorkflow(childId).getStatus().status());
    impl.release.countDown();
  }

  @Test
  public void aGrandchildCannotCancelItsGrandparentsTree() throws Exception {
    var rootId = "tree-root-%d".formatted(System.nanoTime());
    var handle =
        dbos.startWorkflow(proxy::grandparentOfCanceller, new StartWorkflowOptions(rootId));

    var result = handle.getResult();
    assertTrue(
        result.endsWith("cannot cancel the tree of %s, which contains it".formatted(rootId)),
        result);
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
  }

  @Test
  public void aWorkflowCanStillCancelAndResumeAnother() throws Exception {
    var otherId = "other-%d".formatted(System.nanoTime());
    var other = dbos.startWorkflow(proxy::blocked, new StartWorkflowOptions(otherId));
    assertTrue(impl.blockedStarted.await(10, TimeUnit.SECONDS));

    var callerId = "canceller-%d".formatted(System.nanoTime());
    var caller =
        dbos.startWorkflow(
            () -> proxy.cancelThenResume(otherId), new StartWorkflowOptions(callerId));
    assertEquals("done", caller.getResult());

    assertNull(stepNamed(callerId, "DBOS.cancelWorkflow").error());
    assertNull(stepNamed(callerId, "DBOS.resumeWorkflow").error());
    impl.release.countDown();
    assertEquals(otherId, other.getResult());
  }
}

interface SelfTargetService {
  String cancelSelf();

  String resumeSelf();

  String cancelSelfInStep();

  String resumeSelfInStep();

  String parentOfCanceller();

  String deleteSelf();

  String deleteSelfInStep();

  String cancelSelfInUncaughtStep();

  String parentOfDeleter();

  String deletesTree(String rootId);

  String cancelsChildTree(String childId) throws Exception;

  String grandparentOfCanceller();

  String middleOfTree(String rootId);

  String cancelsTree(String rootId);

  String blocked() throws InterruptedException;

  String cancelThenResume(String otherId);
}

class SelfTargetServiceImpl implements SelfTargetService {
  private final DBOS dbos;
  SelfTargetService self;
  final AtomicInteger afterRefusal = new AtomicInteger();
  final CountDownLatch blockedStarted = new CountDownLatch(1);
  final CountDownLatch release = new CountDownLatch(1);

  SelfTargetServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public String cancelSelf() {
    String result;
    try {
      dbos.cancelWorkflow(DBOSContext.workflowId());
      result = "not refused";
    } catch (IllegalArgumentException e) {
      result = "refused: " + e.getMessage();
    }
    dbos.runStep(() -> afterRefusal.incrementAndGet(), "afterRefusal");
    return result;
  }

  @Override
  @Workflow
  public String resumeSelf() {
    String result;
    try {
      dbos.resumeWorkflow(DBOSContext.workflowId());
      result = "not refused";
    } catch (IllegalArgumentException e) {
      result = "refused: " + e.getMessage();
    }
    dbos.runStep(() -> afterRefusal.incrementAndGet(), "afterRefusal");
    return result;
  }

  @Override
  @Workflow
  public String cancelSelfInStep() {
    var workflowId = DBOSContext.workflowId();
    return dbos.runStep(
        () -> {
          try {
            dbos.cancelWorkflow(workflowId);
            return "not refused";
          } catch (IllegalArgumentException e) {
            return "refused: " + e.getMessage();
          }
        },
        "cancelFromStep");
  }

  @Override
  @Workflow
  public String resumeSelfInStep() {
    var workflowId = DBOSContext.workflowId();
    return dbos.runStep(
        () -> {
          try {
            dbos.resumeWorkflow(workflowId);
            return "not refused";
          } catch (IllegalArgumentException e) {
            return "refused: " + e.getMessage();
          }
        },
        "resumeFromStep");
  }

  @Override
  @Workflow
  public String deleteSelf() {
    try {
      dbos.deleteWorkflow(DBOSContext.workflowId());
      return "not refused";
    } catch (IllegalArgumentException e) {
      return "refused: " + e.getMessage();
    }
  }

  @Override
  @Workflow
  public String deleteSelfInStep() {
    var workflowId = DBOSContext.workflowId();
    return dbos.runStep(
        () -> {
          try {
            dbos.deleteWorkflow(workflowId);
            return "not refused";
          } catch (IllegalArgumentException e) {
            return "refused: " + e.getMessage();
          }
        },
        "deleteFromStep");
  }

  @Override
  @Workflow
  public String cancelSelfInUncaughtStep() {
    var workflowId = DBOSContext.workflowId();
    try {
      dbos.runStep(() -> dbos.cancelWorkflow(workflowId), "uncaughtCancel");
      return "step succeeded";
    } catch (IllegalArgumentException e) {
      return "step failed: " + e.getMessage();
    }
  }

  @Override
  @Workflow
  public String parentOfDeleter() {
    return self.deletesTree(DBOSContext.workflowId());
  }

  @Override
  @Workflow
  public String deletesTree(String rootId) {
    try {
      dbos.deleteWorkflow(rootId, true);
      return "not refused";
    } catch (IllegalArgumentException e) {
      return "refused: " + e.getMessage();
    }
  }

  @Override
  @Workflow
  public String cancelsChildTree(String childId) throws Exception {
    dbos.startWorkflow(() -> self.blocked(), new StartWorkflowOptions(childId));
    dbos.cancelWorkflow(childId, true);
    return "cancelled " + childId;
  }

  @Override
  @Workflow
  public String parentOfCanceller() {
    return self.cancelsTree(DBOSContext.workflowId());
  }

  @Override
  @Workflow
  public String grandparentOfCanceller() {
    return self.middleOfTree(DBOSContext.workflowId());
  }

  @Override
  @Workflow
  public String middleOfTree(String rootId) {
    return self.cancelsTree(rootId);
  }

  @Override
  @Workflow
  public String cancelsTree(String rootId) {
    try {
      dbos.cancelWorkflow(rootId, true);
      return "not refused";
    } catch (IllegalArgumentException e) {
      return "refused: " + e.getMessage();
    }
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
  public String cancelThenResume(String otherId) {
    dbos.cancelWorkflow(otherId);
    dbos.resumeWorkflow(otherId);
    return "done";
  }
}
