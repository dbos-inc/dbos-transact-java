package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
@ResourceLock(DebugTriggers.DEBUG_TRIGGER_CHILD_RECORDED)
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
    impl.self = proxy;
    dbos.launch();
  }

  @AfterEach
  void afterEach() {
    DebugTriggers.clearDebugTrigger(DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT);
    DebugTriggers.clearDebugTrigger(DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMITTED);
    DebugTriggers.clearDebugTrigger(DebugTriggers.DEBUG_TRIGGER_CHILD_RECORDED);
    impl.release.countDown();
    impl.startChild.countDown();
  }

  /**
   * Arms {@code trigger} to throw {@code failure} once, on the calling workflow {@code callerId}'s
   * own thread. Test classes run concurrently and the triggers are global, so one armed for any
   * caller could be tripped, and spent, by another class's cancel.
   */
  private static AtomicBoolean failOnce(String trigger, String callerId, SQLException failure) {
    var fired = new AtomicBoolean();
    DebugTriggers.setDebugTrigger(
        trigger,
        new DebugTriggers.DebugAction()
            .setCallback(
                () -> {
                  if (callerId.equals(DBOSContextHolder.get().getWorkflowId())
                      && fired.compareAndSet(false, true)) {
                    sneakyThrow(failure);
                  }
                }));
    return fired;
  }

  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void sneakyThrow(Throwable t) throws E {
    throw (E) t;
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
    var callerId = "caller-%d".formatted(System.nanoTime());
    var fired =
        failOnce(
            DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT,
            callerId,
            new SQLException("crash", "XX000"));

    // The workflow catches the failed cancel, looks at the target, and cancels again.
    var caller =
        dbos.startWorkflow(
            () -> proxy.cancelsAfterAFailure(targetId), new StartWorkflowOptions(callerId));

    assertEquals("first failed, target PENDING", caller.getResult(), "the cancel rolled back");
    assertTrue(fired.get());
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
    var callerId = "caller-%d".formatted(System.nanoTime());
    var fired =
        failOnce(
            DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT,
            callerId,
            new SQLTransientException());

    var caller =
        dbos.startWorkflow(() -> proxy.cancels(targetId), new StartWorkflowOptions(callerId));

    assertEquals("done", caller.getResult());
    assertTrue(fired.get());
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
    var callerId = "caller-%d".formatted(System.nanoTime());
    var fired =
        failOnce(
            DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMITTED,
            callerId,
            new SQLTransientException());

    // The cancel commits, its acknowledgement is lost, and the retry finds the checkpoint it wrote.
    var caller =
        dbos.startWorkflow(
            () -> proxy.cancelsThenDeletes(targetId, deletedId),
            new StartWorkflowOptions(callerId));

    assertEquals("done", caller.getResult());
    assertTrue(fired.get());
    assertEquals(WorkflowState.CANCELLED, statusOf(targetId));
    assertEquals(1, stepsNamed(callerId, "DBOS.cancelWorkflow").size());
    assertEquals(1, stepsNamed(callerId, "DBOS.deleteWorkflow").size());
    assertTrue(dbos.getWorkflowStatus(deletedId).isEmpty(), "deleted");
  }

  private List<WorkflowStatus> forksOf(String workflowId) {
    return dbos.listWorkflows(new ListWorkflowsInput().withForkedFrom(workflowId));
  }

  @Test
  public void aForkThatFailsBeforeCommitLeavesNoFork() throws Exception {
    var sourceId = "source-%d".formatted(System.nanoTime());
    dbos.startWorkflow(proxy::quick, new StartWorkflowOptions(sourceId)).getResult();
    var callerId = "caller-%d".formatted(System.nanoTime());
    var fired =
        failOnce(
            DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT,
            callerId,
            new SQLException("crash", "XX000"));

    var caller =
        dbos.startWorkflow(() -> proxy.forks(sourceId), new StartWorkflowOptions(callerId));

    assertThrows(RuntimeException.class, caller::getResult);
    assertTrue(fired.get());
    assertTrue(forksOf(sourceId).isEmpty(), "the fork rolled back");
    assertTrue(stepsNamed(callerId, "DBOS.forkWorkflow").isEmpty(), "and so did its checkpoint");
  }

  @Test
  public void aLostCommitAcknowledgementReplaysTheFork() throws Exception {
    var sourceId = "source-%d".formatted(System.nanoTime());
    dbos.startWorkflow(proxy::quick, new StartWorkflowOptions(sourceId)).getResult();
    var callerId = "caller-%d".formatted(System.nanoTime());
    var fired =
        failOnce(
            DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMITTED,
            callerId,
            new SQLTransientException());

    var caller =
        dbos.startWorkflow(() -> proxy.forks(sourceId), new StartWorkflowOptions(callerId));

    var forkedId = caller.getResult();
    assertTrue(fired.get());
    var forks = forksOf(sourceId);
    assertEquals(1, forks.size(), "the retry found its checkpoint and did not fork again");
    assertEquals(forkedId, forks.get(0).workflowId());
    assertEquals(1, stepsNamed(callerId, "DBOS.forkWorkflow").size());
  }

  @Test
  public void aLostCommitAcknowledgementDoesNotRewindAgain() throws Exception {
    var targetId = "target-%d".formatted(System.nanoTime());
    dbos.startWorkflow(proxy::quick, new StartWorkflowOptions(targetId)).getResult();
    var callerId = "caller-%d".formatted(System.nanoTime());
    var fired =
        failOnce(
            DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMITTED,
            callerId,
            new SQLTransientException());

    // A second rewind would be refused: the first left the target ENQUEUED.
    var caller =
        dbos.startWorkflow(() -> proxy.rewinds(targetId), new StartWorkflowOptions(callerId));

    assertEquals("done", caller.getResult());
    assertTrue(fired.get());
    assertEquals(1, stepsNamed(callerId, "DBOS.rewindWorkflow").size());
    assertNull(stepsNamed(callerId, "DBOS.rewindWorkflow").get(0).error());
    assertEquals("quick", dbos.retrieveWorkflow(targetId).getResult(), "the target ran again");
  }

  /**
   * A child whose insert holds its parent's row lock when a cancel of the parent's tree arrives:
   * the cancel waits for the insert to commit, then reads the parent's children and finds it.
   */
  @Test
  public void aChildWhoseInsertHoldsItsParentIsCancelledWithTheTree() throws Exception {
    var parentId = "insert-first-%d".formatted(System.nanoTime());
    var childId = parentId + "-child";
    var holding = new CountDownLatch(1);
    var releaseInsert = new CountDownLatch(1);
    DebugTriggers.setDebugTrigger(
        DebugTriggers.DEBUG_TRIGGER_CHILD_RECORDED,
        new DebugTriggers.DebugAction()
            .setCallback(
                () -> {
                  if (parentId.equals(DBOSContextHolder.get().getWorkflowId())) {
                    holding.countDown();
                    try {
                      releaseInsert.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    }
                  }
                }));

    dbos.startWorkflow(() -> proxy.startsChild(childId), new StartWorkflowOptions(parentId));
    assertTrue(holding.await(10, TimeUnit.SECONDS), "the child's insert holds the parent");

    var cancel = CompletableFuture.runAsync(() -> dbos.cancelWorkflow(parentId, true));
    Thread.sleep(500);
    assertFalse(cancel.isDone(), "the cancel waits on the parent's row");
    releaseInsert.countDown();
    cancel.get(30, TimeUnit.SECONDS);

    assertEquals(WorkflowState.CANCELLED, statusOf(parentId));
    assertEquals(WorkflowState.CANCELLED, statusOf(childId), "the new child did not escape");
  }

  /**
   * A child inserted after a cancel of its parent's tree has committed finds its parent no longer
   * owned, and its insert rolls back: no child is left running.
   */
  @Test
  public void aChildInsertedAfterItsParentIsCancelledIsNeverCreated() throws Exception {
    var parentId = "cancel-first-%d".formatted(System.nanoTime());
    var childId = parentId + "-child";
    dbos.startWorkflow(
        () -> proxy.startsChildWhenReleased(childId), new StartWorkflowOptions(parentId));
    assertTrue(impl.parentWaiting.await(10, TimeUnit.SECONDS));

    dbos.cancelWorkflow(parentId, true);
    impl.startChild.countDown();
    assertTrue(impl.childAttempted.await(30, TimeUnit.SECONDS));

    assertEquals(WorkflowState.CANCELLED, statusOf(parentId));
    assertTrue(dbos.getWorkflowStatus(childId).isEmpty(), "the child's insert rolled back");
  }

  /**
   * A child whose insert holds its parent's row lock when a delete of the parent's tree arrives:
   * the delete waits for the insert to commit, then reads the parent's children and finds it.
   */
  @Test
  public void aChildWhoseInsertHoldsItsParentIsDeletedWithTheTree() throws Exception {
    var parentId = "delete-insert-first-%d".formatted(System.nanoTime());
    var childId = parentId + "-child";
    var holding = new CountDownLatch(1);
    var releaseInsert = new CountDownLatch(1);
    DebugTriggers.setDebugTrigger(
        DebugTriggers.DEBUG_TRIGGER_CHILD_RECORDED,
        new DebugTriggers.DebugAction()
            .setCallback(
                () -> {
                  if (parentId.equals(DBOSContextHolder.get().getWorkflowId())) {
                    holding.countDown();
                    try {
                      releaseInsert.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                      Thread.currentThread().interrupt();
                    }
                  }
                }));

    dbos.startWorkflow(() -> proxy.startsChild(childId), new StartWorkflowOptions(parentId));
    assertTrue(holding.await(10, TimeUnit.SECONDS), "the child's insert holds the parent");

    var delete = CompletableFuture.runAsync(() -> dbos.deleteWorkflow(parentId, true));
    Thread.sleep(500);
    assertFalse(delete.isDone(), "the delete waits on the parent's row");
    releaseInsert.countDown();
    delete.get(30, TimeUnit.SECONDS);

    assertTrue(dbos.getWorkflowStatus(parentId).isEmpty(), "the parent is deleted");
    assertTrue(dbos.getWorkflowStatus(childId).isEmpty(), "the new child did not escape");
  }

  /**
   * A child inserted after a delete of its parent's tree has committed finds its parent gone, and
   * its insert rolls back: no orphan is left running.
   */
  @Test
  public void aChildInsertedAfterItsParentIsDeletedIsNeverCreated() throws Exception {
    var parentId = "delete-first-%d".formatted(System.nanoTime());
    var childId = parentId + "-child";
    dbos.startWorkflow(
        () -> proxy.startsChildWhenReleased(childId), new StartWorkflowOptions(parentId));
    assertTrue(impl.parentWaiting.await(10, TimeUnit.SECONDS));

    dbos.deleteWorkflow(parentId, true);
    impl.startChild.countDown();
    assertTrue(impl.childAttempted.await(30, TimeUnit.SECONDS));

    assertTrue(dbos.getWorkflowStatus(parentId).isEmpty(), "the parent is deleted");
    assertTrue(dbos.getWorkflowStatus(childId).isEmpty(), "the child's insert rolled back");
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

  String forks(String sourceId);

  String rewinds(String targetId);

  String startsChild(String childId) throws Exception;

  String startsChildWhenReleased(String childId) throws Exception;
}

class InternalStepServiceImpl implements InternalStepService {
  private final DBOS dbos;
  InternalStepService self;
  final CountDownLatch blockedStarted = new CountDownLatch(1);
  final CountDownLatch release = new CountDownLatch(1);
  final CountDownLatch parentWaiting = new CountDownLatch(1);
  final CountDownLatch startChild = new CountDownLatch(1);
  final CountDownLatch childAttempted = new CountDownLatch(1);

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
  public String startsChild(String childId) throws Exception {
    dbos.startWorkflow(() -> self.blocked(), new StartWorkflowOptions(childId));
    return "started";
  }

  @Override
  @Workflow
  public String startsChildWhenReleased(String childId) throws Exception {
    parentWaiting.countDown();
    startChild.await();
    try {
      dbos.startWorkflow(() -> self.blocked(), new StartWorkflowOptions(childId));
    } finally {
      childAttempted.countDown();
    }
    return "started";
  }

  @Override
  @Workflow
  public String forks(String sourceId) {
    return dbos.forkWorkflow(sourceId, 0).workflowId();
  }

  @Override
  @Workflow
  public String rewinds(String targetId) {
    dbos.rewindWorkflow(targetId, 0);
    return "done";
  }

  @Override
  @Workflow
  public String cancelsFromStep(String targetId) {
    dbos.runStep(() -> dbos.cancelWorkflow(targetId), "cancelStep");
    return "done";
  }
}
