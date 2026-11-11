package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    dbos.registerQueue(DELAYED_QUEUE, new QueueOptions());
  }

  private static final String DELAYED_QUEUE = "internal-step-delayed";

  @AfterEach
  void afterEach() {
    DebugTriggers.clearDebugTrigger(DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT);
    DebugTriggers.clearDebugTrigger(DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMITTED);
    DebugTriggers.clearDebugTrigger(DebugTriggers.DEBUG_TRIGGER_CHILD_RECORDED);
    impl.release.countDown();
    impl.startChild.countDown();
    impl.mutualRelease.countDown();
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

  /** As {@link #failOnce}, but throws {@code failure} the first {@code times} times. */
  private static AtomicInteger failTimes(
      String trigger, String callerId, int times, SQLException failure) {
    var fired = new AtomicInteger();
    DebugTriggers.setDebugTrigger(
        trigger,
        new DebugTriggers.DebugAction()
            .setCallback(
                () -> {
                  if (callerId.equals(DBOSContextHolder.get().getWorkflowId())
                      && fired.getAndIncrement() < times) {
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

  /**
   * A conflict the database raises after the work, in the record or the owner check, and that
   * outlasts the retries is the step's answer as one raised in the work is: which statement a
   * deadlock picks is not the caller's doing.
   */
  @Test
  public void aConflictOutsideTheWorkThatOutlastsItsRetriesIsRecorded() throws Exception {
    var targetId = startBlocked("target");
    var callerId = "caller-%d".formatted(System.nanoTime());
    // Ten is every attempt the first cancel gets; the second cancel is left alone.
    var fired =
        failTimes(
            DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT,
            callerId,
            10,
            new SQLException("conflict", "40001"));

    var caller =
        dbos.<String, RuntimeException>startWorkflow(
            () -> proxy.cancelsAfterAFailure(targetId), new StartWorkflowOptions(callerId));

    assertEquals("first failed, target PENDING", caller.getResult());
    assertEquals(11, fired.get(), "ten conflicts, then the second cancel");
    assertEquals(WorkflowState.CANCELLED, statusOf(targetId));
    var recorded = stepsNamed(callerId, "DBOS.cancelWorkflow");
    assertEquals(2, recorded.size());
    assertTrue(recorded.get(0).error() != null, "the conflict is the first cancel's answer");
    assertNull(recorded.get(1).error());

    // A replay, which would now succeed at once, is refused the same way and takes the same branch.
    var fork =
        dbos.<String, RuntimeException>forkWorkflow(callerId, recorded.get(1).functionId() + 1);
    assertEquals("first failed, target PENDING", fork.getResult());
  }

  @Test
  public void aResumeThatFailsBeforeCommitLeavesNeitherEffectNorCheckpoint() throws Exception {
    var targetId = startBlocked("target");
    dbos.cancelWorkflow(targetId);
    var callerId = "caller-%d".formatted(System.nanoTime());
    var fired =
        failOnce(
            DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT,
            callerId,
            new SQLException("crash", "XX000"));

    var caller =
        dbos.startWorkflow(
            () -> proxy.resumesCatchingAFailure(targetId), new StartWorkflowOptions(callerId));

    assertEquals("failed, target CANCELLED", caller.getResult(), "the resume rolled back");
    assertTrue(fired.get());
    assertTrue(stepsNamed(callerId, "DBOS.resumeWorkflow").isEmpty());
  }

  @Test
  public void aResumeWhoseCommitAcknowledgementIsLostReplaysTheCheckpoint() throws Exception {
    var targetId = startBlocked("target");
    dbos.cancelWorkflow(targetId);
    var callerId = "caller-%d".formatted(System.nanoTime());
    var fired =
        failOnce(
            DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMITTED,
            callerId,
            new SQLTransientException());

    var caller =
        dbos.startWorkflow(
            () -> proxy.resumesCatchingAFailure(targetId), new StartWorkflowOptions(callerId));

    var result = caller.getResult();
    assertTrue(
        result.equals("resumed, target ENQUEUED") || result.equals("resumed, target PENDING"),
        result);
    assertTrue(fired.get());
    var recorded = stepsNamed(callerId, "DBOS.resumeWorkflow");
    assertEquals(1, recorded.size());
    assertNull(recorded.get(0).error());
  }

  /**
   * Two workflows cancelling each other lock each other's row and then their own. Whether one
   * simply commits first or the database breaks a deadlock between them, one cancel wins and the
   * other workflow, by then cancelled, commits nothing: never both, and neither hangs.
   */
  @Test
  public void twoWorkflowsCancellingEachOtherLeaveExactlyOneCancelled() throws Exception {
    var firstId = "mutual-a-%d".formatted(System.nanoTime());
    var secondId = "mutual-b-%d".formatted(System.nanoTime());
    dbos.startWorkflow(
        () -> proxy.cancelsWhenReleased(secondId), new StartWorkflowOptions(firstId));
    dbos.startWorkflow(
        () -> proxy.cancelsWhenReleased(firstId), new StartWorkflowOptions(secondId));
    assertTrue(impl.mutualWaiting.await(10, TimeUnit.SECONDS));
    impl.mutualRelease.countDown();

    var terminal = List.of(WorkflowState.SUCCESS, WorkflowState.CANCELLED, WorkflowState.ERROR);
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (!(terminal.contains(statusOf(firstId)) && terminal.contains(statusOf(secondId)))) {
      assertTrue(System.nanoTime() < deadline, "both workflows end");
      Thread.sleep(100);
    }
    var states = List.of(statusOf(firstId), statusOf(secondId));
    assertTrue(
        states.contains(WorkflowState.CANCELLED) && states.contains(WorkflowState.SUCCESS),
        "one cancelled the other and finished: " + states);
  }

  @Test
  public void aSendInAStartWorkflowLambdaIsRefused() throws Exception {
    var targetId = startBlocked("target");
    var callerId = "caller-%d".formatted(System.nanoTime());
    var caller =
        dbos.startWorkflow(
            () -> proxy.sendsInStartLambda(targetId), new StartWorkflowOptions(callerId));

    var result = caller.getResult();
    assertTrue(
        result.contains("DBOS operations cannot be called from the startWorkflow lambda"), result);
    assertTrue(stepsNamed(callerId, "DBOS.send").isEmpty());
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
  public void aSendToAMissingWorkflowReplaysItsRefusalOnceTheWorkflowExists() throws Exception {
    var destinationId = "missing-%d".formatted(System.nanoTime());
    var callerId = "caller-%d".formatted(System.nanoTime());

    var result =
        dbos.startWorkflow(() -> proxy.sends(destinationId), new StartWorkflowOptions(callerId))
            .getResult();
    assertEquals("refused", result);
    var recorded = stepsNamed(callerId, "DBOS.send");
    assertEquals(1, recorded.size());
    assertNotNull(recorded.get(0).error(), "the refusal is the step's answer");

    // With the destination there, a send would now succeed; the fork replays the refusal.
    dbos.startWorkflow(proxy::quick, new StartWorkflowOptions(destinationId)).getResult();
    assertEquals("refused", dbos.<String, RuntimeException>forkWorkflow(callerId, 1).getResult());
  }

  @Test
  public void aLostCommitAcknowledgementReplaysTheClaimedMessage() throws Exception {
    var receiverId = "receiver-%d".formatted(System.nanoTime());
    var fired =
        failOnce(
            DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMITTED,
            receiverId,
            new SQLTransientException());

    var receiver = dbos.startWorkflow(proxy::receives, new StartWorkflowOptions(receiverId));
    dbos.send(receiverId, "first", "topic");
    dbos.send(receiverId, "second", "topic");

    // The claim commits and its acknowledgement is lost. Claiming again would take the second.
    assertEquals("first", receiver.getResult(), "the retry replayed the claim it committed");
    assertTrue(fired.get());
    assertEquals(1, stepsNamed(receiverId, "DBOS.recv").size());
  }

  /**
   * A send records neither output nor serialization, as earlier releases did, and a replay of that
   * row returns rather than sending again or refusing the row.
   */
  @Test
  public void aSendRecordsNoOutputAndReplays() throws Exception {
    var destinationId = "destination-%d".formatted(System.nanoTime());
    dbos.startWorkflow(proxy::quick, new StartWorkflowOptions(destinationId)).getResult();
    var callerId = "caller-%d".formatted(System.nanoTime());

    assertEquals(
        "sent",
        dbos.startWorkflow(() -> proxy.sends(destinationId), new StartWorkflowOptions(callerId))
            .getResult());
    var recorded = stepsNamed(callerId, "DBOS.send");
    assertEquals(1, recorded.size());
    assertNull(recorded.get(0).output());
    assertNull(recorded.get(0).error());

    assertEquals("sent", dbos.<String, RuntimeException>forkWorkflow(callerId, 1).getResult());
  }

  @Test
  public void aTimedOutRecvIsRecordedFromTheStartOfItsWait() throws Exception {
    var receiverId = "receiver-%d".formatted(System.nanoTime());

    assertEquals(
        "nothing",
        dbos.startWorkflow(() -> proxy.receivesFor(1000), new StartWorkflowOptions(receiverId))
            .getResult());
    var recorded = stepsNamed(receiverId, "DBOS.recv");
    assertEquals(1, recorded.size());
    var step = recorded.get(0);
    assertTrue(
        step.completedAtEpochMs() - step.startedAtEpochMs() >= 900,
        "recorded %d ms".formatted(step.completedAtEpochMs() - step.startedAtEpochMs()));
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
    dbos.startWorkflow(proxy::counted, new StartWorkflowOptions(targetId)).getResult();
    var callerId = "caller-%d".formatted(System.nanoTime());
    var fired =
        failOnce(
            DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMITTED,
            callerId,
            new SQLTransientException());

    // A second rewind would run the target a third time, or be refused if it were still ENQUEUED.
    var caller =
        dbos.startWorkflow(() -> proxy.rewinds(targetId), new StartWorkflowOptions(callerId));

    assertEquals("done", caller.getResult());
    assertTrue(fired.get());
    assertEquals(1, stepsNamed(callerId, "DBOS.rewindWorkflow").size());
    assertNull(stepsNamed(callerId, "DBOS.rewindWorkflow").get(0).error());
    assertEquals("counted", dbos.retrieveWorkflow(targetId).getResult());
    assertEquals(2, impl.runsOf(targetId), "the target ran once, then once more for one rewind");
  }

  @Test
  public void attributesThatFailBeforeCommitAreNotSet() throws Exception {
    var targetId = startBlocked("target");
    var callerId = "caller-%d".formatted(System.nanoTime());
    var fired =
        failOnce(
            DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT,
            callerId,
            new SQLException("crash", "XX000"));

    var caller =
        dbos.startWorkflow(() -> proxy.labels(targetId), new StartWorkflowOptions(callerId));

    assertEquals("failed", caller.getResult());
    assertTrue(fired.get());
    assertNull(dbos.retrieveWorkflow(targetId).getStatus().attributes(), "the update rolled back");
    assertTrue(stepsNamed(callerId, "DBOS.updateWorkflowAttributes").isEmpty());
  }

  @Test
  public void aDelayThatFailsBeforeCommitIsNotSet() throws Exception {
    var targetId = "delayed-%d".formatted(System.nanoTime());
    dbos.startWorkflow(
        proxy::quick,
        new StartWorkflowOptions(targetId).withQueue(DELAYED_QUEUE).withDelay(Duration.ofDays(1)));
    var before = dbos.retrieveWorkflow(targetId).getStatus();
    assertEquals(WorkflowState.DELAYED, before.status());
    var callerId = "caller-%d".formatted(System.nanoTime());
    var fired =
        failOnce(
            DebugTriggers.DEBUG_TRIGGER_INTERNAL_STEP_COMMIT,
            callerId,
            new SQLException("crash", "XX000"));

    var caller =
        dbos.startWorkflow(() -> proxy.delays(targetId), new StartWorkflowOptions(callerId));

    assertEquals("failed", caller.getResult());
    assertTrue(fired.get());
    assertEquals(
        before.delayUntil(),
        dbos.retrieveWorkflow(targetId).getStatus().delayUntil(),
        "the delay rolled back");
    assertTrue(stepsNamed(callerId, "DBOS.setWorkflowDelay").isEmpty());
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

  String receives();

  String receivesFor(long millis);

  String sends(String destinationId);

  String rewinds(String targetId);

  String counted();

  String labels(String targetId);

  String delays(String targetId);

  String resumesCatchingAFailure(String targetId);

  String cancelsWhenReleased(String otherId) throws InterruptedException;

  String sendsInStartLambda(String targetId);

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
  private final Map<String, AtomicInteger> runs = new ConcurrentHashMap<>();
  final CountDownLatch mutualWaiting = new CountDownLatch(2);
  final CountDownLatch mutualRelease = new CountDownLatch(1);

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
  public String sends(String destinationId) {
    try {
      dbos.send(destinationId, "hello", "topic");
      return "sent";
    } catch (DBOSNonExistentWorkflowException e) {
      return "refused";
    }
  }

  @Override
  @Workflow
  public String receivesFor(long millis) {
    return dbos.<String>recv("topic", Duration.ofMillis(millis)).orElse("nothing");
  }

  @Override
  @Workflow
  public String receives() {
    return dbos.<String>recv("topic", Duration.ofSeconds(30)).orElse("nothing");
  }

  @Override
  @Workflow
  public String forks(String sourceId) {
    return dbos.forkWorkflow(sourceId, 0).workflowId();
  }

  @Override
  @Workflow
  public String counted() {
    runs.computeIfAbsent(DBOSContext.workflowId(), id -> new AtomicInteger()).incrementAndGet();
    return "counted";
  }

  int runsOf(String workflowId) {
    var count = runs.get(workflowId);
    return count == null ? 0 : count.get();
  }

  @Override
  @Workflow
  public String labels(String targetId) {
    try {
      dbos.updateWorkflowAttributes(targetId, Map.of("label", "set"));
      return "set";
    } catch (RuntimeException e) {
      return "failed";
    }
  }

  @Override
  @Workflow
  public String delays(String targetId) {
    try {
      dbos.setWorkflowDelay(targetId, Duration.ofDays(2));
      return "set";
    } catch (RuntimeException e) {
      return "failed";
    }
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

  @Override
  @Workflow
  public String resumesCatchingAFailure(String targetId) {
    String outcome;
    try {
      dbos.resumeWorkflow(targetId);
      outcome = "resumed";
    } catch (RuntimeException e) {
      outcome = "failed";
    }
    return outcome + ", target " + dbos.getWorkflowStatus(targetId).orElseThrow().status();
  }

  @Override
  @Workflow
  public String cancelsWhenReleased(String otherId) throws InterruptedException {
    mutualWaiting.countDown();
    mutualRelease.await();
    dbos.cancelWorkflow(otherId);
    return "cancelled " + otherId;
  }

  @Override
  @Workflow
  public String sendsInStartLambda(String targetId) {
    try {
      dbos.startWorkflow(
          () -> {
            dbos.send(targetId, "message", "topic");
            return self.quick();
          },
          new StartWorkflowOptions(DBOSContext.workflowId() + "-child"));
      return "not refused";
    } catch (RuntimeException e) {
      return String.valueOf(e.getCause() == null ? e.getMessage() : e.getCause().getMessage());
    }
  }
}
