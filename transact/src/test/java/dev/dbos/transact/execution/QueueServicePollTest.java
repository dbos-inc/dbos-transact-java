package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.Queue;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What one poll does when part of it fails. Driven through {@code QueueListenerTask} directly
 * rather than through a running queue service: the behaviour under test is which claims a poll
 * still makes after a failure, and a live scheduler would answer that only incidentally, on timing.
 */
// The failure fixtures partition through the deprecated flag on purpose: the sweep is the same
// either way, and it keeps them to one field. The worker-budget fixture cannot, because that flag
// rescopes workerConcurrency to the partition.
@SuppressWarnings("removal")
public class QueueServicePollTest {

  private static final Queue PARTITIONED =
      new Queue("q", null, null, false, true, null, Duration.ofMillis(200), null);
  private static final Queue PLAIN =
      new Queue("q", null, null, false, false, null, Duration.ofMillis(200), null);

  /**
   * Partitioned by a per-partition limit, with a queue-wide worker budget of three. The legacy flag
   * would not do here: under it workerConcurrency is enforced per partition, so there would be no
   * shared budget for the sweep to carry.
   */
  private static final Queue WORKER_BUDGETED =
      new Queue("q", null, 3, false, false, null, 1, null, null, Duration.ofMillis(200), null);

  private SystemDatabase systemDatabase;
  private DBOSExecutor dbosExecutor;
  private QueueService queueService;

  /** A contention failure as it arrives from the DAO: wrapped by dbRetry, SQLSTATE intact. */
  private static RuntimeException contention(String sqlState) {
    return new RuntimeException(new SQLException("contended", sqlState));
  }

  @BeforeEach
  public void setUp() {
    systemDatabase = mock(SystemDatabase.class);
    dbosExecutor = mock(DBOSExecutor.class);
    queueService = new QueueService(dbosExecutor, systemDatabase);
  }

  private QueueService.QueueListenerTask taskFor(Queue queue) {
    return queueService.new QueueListenerTask(queue, false);
  }

  @Test
  @DisplayName("a contended partition costs its own turn, not the rest of the sweep")
  public void contendedPartitionDoesNotStrandTheOthers() {
    when(systemDatabase.getQueuePartitions("q")).thenReturn(List.of("a", "b"));
    when(systemDatabase.startQueuedWorkflows(any(), any(), any(), eq("a"), anyLong(), anyLong()))
        .thenThrow(contention("55P03"));
    when(systemDatabase.startQueuedWorkflows(any(), any(), any(), eq("b"), anyLong(), anyLong()))
        .thenReturn(List.of("wf-b"));

    taskFor(PARTITIONED).sweepPartitions();

    verify(dbosExecutor).executeWorkflowById("wf-b");
  }

  @Test
  @DisplayName("a genuine error on a partition still stops the sweep")
  public void genuineErrorOnAPartitionPropagates() {
    // The sweep visits partitions in a random order, so which one fails is not fixed: every
    // partition throws, and the assertion is that the sweep stopped at the first rather than
    // carrying on through the rest.
    when(systemDatabase.getQueuePartitions("q")).thenReturn(List.of("a", "b", "c"));
    when(systemDatabase.startQueuedWorkflows(any(), any(), any(), any(), anyLong(), anyLong()))
        .thenThrow(new RuntimeException(new SQLException("disk on fire", "58030")));

    var task = taskFor(PARTITIONED);
    try {
      task.sweepPartitions();
      throw new AssertionError("a non-contention failure must reach the poll loop");
    } catch (RuntimeException expected) {
      // The poll loop classifies it there, logs at error, and lets the interval decay.
    }

    verify(systemDatabase, times(1))
        .startQueuedWorkflows(any(), any(), any(), any(), anyLong(), anyLong());
  }

  @Test
  @DisplayName("a workflow that will not start does not strand the rest of the batch")
  public void failedDispatchDoesNotStrandTheBatch() {
    when(systemDatabase.startQueuedWorkflows(any(), any(), any(), eq(null), anyLong(), anyLong()))
        .thenReturn(List.of("wf-1", "wf-2"));
    when(dbosExecutor.executeWorkflowById("wf-1")).thenThrow(contention("40001"));

    taskFor(PLAIN).processPartition(null, 0);

    verify(dbosExecutor).executeWorkflowById("wf-2");
  }

  @Test
  @DisplayName("a sweep carries its own claims into the budget the next partition is given")
  public void theSweepCarriesItsClaimsForward() {
    // Dispatch is asynchronous, so queueActiveCount will not yet report what the partitions
    // earlier in this sweep just claimed. Re-reading it per partition would hand every partition
    // the same budget and let them overshoot it together, which is why the sweep carries its own
    // claims in `claimed` and adds them to the count it snapshotted once.
    when(systemDatabase.getQueuePartitions("q")).thenReturn(List.of("a", "b"));
    when(dbosExecutor.queueActiveCount("q")).thenReturn(1L);
    when(systemDatabase.startQueuedWorkflows(any(), any(), any(), eq("a"), anyLong(), anyLong()))
        .thenReturn(List.of("wf-a"));
    when(systemDatabase.startQueuedWorkflows(any(), any(), any(), eq("b"), anyLong(), anyLong()))
        .thenReturn(List.of("wf-b"));

    taskFor(WORKER_BUDGETED).sweepPartitions();

    // One already running, so the first partition swept is told 1 and the second 2: the claim the
    // first made, counted before the second is given its budget.
    verify(systemDatabase).startQueuedWorkflows(any(), any(), any(), any(), eq(1L), anyLong());
    verify(systemDatabase).startQueuedWorkflows(any(), any(), any(), any(), eq(2L), anyLong());
    verify(dbosExecutor).executeWorkflowById("wf-a");
    verify(dbosExecutor).executeWorkflowById("wf-b");
  }

  @Test
  @DisplayName("a sweep stops at the partition that exhausts the queue-wide worker budget")
  public void theSweepStopsWhenTheBudgetIsSpent() {
    // Three partitions, a budget of three, and two workflows already running: the first partition
    // swept can claim, and once its claim spends the last of the budget no further partition may
    // be dequeued at all -- not dequeued and discarded, not visited.
    when(systemDatabase.getQueuePartitions("q")).thenReturn(List.of("a", "b", "c"));
    when(dbosExecutor.queueActiveCount("q")).thenReturn(2L);
    when(systemDatabase.startQueuedWorkflows(any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(List.of("wf-1"));

    taskFor(WORKER_BUDGETED).sweepPartitions();

    verify(systemDatabase, times(1))
        .startQueuedWorkflows(any(), any(), any(), any(), anyLong(), anyLong());
    verify(dbosExecutor, times(1)).executeWorkflowById("wf-1");
  }

  @Test
  @DisplayName("a sweep with no budget left dequeues nothing")
  public void aSpentBudgetSkipsTheSweepEntirely() {
    when(systemDatabase.getQueuePartitions("q")).thenReturn(List.of("a", "b"));
    when(dbosExecutor.queueActiveCount("q")).thenReturn(3L);

    taskFor(WORKER_BUDGETED).sweepPartitions();

    verify(systemDatabase, never())
        .startQueuedWorkflows(any(), any(), any(), any(), anyLong(), anyLong());
  }

  @Test
  @DisplayName("a queue whose config will not reload keeps polling on the config in hand")
  public void failedRefreshKeepsTheListenerAlive() {
    when(systemDatabase.findQueue("q")).thenThrow(contention("40001"));

    var task = queueService.new QueueListenerTask(PLAIN, true);

    // True means "keep this listener": the alternative is a poller that stops for good, silently,
    // because rescheduling is the only thing keeping it alive.
    assertTrue(task.refreshQueue());
    assertEquals(PLAIN, task.queue, "a failed reload must not disturb the config in hand");
  }

  @Test
  @DisplayName("a queue whose row is gone stops its listener")
  public void deletedQueueStopsTheListener() {
    when(systemDatabase.findQueue("q")).thenReturn(Optional.empty());

    assertFalse(queueService.new QueueListenerTask(PLAIN, true).refreshQueue());
  }

  // ── pause() ───────────────────────────────────────────────────────────────

  /**
   * Holds the next claim on {@code release} after counting down {@code claiming}, then returns
   * {@code claimed}.
   */
  private void blockTheClaim(
      CountDownLatch claiming, CountDownLatch release, List<String> claimed) {
    when(systemDatabase.startQueuedWorkflows(any(), any(), any(), any(), anyLong(), anyLong()))
        .thenAnswer(
            inv -> {
              claiming.countDown();
              assertTrue(release.await(10, TimeUnit.SECONDS), "claim never released");
              return claimed;
            });
  }

  private static Thread start(Runnable body) {
    var thread = new Thread(body);
    thread.setDaemon(true);
    thread.start();
    return thread;
  }

  /** Waits until {@code thread} is parked or done, so the test knows where it stands. */
  private static void awaitParkedOrDone(Thread thread) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (true) {
      var state = thread.getState();
      if (state == Thread.State.TIMED_WAITING
          || state == Thread.State.WAITING
          || state == Thread.State.TERMINATED) {
        return;
      }
      if (System.nanoTime() > deadline) throw new AssertionError("thread never parked: " + state);
      Thread.onSpinWait();
    }
  }

  @Test
  @DisplayName("pause() returns only after the pass in flight has claimed and dispatched")
  public void pauseDrainsThePassInFlight() throws Exception {
    var claiming = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    blockTheClaim(claiming, release, List.of("wf-1"));
    var events = new ConcurrentLinkedQueue<String>();
    when(dbosExecutor.executeWorkflowById("wf-1"))
        .thenAnswer(
            inv -> {
              events.add("dispatched wf-1");
              return null;
            });

    var poll = start(() -> taskFor(PLAIN).processPartition(null, 0));
    assertTrue(claiming.await(10, TimeUnit.SECONDS));

    // The pass is past its paused check and mid-claim: exactly the window #528 is about.
    var pauser =
        start(
            () -> {
              queueService.pause();
              events.add("pause returned");
            });
    awaitParkedOrDone(pauser);
    release.countDown();
    pauser.join(10_000);
    poll.join(10_000);

    assertEquals(List.of("dispatched wf-1", "pause returned"), List.copyOf(events));
  }

  @Test
  @DisplayName("once pause() returns, neither a claim nor a delayed transition runs")
  public void nothingRunsAfterPauseReturns() {
    queueService.pause();

    assertEquals(0, taskFor(PLAIN).processPartition(null, 0));
    queueService.transitionDelayedWorkflows();

    verifyNoInteractions(systemDatabase);
  }

  @Test
  @DisplayName("pause() from inside a pass does not wait on that pass")
  public void pauseFromInsideAPassDoesNotWaitOnItself() {
    when(systemDatabase.startQueuedWorkflows(any(), any(), any(), any(), anyLong(), anyLong()))
        .thenReturn(List.of("wf-1", "wf-2"));
    when(dbosExecutor.executeWorkflowById("wf-1"))
        .thenAnswer(
            inv -> {
              queueService.pause();
              return null;
            });

    // Without the exemption this waits out the drain timeout, far past the bound here.
    assertTimeoutPreemptively(
        Duration.ofSeconds(5), () -> taskFor(PLAIN).processPartition(null, 0));

    // What the pass had already claimed is still dispatched; the next claim is refused.
    verify(dbosExecutor).executeWorkflowById("wf-2");
    assertEquals(0, taskFor(PLAIN).processPartition(null, 0));
  }

  @Test
  @DisplayName("unpause() releases a pause() still waiting on a pass")
  public void unpauseSupersedesAWaitingPause() throws Exception {
    var claiming = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    blockTheClaim(claiming, release, List.of());

    var poll = start(() -> taskFor(PLAIN).processPartition(null, 0));
    try {
      assertTrue(claiming.await(10, TimeUnit.SECONDS));
      var pauser = start(queueService::pause);
      awaitParkedOrDone(pauser);

      queueService.unpause();
      pauser.join(10_000);
      assertFalse(pauser.isAlive(), "pause() kept waiting after unpause()");
    } finally {
      release.countDown();
      poll.join(10_000);
    }
  }

  @Test
  @DisplayName("an interrupted pause() returns with the interrupt restored")
  public void interruptedPauseRestoresTheInterrupt() throws Exception {
    var claiming = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    blockTheClaim(claiming, release, List.of());

    var poll = start(() -> taskFor(PLAIN).processPartition(null, 0));
    try {
      assertTrue(claiming.await(10, TimeUnit.SECONDS));
      var interrupted = new CountDownLatch(1);
      var pauser =
          start(
              () -> {
                queueService.pause();
                if (Thread.currentThread().isInterrupted()) interrupted.countDown();
              });
      awaitParkedOrDone(pauser);

      pauser.interrupt();
      assertTrue(interrupted.await(10, TimeUnit.SECONDS), "pause() swallowed the interrupt");
    } finally {
      release.countDown();
      poll.join(10_000);
    }
    // Still paused: giving up the wait does not give up the pause.
    assertEquals(0, taskFor(PLAIN).processPartition(null, 0));
  }
}
