package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.Queue;

import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What one poll does when part of it fails. Driven through {@code QueueListenerTask} directly
 * rather than through a running queue service: the behaviour under test is which claims a poll
 * still makes after a failure, and a live scheduler would answer that only incidentally, on timing.
 */
public class QueueServicePollTest {

  private static final Queue PARTITIONED =
      new Queue("q", null, null, false, true, null, Duration.ofMillis(200), null);
  private static final Queue PLAIN =
      new Queue("q", null, null, false, false, null, Duration.ofMillis(200), null);

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
    when(systemDatabase.startQueuedWorkflows(any(), any(), any(), eq("a"), anyLong()))
        .thenThrow(contention("55P03"));
    when(systemDatabase.startQueuedWorkflows(any(), any(), any(), eq("b"), anyLong()))
        .thenReturn(List.of("wf-b"));

    taskFor(PARTITIONED).sweepPartitions();

    verify(dbosExecutor).executeWorkflowById("wf-b");
  }

  @Test
  @DisplayName("a genuine error on a partition still stops the sweep")
  public void genuineErrorOnAPartitionPropagates() {
    when(systemDatabase.getQueuePartitions("q")).thenReturn(List.of("a", "b"));
    when(systemDatabase.startQueuedWorkflows(any(), any(), any(), eq("a"), anyLong()))
        .thenThrow(new RuntimeException(new SQLException("disk on fire", "58030")));

    var task = taskFor(PARTITIONED);
    try {
      task.sweepPartitions();
      throw new AssertionError("a non-contention failure must reach the poll loop");
    } catch (RuntimeException expected) {
      // The poll loop classifies it there, logs at error, and lets the interval decay.
    }

    verify(systemDatabase, never()).startQueuedWorkflows(any(), any(), any(), eq("b"), anyLong());
  }

  @Test
  @DisplayName("a workflow that will not start does not strand the rest of the batch")
  public void failedDispatchDoesNotStrandTheBatch() {
    when(systemDatabase.startQueuedWorkflows(any(), any(), any(), eq(null), anyLong()))
        .thenReturn(List.of("wf-1", "wf-2"));
    when(dbosExecutor.executeWorkflowById("wf-1")).thenThrow(contention("40001"));

    taskFor(PLAIN).processPartition(null);

    verify(dbosExecutor).executeWorkflowById("wf-2");
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
}
