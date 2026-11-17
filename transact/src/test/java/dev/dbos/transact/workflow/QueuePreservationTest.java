package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Cancelling, dead-lettering, timing out, resuming, rewinding and forking a workflow keep the
 * queue, partition and priority it was on, unless the caller names another. The rows are written by
 * hand on a queue nothing listens to, and driven through a client, so no executor moves them.
 */
public class QueuePreservationTest {

  private static final String QUEUE = "unlistened-queue";

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose HikariDataSource dataSource;
  @AutoClose DBOSClient client;
  SystemDatabase sysdb;

  @BeforeEach
  void beforeEach() {
    // Launching migrates the schema; the client needs nothing else from it.
    try (DBOS dbos = new DBOS(pgContainer.dbosConfig())) {
      dbos.launch();
    }
    dataSource = pgContainer.dataSource();
    client = pgContainer.dbosClient();
    sysdb = DBOSTestAccess.getSystemDatabase(client);
  }

  private String plant(WorkflowState status, String partition, int priority, String suspendKey)
      throws SQLException {
    var id = UUID.randomUUID().toString();
    var sql =
        """
          INSERT INTO "dbos".workflow_status
              (workflow_uuid, status, name, class_name, queue_name, queue_partition_key, priority,
               suspend_key, executor_id, recovery_attempts)
          VALUES (?, ?, 'planted', 'com.example.Planted', ?, ?, ?, ?, 'other-sdk', ?)
        """;
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, id);
      stmt.setString(2, status.name());
      stmt.setString(3, QUEUE);
      stmt.setString(4, partition);
      stmt.setInt(5, priority);
      stmt.setString(6, suspendKey);
      stmt.setInt(7, status == WorkflowState.PENDING ? 5 : 0);
      stmt.executeUpdate();
    }
    return id;
  }

  private String suspendKey(String id) throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "SELECT suspend_key FROM \"dbos\".workflow_status WHERE workflow_uuid = ?")) {
      stmt.setString(1, id);
      try (var rs = stmt.executeQuery()) {
        rs.next();
        return rs.getString(1);
      }
    }
  }

  private void assertQueue(String id, WorkflowState state, String queue, String partition, int pri)
      throws SQLException {
    var row = DBUtils.getWorkflowRow(dataSource, id);
    assertEquals(state.name(), row.status());
    assertEquals(queue, row.queueName());
    assertEquals(partition, row.queuePartitionKey());
    assertEquals(pri, row.priority());
  }

  @Test
  void cancelKeepsTheQueuePartitionAndPriority() throws Exception {
    var id = plant(WorkflowState.ENQUEUED, "p1", 7, null);

    client.cancelWorkflow(id);

    assertQueue(id, WorkflowState.CANCELLED, QUEUE, "p1", 7);
  }

  @Test
  void bulkCancelKeepsEachRowsQueue() throws Exception {
    var a = plant(WorkflowState.ENQUEUED, "a", 1, null);
    var b = plant(WorkflowState.DELAYED, "b", 2, null);

    client.cancelWorkflows(List.of(a, b));

    assertQueue(a, WorkflowState.CANCELLED, QUEUE, "a", 1);
    assertQueue(b, WorkflowState.CANCELLED, QUEUE, "b", 2);
  }

  @Test
  void deadLetteringKeepsTheQueue() throws Exception {
    var id = plant(WorkflowState.PENDING, "p1", 3, null);

    sysdb.deadLetterWorkflows(List.of(id), 5);

    assertQueue(id, WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED, QUEUE, "p1", 3);
  }

  @Test
  void aResumeWithNoQueueGoesBackToTheWorkflowsOwn() throws Exception {
    var id = plant(WorkflowState.ENQUEUED, "p1", 7, null);
    client.cancelWorkflow(id);

    client.resumeWorkflow(id);

    assertQueue(id, WorkflowState.ENQUEUED, QUEUE, "p1", 7);
  }

  @Test
  void aResumeThatNamesAQueueOrPartitionChangesOnlyThat() throws Exception {
    var id = plant(WorkflowState.ENQUEUED, "p1", 7, null);
    client.cancelWorkflow(id);

    // A partition alone moves it within its queue.
    client.resumeWorkflow(id, null, "p2");
    assertQueue(id, WorkflowState.ENQUEUED, QUEUE, "p2", 7);

    // A queue alone leaves the partition.
    client.cancelWorkflow(id);
    client.resumeWorkflow(id, "other-queue");
    assertQueue(id, WorkflowState.ENQUEUED, "other-queue", "p2", 7);

    // Both change both.
    client.cancelWorkflow(id);
    client.resumeWorkflow(id, QUEUE, "p3");
    assertQueue(id, WorkflowState.ENQUEUED, QUEUE, "p3", 7);
  }

  @Test
  void aBulkResumePutsEachRowBackInItsOwnPartition() throws Exception {
    var a = plant(WorkflowState.ENQUEUED, "a", 0, null);
    var b = plant(WorkflowState.ENQUEUED, "b", 0, null);
    client.cancelWorkflows(List.of(a, b));

    client.resumeWorkflows(List.of(a, b));

    assertQueue(a, WorkflowState.ENQUEUED, QUEUE, "a", 0);
    assertQueue(b, WorkflowState.ENQUEUED, QUEUE, "b", 0);
  }

  @Test
  void dbosChecksTheQueueAResumeNamesAndMovesEveryRowToTheKeyItGives() throws Exception {
    var a = plant(WorkflowState.ENQUEUED, "a", 1, null);
    var b = plant(WorkflowState.ENQUEUED, "b", 2, null);
    client.cancelWorkflows(List.of(a, b));

    try (var dbos = new DBOS(pgContainer.dbosConfig())) {
      dbos.launch();

      // A named queue must exist, and a key cannot go with one that is not partitioned. Nothing
      // is resumed when either is refused.
      assertThrows(IllegalArgumentException.class, () -> dbos.resumeWorkflow(a, "no-such-queue"));
      assertThrows(
          IllegalArgumentException.class,
          () -> dbos.resumeWorkflow(a, Constants.DBOS_INTERNAL_QUEUE, "a"));
      assertQueue(a, WorkflowState.CANCELLED, QUEUE, "a", 1);

      // A key alone is not checked: it goes to each workflow's own queue.
      dbos.resumeWorkflows(List.of(a, b), null, "moved");
    }

    assertQueue(a, WorkflowState.ENQUEUED, QUEUE, "moved", 1);
    assertQueue(b, WorkflowState.ENQUEUED, QUEUE, "moved", 2);
  }

  @Test
  void aWorkflowWithNoQueueResumesOntoTheInternalQueue() throws Exception {
    var id = UUID.randomUUID().toString();
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                """
                INSERT INTO "dbos".workflow_status
                    (workflow_uuid, status, name, class_name, executor_id, recovery_attempts)
                VALUES (?, 'CANCELLED', 'planted', 'com.example.Planted', 'other-sdk', 0)
                """)) {
      stmt.setString(1, id);
      stmt.executeUpdate();
    }

    client.resumeWorkflow(id);

    var row = DBUtils.getWorkflowRow(dataSource, id);
    assertEquals(Constants.DBOS_INTERNAL_QUEUE, row.queueName());
    assertNull(row.queuePartitionKey());
  }

  @Test
  void aForkRunsWhereItsOriginalRan() throws Exception {
    var original = plant(WorkflowState.SUCCESS, "p1", 7, null);

    var inherited = client.<Object, Exception>forkWorkflow(original, 0, new ForkOptions());
    assertQueue(inherited.workflowId(), WorkflowState.ENQUEUED, QUEUE, "p1", 7);

    // A partition alone moves the fork within the original's queue.
    var repartitioned =
        client.<Object, Exception>forkWorkflow(
            original, 0, new ForkOptions().withQueuePartitionKey("p9"));
    assertQueue(repartitioned.workflowId(), WorkflowState.ENQUEUED, QUEUE, "p9", 7);

    // A queue alone leaves the original's partition and priority.
    var requeued =
        client.<Object, Exception>forkWorkflow(
            original, 0, new ForkOptions().withQueue("other-queue"));
    assertQueue(requeued.workflowId(), WorkflowState.ENQUEUED, "other-queue", "p1", 7);
  }

  @Test
  void cancellingOrResumingASuspendedWorkflowClearsItsWakeKey() throws Exception {
    var cancelled = plant(WorkflowState.SUSPENDED, null, 0, "topic");
    client.cancelWorkflow(cancelled);
    assertNull(suspendKey(cancelled));

    var resumed = plant(WorkflowState.SUSPENDED, null, 0, "topic");
    client.resumeWorkflow(resumed);
    assertNull(suspendKey(resumed));
    assertQueue(resumed, WorkflowState.ENQUEUED, QUEUE, null, 0);
  }

  @Test
  void theTimeoutSweepClearsASuspendedWorkflowsWakeKey() throws Exception {
    var id = plant(WorkflowState.SUSPENDED, null, 0, "topic");
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "UPDATE \"dbos\".workflow_status SET workflow_deadline_epoch_ms = 1 WHERE"
                    + " workflow_uuid = ?")) {
      stmt.setString(1, id);
      stmt.executeUpdate();
    }

    assertEquals(List.of(id), sysdb.cancelTimedOutWorkflows(1_000));

    assertNull(suspendKey(id));
    assertQueue(id, WorkflowState.CANCELLED, QUEUE, null, 0);
  }
}
