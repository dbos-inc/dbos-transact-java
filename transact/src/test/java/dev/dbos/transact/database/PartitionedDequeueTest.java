package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.dao.QueuesDAO;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.WorkflowStatusInternalBuilder;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The batched dequeue that claims every idle partition's head workflow in one transaction.
 *
 * <p>Each claim is polled rather than asserted on its first pass. The lock takes FOR UPDATE SKIP
 * LOCKED, and CockroachDB skips a key that still carries a write intent even after the transaction
 * that wrote it has committed (cockroachdb/cockroach#167582), so a claim right behind the inserts
 * can come back short. Claims accumulate across passes, which is sound because a claimed head makes
 * its partition busy and no later pass takes a second workflow from it.
 */
// Builds Queue values as fixtures, never to register one: only authoring a Queue by hand is
// deprecated.
@SuppressWarnings("removal")
public class PartitionedDequeueTest {

  private static final String APP_VERSION = "v-batched";

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose SystemDatabase sysdb;
  @AutoClose HikariDataSource dataSource;
  DbContext ctx;

  /** Which partition each seeded workflow is in, so a claim's order can be checked. */
  final Map<String, String> partitionOf = new HashMap<>();

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    MigrationManager.runMigrations(dbosConfig);
    sysdb = SystemDatabase.create(dbosConfig, null, dbosConfig.appName());
    dataSource = pgContainer.dataSource();
    String schema = SystemDatabase.sanitizeSchema(dbosConfig.databaseSchema());
    ctx = new DbContext(dataSource, schema, null, () -> false, null, null, new PollingLimiter(0));
  }

  /** Partitioned by a per-partition limit, as a queue registered today is. */
  private static Queue partitionedQueue(
      String name, Integer concurrency, Integer partitionConcurrency) {
    return new Queue(
        name,
        concurrency,
        null,
        false,
        false,
        null,
        partitionConcurrency,
        null,
        null,
        Queue.DEFAULT_POLLING_INTERVAL,
        null);
  }

  private void enqueue(Queue queue, String workflowId, String partition) {
    enqueue(queue, workflowId, partition, 0);
  }

  private void enqueue(Queue queue, String workflowId, String partition, int priority) {
    var status =
        WorkflowStatusInternalBuilder.create(workflowId)
            .workflowName("wf-name")
            .inputs("wf-inputs")
            .queueName(queue.name())
            .queuePartitionKey(partition)
            .priority(priority)
            // Pinned, not left null: a null version is only claimable while this worker runs the
            // latest registered one, which depends on what other tests left in
            // application_versions.
            .appVersion(APP_VERSION)
            .build();
    assertEquals(WorkflowState.ENQUEUED, sysdb.initWorkflowStatus(status, 5).status());
    partitionOf.put(workflowId, partition);
  }

  private List<String> claimOnce(Queue queue, long maxTasks, int sweepCap) throws SQLException {
    var claimed =
        QueuesDAO.startQueuedPartitionedWorkflows(
            ctx, queue, "exec", APP_VERSION, maxTasks, sweepCap);
    assertEquals(
        claimed.stream().sorted(Comparator.comparing(partitionOf::get)).toList(),
        claimed,
        "a claim must come back ordered by partition key");
    assertEquals(
        claimed.size(),
        claimed.stream().map(partitionOf::get).distinct().count(),
        "a claim must take at most one workflow per partition");
    return claimed;
  }

  /** Claims until {@code expected} workflows are taken or five seconds pass. */
  private List<String> claimUntil(Queue queue, int expected, long maxTasks, int sweepCap)
      throws Exception {
    var all = new ArrayList<String>();
    long deadline = System.currentTimeMillis() + 5_000;
    while (all.size() < expected && System.currentTimeMillis() < deadline) {
      var claimed = claimOnce(queue, maxTasks, sweepCap);
      assertTrue(
          claimed.size() <= Math.min(maxTasks, sweepCap),
          "a claim must stay within the sweep cap and the worker budget: " + claimed);
      all.addAll(claimed);
      if (claimed.isEmpty()) Thread.sleep(100);
    }
    return all;
  }

  private String status(String workflowId) throws SQLException {
    return DBUtils.getWorkflowRow(dataSource, workflowId).status();
  }

  @Test
  public void claimsOneHeadPerPartition() throws Exception {
    var queue = partitionedQueue("batched-heads", null, 1);
    // Enqueued out of partition order, so the claim's order comes from the keys, not insertion.
    enqueue(queue, "c-1", "c");
    enqueue(queue, "a-1", "a");
    enqueue(queue, "b-1", "b");
    enqueue(queue, "a-2", "a");
    enqueue(queue, "c-2", "c");

    var claimed = claimUntil(queue, 3, Long.MAX_VALUE, QueuesDAO.PARTITIONED_DEQUEUE_SWEEP_CAP);
    assertEquals(Set.of("a-1", "b-1", "c-1"), Set.copyOf(claimed));

    for (var id : claimed) {
      var row = DBUtils.getWorkflowRow(dataSource, id);
      assertEquals(WorkflowState.PENDING.name(), row.status());
      assertEquals("exec", row.executorId());
      assertEquals(1L, row.recoveryAttempts(), "the claim must count this dispatch");
    }
    assertEquals(WorkflowState.ENQUEUED.name(), status("a-2"));
    assertEquals(WorkflowState.ENQUEUED.name(), status("c-2"));

    // Every partition is now busy, so another sweep claims nothing.
    assertEquals(
        List.of(), claimOnce(queue, Long.MAX_VALUE, QueuesDAO.PARTITIONED_DEQUEUE_SWEEP_CAP));
  }

  @Test
  public void skipsPartitionWithAPendingWorkflow() throws Exception {
    var queue = partitionedQueue("batched-busy", null, 1);
    enqueue(queue, "busy-running", "busy");
    enqueue(queue, "busy-waiting", "busy");
    enqueue(queue, "idle-1", "idle");
    DBUtils.setWorkflowState(dataSource, "busy-running", WorkflowState.PENDING.name());

    var claimed = claimUntil(queue, 1, Long.MAX_VALUE, QueuesDAO.PARTITIONED_DEQUEUE_SWEEP_CAP);
    assertEquals(List.of("idle-1"), claimed);
    assertEquals(WorkflowState.ENQUEUED.name(), status("busy-waiting"));
  }

  @Test
  public void headFollowsPriority() throws Exception {
    var queue = partitionedQueue("batched-priority", null, 1);
    enqueue(queue, "low", "p", 5);
    enqueue(queue, "high", "p", 1);

    assertEquals(
        List.of("high"),
        claimUntil(queue, 1, Long.MAX_VALUE, QueuesDAO.PARTITIONED_DEQUEUE_SWEEP_CAP));
  }

  @Test
  public void workflowIdBreaksACreatedAtTie() throws Exception {
    var queue = partitionedQueue("batched-tie", null, 1);
    enqueue(queue, "tie-b", "p");
    enqueue(queue, "tie-a", "p");
    enqueue(queue, "tie-c", "p");
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "UPDATE \"%s\".workflow_status SET created_at = 1000 WHERE queue_name = ?"
                    .formatted(ctx.schema()))) {
      ps.setString(1, queue.name());
      assertEquals(3, ps.executeUpdate());
    }

    assertEquals(
        List.of("tie-a"),
        claimUntil(queue, 1, Long.MAX_VALUE, QueuesDAO.PARTITIONED_DEQUEUE_SWEEP_CAP));
  }

  @Test
  public void sweepCapBoundsTheClaim() throws Exception {
    var queue = partitionedQueue("batched-cap", null, 1);
    for (var p : List.of("a", "b", "c", "d", "e")) {
      enqueue(queue, p + "-1", p);
    }

    // With the cap binding, partitions are taken in key order.
    var first = claimUntil(queue, 1, Long.MAX_VALUE, 2);
    assertTrue(Set.of("a-1", "b-1").containsAll(first), "key order under the cap: " + first);

    var all = new ArrayList<>(first);
    all.addAll(claimUntil(queue, 5 - first.size(), Long.MAX_VALUE, 2));
    assertEquals(Set.of("a-1", "b-1", "c-1", "d-1", "e-1"), Set.copyOf(all));
  }

  @Test
  public void workerBudgetBoundsTheClaim() throws Exception {
    var queue = partitionedQueue("batched-budget", null, 1);
    for (var p : List.of("a", "b", "c", "d")) {
      enqueue(queue, p + "-1", p);
    }

    var all = claimUntil(queue, 4, 1, QueuesDAO.PARTITIONED_DEQUEUE_SWEEP_CAP);
    assertEquals(Set.of("a-1", "b-1", "c-1", "d-1"), Set.copyOf(all));

    assertEquals(List.of(), claimOnce(queue, 0, QueuesDAO.PARTITIONED_DEQUEUE_SWEEP_CAP));
  }

  @Test
  public void leavesOtherVersionsAndQueuesAlone() throws Exception {
    var queue = partitionedQueue("batched-scope", null, 1);
    var other = partitionedQueue("batched-scope-other", null, 1);
    enqueue(other, "other-queue", "a");
    var foreign =
        WorkflowStatusInternalBuilder.create("other-version")
            .workflowName("wf-name")
            .inputs("wf-inputs")
            .queueName(queue.name())
            .queuePartitionKey("b")
            .appVersion("v-someone-else")
            .build();
    sysdb.initWorkflowStatus(foreign, 5);
    enqueue(queue, "mine", "c");

    assertEquals(
        List.of("mine"),
        claimUntil(queue, 1, Long.MAX_VALUE, QueuesDAO.PARTITIONED_DEQUEUE_SWEEP_CAP));
    assertEquals(WorkflowState.ENQUEUED.name(), status("other-queue"));
    assertEquals(WorkflowState.ENQUEUED.name(), status("other-version"));
  }

  @Test
  public void listsEachPartitionWithAnEnqueuedWorkflowOnce() throws Exception {
    var queue = partitionedQueue("partition-list", null, 1);
    var other = partitionedQueue("partition-list-other", null, 1);
    enqueue(queue, "c-1", "c");
    enqueue(queue, "a-1", "a");
    enqueue(queue, "a-2", "a");
    enqueue(queue, "b-1", "b");
    enqueue(queue, "running", "d");
    enqueue(other, "other", "e");
    DBUtils.setWorkflowState(dataSource, "running", WorkflowState.PENDING.name());

    // Sorted here because the listing promises no order: every caller shuffles it anyway.
    assertEquals(
        List.of("a", "b", "c"),
        QueuesDAO.getQueuePartitions(ctx, queue.name()).stream().sorted().toList());
    assertEquals(List.of(), QueuesDAO.getQueuePartitions(ctx, "partition-list-empty"));
  }

  @Test
  public void rejectsAQueueThatCannotBatch() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            QueuesDAO.startQueuedPartitionedWorkflows(
                ctx, partitionedQueue("q", null, 2), "exec", APP_VERSION, 1, 1));
  }
}
