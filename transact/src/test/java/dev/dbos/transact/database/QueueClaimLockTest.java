package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.DequeueResult.StopReason;
import dev.dbos.transact.database.dao.QueuesDAO;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.WorkflowStatusInternalBuilder;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Queue claims that spend a shared budget serialize on an advisory lock, so two workers that pick
 * different heads -- a higher-priority row enqueued between their reads, or two application
 * versions on one queue -- cannot both spend it.
 *
 * <p>Two claimers are interleaved through debug triggers: claimer B is paused mid-claim on a thread
 * of its own, claimer A runs to completion, and then B is released. A trigger point holds one
 * action for the whole process, so tests that set one take turns on it, and other tests dequeue
 * concurrently, so each pause matches B's thread by name and lets every other thread pass.
 *
 * <p>The interleavings assume PostgreSQL's advisory locks. CockroachDB has none to rely on and
 * claims at SERIALIZABLE instead, where the same interleavings end in a serialization failure
 * rather than a claim lock, so those tests are skipped there.
 */
// Builds Queue values as fixtures, never to register one: only authoring a Queue by hand is
// deprecated.
@SuppressWarnings("removal")
public class QueueClaimLockTest {

  private static final String VERSION = "v-claim-lock";
  private static final String OTHER_VERSION = "v-claim-lock-other";
  private static final String CLAIMER_B = "claimer-b";

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose SystemDatabase sysdb;
  @AutoClose HikariDataSource dataSource;
  DbContext ctx;
  ExecutorService claimerB;
  final CountDownLatch paused = new CountDownLatch(1);
  final CountDownLatch release = new CountDownLatch(1);
  String trigger;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    MigrationManager.runMigrations(dbosConfig);
    sysdb = SystemDatabase.create(dbosConfig, null, dbosConfig.appName());
    dataSource = pgContainer.dataSource();
    String schema = SystemDatabase.sanitizeSchema(dbosConfig.databaseSchema());
    ctx =
        new DbContext(
            dataSource,
            schema,
            null,
            () -> false,
            null,
            null,
            new PollingLimiter(0),
            PgContainer.USE_COCKROACH_DB);
    claimerB = Executors.newSingleThreadExecutor(r -> new Thread(r, CLAIMER_B));
  }

  @AfterEach
  void afterEach() {
    release.countDown();
    if (trigger != null) {
      DebugTriggers.clearDebugTrigger(trigger);
    }
    claimerB.shutdownNow();
  }

  private static Queue queue(
      Integer concurrency, Integer partitionConcurrency, Queue.RateLimit partitionRateLimit) {
    return new Queue(
        "claim-lock-" + UUID.randomUUID(),
        concurrency,
        null,
        true,
        false,
        null,
        partitionConcurrency,
        null,
        partitionRateLimit,
        Queue.DEFAULT_POLLING_INTERVAL,
        null);
  }

  private String enqueue(Queue queue, String partition, int priority, String version) {
    var workflowId = "wf-" + UUID.randomUUID();
    var status =
        WorkflowStatusInternalBuilder.create(workflowId)
            .workflowName("wf-name")
            .inputs("wf-inputs")
            .queueName(queue.name())
            .queuePartitionKey(partition)
            .priority(priority)
            // Pinned, not left null: a null version is only claimable while this worker runs the
            // latest registered one, which depends on what other tests left behind.
            .appVersion(version)
            .build();
    assertEquals(WorkflowState.ENQUEUED, sysdb.initWorkflowStatus(status, 5, null).status());
    return workflowId;
  }

  private DequeueResult claim(Queue queue, String executorId, String version, String partition)
      throws SQLException {
    return QueuesDAO.startQueuedWorkflows(
        ctx, queue, executorId, version, partition, 0, 0, "xid-" + executorId);
  }

  private List<String> sweep(Queue queue, String executorId, String version, int batchSize)
      throws SQLException {
    return QueuesDAO.startQueuedPartitionedWorkflows(
        ctx,
        queue,
        executorId,
        version,
        Long.MAX_VALUE,
        QueuesDAO.PARTITIONED_DEQUEUE_SWEEP_CAP,
        batchSize,
        "xid-" + executorId);
  }

  /** Pauses claimer B at {@code point} until released; every other thread passes. */
  private void pauseClaimerBAt(String point) {
    trigger = point;
    DebugTriggers.setDebugTrigger(
        point,
        new DebugTriggers.DebugAction()
            .setCallback(
                () -> {
                  if (!Thread.currentThread().getName().equals(CLAIMER_B)) return;
                  paused.countDown();
                  try {
                    release.await(20, TimeUnit.SECONDS);
                  } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                  }
                }));
  }

  /** Starts {@code body} as claimer B and waits until it pauses. */
  private <T> Future<T> startClaimerB(Callable<T> body) throws InterruptedException {
    var future = claimerB.submit(body);
    assertTrue(paused.await(10, TimeUnit.SECONDS), "claimer B never paused");
    return future;
  }

  /** The number of PENDING workflows on {@code queue}, by partition key. */
  private Map<String, Integer> pendingByPartition(Queue queue) throws SQLException {
    var counts = new HashMap<String, Integer>();
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                """
                SELECT queue_partition_key, COUNT(*) FROM "%s".workflow_status
                WHERE queue_name = ? AND status = ? GROUP BY queue_partition_key
                """
                    .formatted(ctx.schema()))) {
      ps.setString(1, queue.name());
      ps.setString(2, WorkflowState.PENDING.name());
      try (var rs = ps.executeQuery()) {
        while (rs.next()) {
          counts.put(rs.getString(1), rs.getInt(2));
        }
      }
    }
    return counts;
  }

  private String status(String workflowId) throws SQLException {
    return DBUtils.getWorkflowRow(dataSource, workflowId).status();
  }

  // ── The per-scope claim ──────────────────────────────────────────────────

  static Stream<Arguments> claimers() {
    return Stream.of(
        // A higher-priority row enqueued while B is mid-claim, which only A sees.
        Arguments.of("higher-priority", queue(1, null, null), null, false),
        // Two application versions on one queue, each worker picking its own version's head.
        Arguments.of("other-version", queue(1, null, null), null, true),
        Arguments.of(
            "partition, higher-priority",
            queue(null, 1, new Queue.RateLimit(100, Duration.ofSeconds(60))),
            "p",
            false),
        Arguments.of(
            "partition, other-version",
            queue(null, 1, new Queue.RateLimit(100, Duration.ofSeconds(60))),
            "p",
            true));
  }

  // One global trigger slot per point, so tests that pause on one take turns.
  @ResourceLock(DebugTriggers.DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT)
  @ParameterizedTest(name = "{0}")
  @MethodSource("claimers")
  public void aClaimerThatMissesANewHeadDoesNotOverspend(
      String name, Queue queue, String partition, boolean splitVersions) throws Exception {
    assumeFalse(PgContainer.USE_COCKROACH_DB, "CockroachDB claims at SERIALIZABLE, not by lock");
    String versionB = VERSION;
    String versionA = splitVersions ? OTHER_VERSION : VERSION;
    var headB = enqueue(queue, partition, 5, versionB);

    // B pauses after it has locked its head, before it marks it PENDING.
    pauseClaimerBAt(DebugTriggers.DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT);
    var claimB = startClaimerB(() -> claim(queue, "executor-b", versionB, partition));

    // A head B cannot see: it outranks the one B locked, or only A's version runs it. Its row is
    // not locked, so only the claim lock stops A from claiming it too.
    var headA = enqueue(queue, partition, 1, versionA);
    var claimA = claim(queue, "executor-a", versionA, partition);
    release.countDown();

    assertEquals(new DequeueResult(List.of(headB), null), claimB.get(10, TimeUnit.SECONDS));
    assertEquals(DequeueResult.stopped(StopReason.CLAIM_LOCK), claimA);
    assertEquals(WorkflowState.ENQUEUED.name(), status(headA));
    var expected = new HashMap<String, Integer>();
    expected.put(partition, 1);
    assertEquals(expected, pendingByPartition(queue));
  }

  /** Holds {@code key}'s advisory lock in a transaction of its own until the connection closes. */
  private Connection holdLock(long key) throws SQLException {
    var conn = dataSource.getConnection();
    conn.setAutoCommit(false);
    try (var ps = conn.prepareStatement("SELECT pg_advisory_xact_lock(?)")) {
      ps.setLong(1, key);
      ps.execute();
    }
    return conn;
  }

  /** Releases the transaction-scoped locks {@code conn} holds and returns it to the pool. */
  private static void releaseLocks(Connection conn) throws SQLException {
    try (conn) {
      conn.rollback();
    }
  }

  @Test
  public void aQueueWideBudgetLocksEveryPartition() throws Exception {
    assumeFalse(PgContainer.USE_COCKROACH_DB, "CockroachDB has no advisory locks");
    // A queue-wide concurrency limit is spent by every partition's claims, so each claims under
    // the whole queue's lock, the one an unpartitioned claim takes.
    var queue = queue(10, 2, null);
    var p0 = enqueue(queue, "p0", 0, VERSION);
    var p1 = enqueue(queue, "p1", 0, VERSION);

    var holder = holdLock(AdvisoryLocks.queueClaimKey(ctx.schema(), queue.name(), null));
    try {
      assertEquals(
          DequeueResult.stopped(StopReason.CLAIM_LOCK), claim(queue, "exec", VERSION, "p0"));
      assertEquals(
          DequeueResult.stopped(StopReason.CLAIM_LOCK), claim(queue, "exec", VERSION, "p1"));
    } finally {
      releaseLocks(holder);
    }
    assertEquals(List.of(p0), claim(queue, "exec", VERSION, "p0").workflowIds());
    assertEquals(List.of(p1), claim(queue, "exec", VERSION, "p1").workflowIds());
  }

  @Test
  public void aPartitionBudgetLocksOnlyItsPartition() throws Exception {
    assumeFalse(PgContainer.USE_COCKROACH_DB, "CockroachDB has no advisory locks");
    var queue = queue(null, 2, null);
    var p0 = enqueue(queue, "p0", 0, VERSION);
    var p1 = enqueue(queue, "p1", 0, VERSION);

    var holder = holdLock(AdvisoryLocks.queueClaimKey(ctx.schema(), queue.name(), "p0"));
    try {
      assertEquals(
          DequeueResult.stopped(StopReason.CLAIM_LOCK), claim(queue, "exec", VERSION, "p0"));
      assertEquals(List.of(p1), claim(queue, "exec", VERSION, "p1").workflowIds());
    } finally {
      releaseLocks(holder);
    }
    assertEquals(List.of(p0), claim(queue, "exec", VERSION, "p0").workflowIds());
  }

  @Test
  public void aWorkerOnlyBudgetTakesNoLock() throws Exception {
    assumeFalse(PgContainer.USE_COCKROACH_DB, "CockroachDB has no advisory locks to hold");
    // Nothing shared, so nothing to serialize: a held queue lock does not stop the claim.
    var queue =
        new Queue(
            "claim-lock-" + UUID.randomUUID(),
            null,
            5,
            false,
            false,
            null,
            Queue.DEFAULT_POLLING_INTERVAL,
            null);
    var wf = enqueue(queue, null, 0, VERSION);

    var holder = holdLock(AdvisoryLocks.queueClaimKey(ctx.schema(), queue.name(), null));
    try {
      assertEquals(List.of(wf), claim(queue, "exec", VERSION, null).workflowIds());
    } finally {
      releaseLocks(holder);
    }
  }

  // ── The batched partition sweep ──────────────────────────────────────────

  static Stream<Arguments> sweeps() {
    return Stream.of(
        // B holds p0's lock, so A leaves p0 to it and B claims the head it picked.
        Arguments.of(
            "holding the partition lock",
            DebugTriggers.DEBUG_TRIGGER_PARTITIONED_DEQUEUE_AFTER_CANDIDATES,
            false,
            false),
        Arguments.of(
            "holding the partition lock, other version",
            DebugTriggers.DEBUG_TRIGGER_PARTITIONED_DEQUEUE_AFTER_CANDIDATES,
            false,
            true),
        // B has not locked p0 yet, so A claims its head, and B, picking again under the lock,
        // finds p0 busy.
        Arguments.of(
            "before the partition lock",
            DebugTriggers.DEBUG_TRIGGER_PARTITIONED_DEQUEUE_BEFORE_LOCKS,
            true,
            false),
        Arguments.of(
            "before the partition lock, other version",
            DebugTriggers.DEBUG_TRIGGER_PARTITIONED_DEQUEUE_BEFORE_LOCKS,
            true,
            true));
  }

  @ResourceLock(DebugTriggers.DEBUG_TRIGGER_PARTITIONED_DEQUEUE_BEFORE_LOCKS)
  @ResourceLock(DebugTriggers.DEBUG_TRIGGER_PARTITIONED_DEQUEUE_AFTER_CANDIDATES)
  @ParameterizedTest(name = "{0}")
  @MethodSource("sweeps")
  public void aSweepThatMissesANewHeadNeverCoAdmits(
      String name, String point, boolean newHeadWins, boolean splitVersions) throws Exception {
    assumeFalse(PgContainer.USE_COCKROACH_DB, "CockroachDB claims at SERIALIZABLE, not by lock");
    var queue = queue(null, 1, null);
    String versionB = VERSION;
    String versionA = splitVersions ? OTHER_VERSION : VERSION;
    var oldHead = enqueue(queue, "p0", 5, versionB);

    pauseClaimerBAt(point);
    var sweepB = startClaimerB(() -> sweep(queue, "executor-b", versionB, 64));

    // A head B never saw, outranking the one it chose or of A's version only, and another
    // partition, which B's lock must not block.
    var newHead = enqueue(queue, "p0", 1, versionA);
    var otherHead = enqueue(queue, "p1", 5, versionA);
    var claimedA = sweep(queue, "executor-a", versionA, 64);
    release.countDown();
    var claimedB = sweepB.get(10, TimeUnit.SECONDS);

    if (newHeadWins) {
      assertEquals(List.of(newHead, otherHead), claimedA);
      assertEquals(List.of(), claimedB);
    } else {
      assertEquals(List.of(otherHead), claimedA);
      assertEquals(List.of(oldHead), claimedB);
    }
    assertEquals(Map.of("p0", 1, "p1", 1), pendingByPartition(queue));
  }

  @Test
  public void aSweepSkipsAPartitionAnotherSweepHolds() throws Exception {
    assumeFalse(PgContainer.USE_COCKROACH_DB, "CockroachDB has no advisory locks");
    var queue = queue(null, 1, null);
    var p0 = enqueue(queue, "p0", 0, VERSION);
    var p1 = enqueue(queue, "p1", 0, VERSION);

    // The two-int key every SDK's sweep takes on p0.
    var holder = dataSource.getConnection();
    holder.setAutoCommit(false);
    try (var ps = holder.prepareStatement("SELECT pg_advisory_xact_lock(?, ?)")) {
      ps.setInt(1, AdvisoryLocks.partitionClaimQueueId(ctx.schema(), queue.name()));
      ps.setInt(2, AdvisoryLocks.partitionClaimId("p0"));
      ps.execute();
    }
    try {
      assertEquals(List.of(p1), sweep(queue, "exec", VERSION, 64));
    } finally {
      releaseLocks(holder);
    }
    assertEquals(List.of(p0), sweep(queue, "exec", VERSION, 64));
  }

  /** Fails the {@code failing}th batch of this thread's sweeps, counting from 1. */
  private AtomicInteger failBatch(int failing) {
    var testThread = Thread.currentThread();
    var batches = new AtomicInteger();
    trigger = DebugTriggers.DEBUG_TRIGGER_PARTITIONED_DEQUEUE_AFTER_CANDIDATES;
    DebugTriggers.setDebugTrigger(
        trigger,
        new DebugTriggers.DebugAction()
            .setCallback(
                () -> {
                  if (Thread.currentThread() != testThread) return;
                  if (batches.incrementAndGet() == failing) {
                    throw new IllegalStateException("synthetic batch failure");
                  }
                }));
    return batches;
  }

  // The batch-failure tests below pin exact claims on the first pass. CockroachDB's SKIP LOCKED can
  // skip a row right behind its insert (see PartitionedDequeueTest), so they run on PostgreSQL.

  @ResourceLock(DebugTriggers.DEBUG_TRIGGER_PARTITIONED_DEQUEUE_AFTER_CANDIDATES)
  @Test
  public void aBatchThatFailsAfterAnotherCommittedKeepsItsClaims() throws Exception {
    assumeFalse(PgContainer.USE_COCKROACH_DB, "first-pass claims are not exact on CockroachDB");
    var queue = queue(null, 1, null);
    var p0 = enqueue(queue, "p0", 0, VERSION);
    var p1 = enqueue(queue, "p1", 0, VERSION);
    var p2 = enqueue(queue, "p2", 0, VERSION);

    var batches = failBatch(2);
    // The first batch committed, so its claim reaches dispatch; the failed batch and the one after
    // it are left to the next poll.
    assertEquals(List.of(p0), sweep(queue, "exec", VERSION, 1));
    assertEquals(2, batches.get());
    assertEquals(WorkflowState.PENDING.name(), status(p0));
    assertEquals(WorkflowState.ENQUEUED.name(), status(p1));
    assertEquals(WorkflowState.ENQUEUED.name(), status(p2));

    // The rollback released the failed batch's partition lock, so the next sweep claims the rest.
    DebugTriggers.clearDebugTrigger(trigger);
    assertEquals(List.of(p1, p2), sweep(queue, "exec", VERSION, 1));
  }

  @ResourceLock(DebugTriggers.DEBUG_TRIGGER_PARTITIONED_DEQUEUE_AFTER_CANDIDATES)
  @Test
  public void aFirstBatchThatFailsThrowsAndClaimsNothing() throws Exception {
    assumeFalse(PgContainer.USE_COCKROACH_DB, "first-pass claims are not exact on CockroachDB");
    var queue = queue(null, 1, null);
    var p0 = enqueue(queue, "p0", 0, VERSION);
    var p1 = enqueue(queue, "p1", 0, VERSION);

    failBatch(1);
    var thrown = assertThrows(IllegalStateException.class, () -> sweep(queue, "exec", VERSION, 1));
    assertEquals("synthetic batch failure", thrown.getMessage());
    assertEquals(WorkflowState.ENQUEUED.name(), status(p0));
    assertEquals(WorkflowState.ENQUEUED.name(), status(p1));

    DebugTriggers.clearDebugTrigger(trigger);
    assertEquals(List.of(p0, p1), sweep(queue, "exec", VERSION, 1));
  }
}
