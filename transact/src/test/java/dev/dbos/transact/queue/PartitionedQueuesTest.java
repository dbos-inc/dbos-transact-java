package dev.dbos.transact.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowState;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface ResumingTestService {
  int stuckWorkflow() throws InterruptedException;

  int regularWorkflow();
}

class ResumingTestServiceImpl implements ResumingTestService {

  public CountDownLatch startLatch = new CountDownLatch(1);
  public CountDownLatch blockingLatch = new CountDownLatch(1);

  @Override
  @Workflow
  public int stuckWorkflow() throws InterruptedException {
    startLatch.countDown();
    blockingLatch.await();
    return 13;
  }

  @Override
  @Workflow
  public int regularWorkflow() {
    return 42;
  }
}

interface PartitionsTestService {
  String blockedWorkflow() throws InterruptedException;

  String normalWorkflow();
}

class PartitionsTestServiceImpl implements PartitionsTestService {
  public CountDownLatch waitingLatch = new CountDownLatch(1);
  public CountDownLatch blockingLatch = new CountDownLatch(1);

  @Override
  @Workflow
  public String blockedWorkflow() throws InterruptedException {
    waitingLatch.countDown();
    blockingLatch.await();
    assertNotNull(DBOSContext.workflowId());
    return DBOSContext.workflowId();
  }

  @Override
  @Workflow
  public String normalWorkflow() {
    assertNotNull(DBOSContext.workflowId());
    return DBOSContext.workflowId();
  }
}

interface PartitionLimitTestService {
  String blockedWorkflow() throws InterruptedException;
}

class PartitionLimitTestServiceImpl implements PartitionLimitTestService {
  public final CountDownLatch blockingLatch = new CountDownLatch(1);
  public final AtomicInteger started = new AtomicInteger();

  @Override
  @Workflow
  public String blockedWorkflow() throws InterruptedException {
    started.incrementAndGet();
    blockingLatch.await();
    return DBOSContext.workflowId();
  }
}

@SuppressWarnings(
    "removal") // exercises the deprecated partitionQueue flag alongside its replacement
public class PartitionedQueuesTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  private DBOSConfig dbosConfig;
  @AutoClose private DBOS dbos;
  @AutoClose private HikariDataSource dataSource;

  @BeforeEach
  void setup() {
    this.dbosConfig = pgContainer.dbosConfig();
    this.dbos = new DBOS(dbosConfig);
    this.dataSource = pgContainer.dataSource();
  }

  @Test
  public void testResumingQueuedPartitionedWorkflows() throws Exception {
    String queue = "testQueue";

    var impl = new ResumingTestServiceImpl();
    var proxy = dbos.registerProxy(ResumingTestService.class, impl);
    dbos.launch();
    dbos.registerQueue(queue, QueueOptions.empty().andConcurrency(1).andPartitionQueue(true));

    var options = new StartWorkflowOptions().withQueue(queue).withQueuePartitionKey("key");
    var wfid = UUID.randomUUID().toString();

    // Enqueue a blocked workflow and two regular workflows on a queue with concurrency 1
    var blockedHandle = dbos.startWorkflow(() -> proxy.stuckWorkflow(), options);
    var regHandle1 =
        dbos.startWorkflow(() -> proxy.regularWorkflow(), options.withWorkflowId(wfid));
    var regHandle2 = dbos.startWorkflow(() -> proxy.regularWorkflow(), options);

    // Verify that the blocked workflow starts and is PENDING while the regular workflows remain
    // ENQUEUED.
    impl.startLatch.await();
    assertEquals(WorkflowState.PENDING, blockedHandle.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED, regHandle1.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED, regHandle2.getStatus().status());

    // Resume a regular workflow. Verify it completes.
    dbos.resumeWorkflow(wfid);
    assertEquals(42, regHandle1.getResult());
    assertEquals(WorkflowState.SUCCESS, regHandle1.getStatus().status());

    // Complete the blocked workflow. Verify the second regular workflow also completes.
    impl.blockingLatch.countDown();
    assertEquals(13, blockedHandle.getResult());
    assertEquals(42, regHandle2.getResult());

    assertTrue(DBUtils.queueEntriesCleanedUp(dataSource));
  }

  @Test
  public void testQueuePartitions() throws Exception {
    String queue = "testQueue";
    String partitionlessQueue = "partitionless-queue";

    var impl = new PartitionsTestServiceImpl();
    var proxy = dbos.registerProxy(PartitionsTestService.class, impl);
    dbos.launch();
    dbos.registerQueue(partitionlessQueue, QueueOptions.empty());
    dbos.registerQueue(queue, QueueOptions.empty().andWorkerConcurrency(1).andPartitionQueue(true));

    var blockedPartitionKey = "blocked";
    var normalPartitionKey = "normal";

    // Enqueue a blocked workflow and a normal workflow on
    // the blocked partition. Verify the blocked workflow starts
    // but the normal workflow is stuck behind it.
    var options =
        new StartWorkflowOptions().withQueue(queue).withQueuePartitionKey(blockedPartitionKey);
    var blockedBlockedHandle = dbos.startWorkflow(() -> proxy.blockedWorkflow(), options);
    var blockedNormalHandle = dbos.startWorkflow(() -> proxy.normalWorkflow(), options);

    impl.waitingLatch.await();
    assertEquals(WorkflowState.PENDING, blockedBlockedHandle.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED, blockedNormalHandle.getStatus().status());
    assertEquals(blockedPartitionKey, blockedBlockedHandle.getStatus().queuePartitionKey());
    assertEquals(blockedPartitionKey, blockedNormalHandle.getStatus().queuePartitionKey());

    // Enqueue a normal workflow on the other partition and verify it runs normally
    var normalHandle =
        dbos.startWorkflow(
            () -> proxy.normalWorkflow(), options.withQueuePartitionKey(normalPartitionKey));
    assertEquals(normalHandle.workflowId(), normalHandle.getResult());

    // Unblock the blocked partition and verify its workflows complete
    impl.blockingLatch.countDown();
    assertEquals(blockedBlockedHandle.workflowId(), blockedBlockedHandle.getResult());
    assertEquals(blockedNormalHandle.workflowId(), blockedNormalHandle.getResult());

    try (var client = pgContainer.dbosClient()) {
      var className = "dev.dbos.transact.queue.PartitionsTestServiceImpl";
      var wfName = "normalWorkflow";
      var nqOptions =
          new DBOSClient.EnqueueOptions(wfName, className, queue)
              .withQueuePartitionKey(blockedPartitionKey);
      var clientHandle = client.enqueueWorkflow(nqOptions, null);
      assertEquals(clientHandle.workflowId(), clientHandle.getResult());
    }

    // You can only enqueue on a partitioned queue with a partition key
    assertThrows(
        IllegalArgumentException.class,
        () ->
            dbos.startWorkflow(
                () -> proxy.normalWorkflow(), new StartWorkflowOptions().withQueue(queue)));

    // Deduplication is not supported for partitioned queues
    assertThrows(
        IllegalArgumentException.class,
        () ->
            dbos.startWorkflow(
                () -> proxy.normalWorkflow(), options.withDeduplicationId("dedupe")));

    // You can only enqueue with a partition key on a partitioned queue
    assertThrows(
        IllegalArgumentException.class,
        () ->
            dbos.startWorkflow(
                () -> proxy.normalWorkflow(),
                new StartWorkflowOptions()
                    .withQueue(partitionlessQueue)
                    .withQueuePartitionKey("test")));

    assertTrue(DBUtils.queueEntriesCleanedUp(dataSource));
  }

  @Test
  public void testPartitionKeyOnNonPartitionedQueue() throws Exception {
    String queue = "non-partitioned-queue";
    var impl = new PartitionsTestServiceImpl();
    var proxy = dbos.registerProxy(PartitionsTestService.class, impl);
    dbos.launch();
    dbos.registerQueue(queue, QueueOptions.empty());
    var options = new StartWorkflowOptions().withQueue(queue).withQueuePartitionKey("partition-1");
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.startWorkflow(() -> proxy.normalWorkflow(), options));
  }

  @Test
  public void testPartitionedQueueWithoutPartitionKey() throws Exception {
    String queue = "partitioned-queue";
    var impl = new PartitionsTestServiceImpl();
    var proxy = dbos.registerProxy(PartitionsTestService.class, impl);
    dbos.launch();
    dbos.registerQueue(queue, QueueOptions.empty().andPartitionQueue(true));
    var options = new StartWorkflowOptions().withQueue(queue);
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.startWorkflow(() -> proxy.normalWorkflow(), options));
  }

  @Test
  public void testPartitionKeyWithDeduplicationID() throws Exception {
    String queue = "partitioned-queue";
    var impl = new PartitionsTestServiceImpl();
    var proxy = dbos.registerProxy(PartitionsTestService.class, impl);
    dbos.launch();
    dbos.registerQueue(queue, QueueOptions.empty().andPartitionQueue(true));

    var options =
        new StartWorkflowOptions()
            .withQueue(queue)
            .withQueuePartitionKey("partition-1")
            .withDeduplicationId("dedupe");
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.startWorkflow(() -> proxy.normalWorkflow(), options));
  }

  /** Waits until {@code counter} reaches {@code target}, or fails once {@code timeoutMs} passes. */
  private static void awaitCount(AtomicInteger counter, int target, long timeoutMs)
      throws InterruptedException {
    var deadline = System.currentTimeMillis() + timeoutMs;
    while (counter.get() < target && System.currentTimeMillis() < deadline) {
      Thread.sleep(20);
    }
    assertEquals(target, counter.get());
  }

  /**
   * A per-partition limit partitions the queue on its own: the caller sets no partitionQueue flag,
   * yet the queue demands a partition key and gates each partition separately. The stored flag is
   * derived from the limits, so it reads back set, but not as legacy partitioning.
   */
  @Test
  public void testPartitionConcurrencyPartitionsTheQueue() throws Exception {
    String queue = "partition-concurrency-queue";
    var impl = new PartitionLimitTestServiceImpl();
    var proxy = dbos.registerProxy(PartitionLimitTestService.class, impl);
    dbos.launch();
    dbos.registerQueue(queue, QueueOptions.empty().andPartitionConcurrency(1));

    var registered = dbos.findQueue(queue).orElseThrow();
    assertTrue(registered.isPartitioned());
    // Derived onto the legacy column, which is what other SDKs read to decide the same thing.
    assertTrue(registered.partitioningEnabled());
    assertFalse(registered.isLegacyPartitioned());

    // Two workflows on partition "a" and one on partition "b". partitionConcurrency=1 lets one
    // from each partition run, so "b" is not stuck behind "a"'s backlog.
    var a = new StartWorkflowOptions().withQueue(queue).withQueuePartitionKey("a");
    var b = new StartWorkflowOptions().withQueue(queue).withQueuePartitionKey("b");
    var a1 = dbos.startWorkflow(() -> proxy.blockedWorkflow(), a);
    var a2 = dbos.startWorkflow(() -> proxy.blockedWorkflow(), a);
    var b1 = dbos.startWorkflow(() -> proxy.blockedWorkflow(), b);

    awaitCount(impl.started, 2, 10_000);
    assertEquals(WorkflowState.PENDING, a1.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED, a2.getStatus().status());
    assertEquals(WorkflowState.PENDING, b1.getStatus().status());

    impl.blockingLatch.countDown();
    assertEquals(a1.workflowId(), a1.getResult());
    assertEquals(a2.workflowId(), a2.getResult());
    assertEquals(b1.workflowId(), b1.getResult());
    assertTrue(DBUtils.queueEntriesCleanedUp(dataSource));
  }

  /**
   * partitionRateLimit bounds each partition over its period, which needs the claim to be marked
   * rate-limited: the limiter counts only rows carrying that mark, so a queue limited solely per
   * partition would otherwise count none of its own claims and start the full limit every poll.
   */
  @Test
  public void testPartitionRateLimitIsCountedAcrossPolls() throws Exception {
    String queue = "partition-rate-limit-queue";
    var impl = new PartitionLimitTestServiceImpl();
    var proxy = dbos.registerProxy(PartitionLimitTestService.class, impl);
    dbos.launch();
    dbos.registerQueue(
        queue, QueueOptions.empty().andPartitionRateLimit(2, Duration.ofSeconds(30)));

    var registered = dbos.findQueue(queue).orElseThrow();
    assertTrue(registered.isPartitioned());

    // Three on "a" and one on "b", with a period long enough that nothing refills mid-test. Two
    // from "a" may start; the third waits for the next period. "b" has its own budget.
    var a = new StartWorkflowOptions().withQueue(queue).withQueuePartitionKey("a");
    var b = new StartWorkflowOptions().withQueue(queue).withQueuePartitionKey("b");
    var a1 = dbos.startWorkflow(() -> proxy.blockedWorkflow(), a);
    var a2 = dbos.startWorkflow(() -> proxy.blockedWorkflow(), a);
    var a3 = dbos.startWorkflow(() -> proxy.blockedWorkflow(), a);
    var b1 = dbos.startWorkflow(() -> proxy.blockedWorkflow(), b);

    awaitCount(impl.started, 3, 10_000);
    // Several polls have run by now; without the mark each would hand back the full limit again
    // and a3 would have started too.
    Thread.sleep(1_000);
    assertEquals(3, impl.started.get(), "the partition limiter must hold across polls");
    assertEquals(WorkflowState.ENQUEUED, a3.getStatus().status());

    impl.blockingLatch.countDown();
    assertEquals(a1.workflowId(), a1.getResult());
    assertEquals(a2.workflowId(), a2.getResult());
    assertEquals(b1.workflowId(), b1.getResult());
  }

  /** partitionWorkerConcurrency bounds each partition on this executor and partitions the queue. */
  @Test
  public void testPartitionWorkerConcurrencyPartitionsTheQueue() throws Exception {
    String queue = "partition-worker-concurrency-queue";
    var impl = new PartitionLimitTestServiceImpl();
    var proxy = dbos.registerProxy(PartitionLimitTestService.class, impl);
    dbos.launch();
    dbos.registerQueue(queue, QueueOptions.empty().andPartitionWorkerConcurrency(1));

    assertTrue(dbos.findQueue(queue).orElseThrow().isPartitioned());

    var a = new StartWorkflowOptions().withQueue(queue).withQueuePartitionKey("a");
    var b = new StartWorkflowOptions().withQueue(queue).withQueuePartitionKey("b");
    var a1 = dbos.startWorkflow(() -> proxy.blockedWorkflow(), a);
    var a2 = dbos.startWorkflow(() -> proxy.blockedWorkflow(), a);
    var b1 = dbos.startWorkflow(() -> proxy.blockedWorkflow(), b);

    awaitCount(impl.started, 2, 10_000);
    assertEquals(WorkflowState.PENDING, a1.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED, a2.getStatus().status());
    assertEquals(WorkflowState.PENDING, b1.getStatus().status());

    impl.blockingLatch.countDown();
    assertEquals(a1.workflowId(), a1.getResult());
    assertEquals(a2.workflowId(), a2.getResult());
    assertEquals(b1.workflowId(), b1.getResult());
    assertTrue(DBUtils.queueEntriesCleanedUp(dataSource));
  }

  /**
   * The two scopes bind at once: partitionConcurrency would let one workflow per partition run, but
   * the queue-wide concurrency caps the whole queue below that.
   */
  @Test
  public void testQueueWideLimitBoundsPartitionLimit() throws Exception {
    String queue = "both-scopes-queue";
    var impl = new PartitionLimitTestServiceImpl();
    var proxy = dbos.registerProxy(PartitionLimitTestService.class, impl);
    dbos.launch();
    dbos.registerQueue(queue, QueueOptions.empty().andConcurrency(1).andPartitionConcurrency(1));

    var a = new StartWorkflowOptions().withQueue(queue).withQueuePartitionKey("a");
    var b = new StartWorkflowOptions().withQueue(queue).withQueuePartitionKey("b");
    var a1 = dbos.startWorkflow(() -> proxy.blockedWorkflow(), a);
    var b1 = dbos.startWorkflow(() -> proxy.blockedWorkflow(), b);

    awaitCount(impl.started, 1, 10_000);
    // Give the sweep time to visit the other partition, which the queue-wide limit must refuse.
    Thread.sleep(1_000);
    assertEquals(1, impl.started.get());

    impl.blockingLatch.countDown();
    assertEquals(a1.workflowId(), a1.getResult());
    assertEquals(b1.workflowId(), b1.getResult());
    assertTrue(DBUtils.queueEntriesCleanedUp(dataSource));
  }

  /** The four per-partition columns survive a registration/read round trip, and clear on update. */
  @Test
  public void testPartitionLimitsRoundTrip() throws Exception {
    String queue = "partition-limits-crud";
    dbos.launch();
    dbos.registerQueue(
        queue,
        QueueOptions.empty()
            .andConcurrency(10)
            .andPartitionConcurrency(4)
            .andPartitionWorkerConcurrency(2)
            .andPartitionRateLimit(3, Duration.ofSeconds(1)));

    var q = dbos.findQueue(queue).orElseThrow();
    assertEquals(10, q.concurrency());
    assertEquals(4, q.partitionConcurrency());
    assertEquals(2, q.partitionWorkerConcurrency());
    assertEquals(3, q.partitionRateLimit().limit());
    assertEquals(Duration.ofSeconds(1), q.partitionRateLimit().period());
    assertTrue(q.hasPartitionLimits());
    assertTrue(q.isPartitioned());

    // A legacy queue still reports its queue-wide limits at the partition scope.
    var limits = q.resolveLimits();
    assertEquals(10, limits.concurrency());
    assertEquals(4, limits.partitionConcurrency());

    var listed =
        dbos.listQueues().stream().filter(x -> x.name().equals(queue)).findFirst().orElseThrow();
    assertEquals(4, listed.partitionConcurrency());
    assertEquals(2, listed.partitionWorkerConcurrency());

    dbos.updateQueue(
        queue,
        QueueOptions.empty()
            .andPartitionConcurrency(null)
            .andPartitionWorkerConcurrency(null)
            .andPartitionRateLimit(null, null));

    var cleared = dbos.findQueue(queue).orElseThrow();
    assertNull(cleared.partitionConcurrency());
    assertNull(cleared.partitionWorkerConcurrency());
    assertNull(cleared.partitionRateLimit());
    assertFalse(cleared.hasPartitionLimits());
    assertFalse(cleared.isPartitioned());
    assertEquals(10, cleared.concurrency());
  }

  /** A legacy partitionQueue queue enforces its queue-wide limits per partition instead. */
  @Test
  public void testLegacyPartitionQueueResolvesLimitsPerPartition() throws Exception {
    String queue = "legacy-partition-queue";
    dbos.launch();
    dbos.registerQueue(
        queue,
        QueueOptions.empty().andConcurrency(3).andWorkerConcurrency(2).andPartitionQueue(true));

    var q = dbos.findQueue(queue).orElseThrow();
    assertTrue(q.isLegacyPartitioned());
    assertTrue(q.isPartitioned());
    assertFalse(q.hasPartitionLimits());

    var limits = q.resolveLimits();
    assertNull(limits.concurrency());
    assertNull(limits.workerConcurrency());
    assertEquals(3, limits.partitionConcurrency());
    assertEquals(2, limits.partitionWorkerConcurrency());
  }

  /**
   * partitionQueue and the per-partition limits are two ways to ask for the same thing, so setting
   * both is redundant rather than contradictory: the flag only ever asked for partitioning, which
   * the limits already say, and its legacy enforcement is defined as the case where no
   * per-partition limit is set.
   */
  @Test
  public void testPartitionQueueAlongsidePartitionLimitsResolves() throws Exception {
    String queue = "redundant-flag-queue";
    dbos.launch();
    dbos.registerQueue(
        queue,
        QueueOptions.empty().andConcurrency(10).andPartitionQueue(true).andPartitionConcurrency(2));

    var q = dbos.findQueue(queue).orElseThrow();
    assertTrue(q.isPartitioned());
    assertTrue(q.hasPartitionLimits());
    // The explicit limits win, so this is not the legacy mode however the flag was set.
    assertFalse(q.isLegacyPartitioned());

    var limits = q.resolveLimits();
    assertEquals(10, limits.concurrency());
    assertEquals(2, limits.partitionConcurrency());
  }

  /**
   * An update is validated against the row it would produce, not on its own. Writing a limit that
   * contradicts the stored ones would leave a row that no longer reads back as a valid queue, which
   * its listener would then fail to reload for as long as the process ran.
   */
  @Test
  public void testUpdateQueueValidatesAgainstStoredLimits() throws Exception {
    String queue = "update-validation-queue";
    dbos.launch();
    dbos.registerQueue(queue, QueueOptions.empty().andConcurrency(2));

    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> dbos.updateQueue(queue, QueueOptions.empty().andPartitionConcurrency(5)));
    assertTrue(
        ex.getMessage().contains("concurrency must be greater than or equal to"), ex.getMessage());

    // The rejected update left nothing behind, so the queue still loads.
    var unchanged = dbos.findQueue(queue).orElseThrow();
    assertEquals(2, unchanged.concurrency());
    assertNull(unchanged.partitionConcurrency());
    assertFalse(unchanged.isPartitioned());
  }

  /**
   * The stored partitionQueue flag is derived, so an update maintains it in both directions. Other
   * SDKs read that column to decide whether to dequeue per partition, and a stale value silently
   * changes the scope every limit on the queue is enforced at.
   */
  @Test
  public void testUpdateQueueKeepsPartitionFlagInSync() throws Exception {
    String queue = "partition-flag-sync-queue";
    dbos.launch();
    dbos.registerQueue(queue, QueueOptions.empty().andConcurrency(10));

    var initial = dbos.findQueue(queue).orElseThrow();
    assertFalse(initial.partitioningEnabled());

    dbos.updateQueue(queue, QueueOptions.empty().andPartitionConcurrency(3));
    var partitioned = dbos.findQueue(queue).orElseThrow();
    assertTrue(partitioned.partitioningEnabled());
    assertTrue(partitioned.isPartitioned());
    assertFalse(partitioned.isLegacyPartitioned());

    dbos.updateQueue(queue, QueueOptions.empty().andPartitionConcurrency(null));
    var cleared = dbos.findQueue(queue).orElseThrow();
    assertFalse(cleared.partitioningEnabled());
    assertFalse(cleared.isPartitioned());
    // The queue-wide limit is enforced queue-wide again, rather than within each partition.
    assertEquals(10, cleared.resolveLimits().concurrency());
  }

  /**
   * A legacy-partitioned queue keeps its meaning across an update, and its limits are frozen: the
   * two partitioning modes disagree about what {@code concurrency} means, so no update may carry a
   * queue between them.
   */
  @Test
  public void aLegacyQueueKeepsItsPartitionedMeaning() throws Exception {
    String queue = "legacy-flag-sync-queue";
    dbos.launch();
    dbos.registerQueue(queue, QueueOptions.empty().andConcurrency(4).andPartitionQueue(true));

    // An update that touches no limit leaves the queue legacy, and its concurrency per partition.
    dbos.updateQueue(queue, QueueOptions.empty().andPollingInterval(Duration.ofSeconds(2)));
    var updated = dbos.findQueue(queue).orElseThrow();
    assertTrue(updated.isLegacyPartitioned());
    assertEquals(4, updated.resolveLimits().partitionConcurrency());
    assertNull(updated.resolveLimits().concurrency());

    // Limits are refused at either scope: migrating off legacy enforcement would rescope every
    // limit the update did not mention, silently. Re-registration is the way across.
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.updateQueue(queue, QueueOptions.empty().andPartitionConcurrency(2)));
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.updateQueue(queue, QueueOptions.empty().andConcurrency(6)));

    var after = dbos.findQueue(queue).orElseThrow();
    assertTrue(after.isLegacyPartitioned(), "neither rejected update may have been written");
    assertEquals(4, after.resolveLimits().partitionConcurrency());
  }

  /**
   * Partitioning an existing queue abandons whatever is already enqueued on it: those rows have no
   * partition key, and a partitioned queue dequeues only from the keys present. The update warns;
   * this pins the behaviour the warning describes, so it cannot change unnoticed.
   */
  @Test
  public void partitioningAQueueStrandsItsKeylessBacklog() throws Exception {
    String queue = "newly-partitioned-queue";
    var impl = new PartitionLimitTestServiceImpl();
    var proxy = dbos.registerProxy(PartitionLimitTestService.class, impl);
    dbos.launch();
    dbos.registerQueue(queue, QueueOptions.empty().andConcurrency(1));

    // Enqueued while the queue is unpartitioned, so it has no partition key.
    var orphan =
        dbos.startWorkflow(
            () -> proxy.blockedWorkflow(), new StartWorkflowOptions().withQueue(queue));
    awaitCount(impl.started, 1, 10_000);
    impl.blockingLatch.countDown();
    assertEquals(orphan.workflowId(), orphan.getResult());

    // Hold the listener off across the enqueue and the update. The latch is already down, so a
    // poll landing in that window would claim this row and run it to completion while the queue
    // is still unpartitioned -- which is not what this test is about, and would fail it.
    var queueService = DBOSTestAccess.getQueueService(dbos);
    queueService.pause();
    // Long enough for a poll already in flight when pause() was set to have finished.
    Thread.sleep(500);

    var stranded =
        dbos.startWorkflow(
            () -> proxy.blockedWorkflow(), new StartWorkflowOptions().withQueue(queue));
    assertEquals(WorkflowState.ENQUEUED, stranded.getStatus().status());

    dbos.updateQueue(queue, QueueOptions.empty().andPartitionConcurrency(1));
    assertTrue(dbos.findQueue(queue).orElseThrow().isPartitioned());

    // getQueuePartitions reads the keys present, and this row has none, so no sweep reaches it.
    // Polling resumes here, against the partitioned queue, so the sweeps that follow are the ones
    // the assertion is about.
    queueService.unpause();
    Thread.sleep(1_000);
    assertEquals(
        WorkflowState.ENQUEUED,
        stranded.getStatus().status(),
        "a keyless row is invisible to a partitioned sweep");
  }

  /** A limit enforced at a narrower scope may never exceed one enforced at a wider scope. */
  @Test
  public void testPartitionLimitValidation() throws Exception {
    dbos.launch();
    record Case(String name, QueueOptions options, String expected) {}
    var cases =
        List.of(
            new Case(
                "v1",
                QueueOptions.empty().andPartitionConcurrency(0),
                "partitionConcurrency must be greater than zero"),
            new Case(
                "v2",
                QueueOptions.empty().andPartitionWorkerConcurrency(0),
                "partitionWorkerConcurrency must be greater than zero"),
            new Case(
                "v3",
                QueueOptions.empty().andPartitionConcurrency(1).andPartitionWorkerConcurrency(2),
                "partitionConcurrency must be greater than or equal to partitionWorkerConcurrency"),
            new Case(
                "v4",
                QueueOptions.empty().andWorkerConcurrency(1).andPartitionWorkerConcurrency(2),
                "workerConcurrency must be greater than or equal to partitionWorkerConcurrency"),
            new Case(
                "v5",
                QueueOptions.empty().andConcurrency(1).andPartitionConcurrency(2),
                "concurrency must be greater than or equal to partitionConcurrency"),
            new Case(
                "v6",
                QueueOptions.empty().andConcurrency(1).andPartitionWorkerConcurrency(2),
                "concurrency must be greater than or equal to partitionWorkerConcurrency"),
            new Case(
                "v7",
                QueueOptions.empty().andPartitionRateLimit(0, Duration.ofSeconds(1)),
                "partitionRateLimit limit must be greater than zero"),
            new Case(
                "v8",
                QueueOptions.empty().andPartitionRateLimit(1, Duration.ZERO),
                "partitionRateLimit period must be greater than zero"));

    for (var c : cases) {
      var ex =
          assertThrows(
              IllegalArgumentException.class,
              () -> dbos.registerQueue(c.name(), c.options()),
              c.expected());
      assertTrue(ex.getMessage().contains(c.expected()), c.expected() + " -> " + ex.getMessage());
    }
  }
}
