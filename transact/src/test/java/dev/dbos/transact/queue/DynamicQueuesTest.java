package dev.dbos.transact.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.WorkflowStatusInternalBuilder;
import dev.dbos.transact.workflow.Field;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.QueueConflictResolution;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DynamicQueuesTest {

  private static final Logger logger = LoggerFactory.getLogger(DynamicQueuesTest.class);

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
    dataSource = pgContainer.dataSource();
  }

  @Test
  public void testDynamicQueueWorkflowExecution() throws Exception {
    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();

    // Register a dynamic queue after launch — this writes to DB.
    dbos.registerQueue("dynQueue", QueueOptions.empty());

    // The supervisor polls every 1s; wait for it to discover and start a listener.
    var handle =
        dbos.startWorkflow(
            () -> serviceQ.simpleQWorkflow("hello"),
            new StartWorkflowOptions().withQueue("dynQueue"));

    assertEquals("hellohello", handle.getResult());
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
  }

  @Test
  public void testDynamicQueueConcurrency() throws Exception {
    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();

    dbos.registerQueue("concQ", QueueOptions.setConcurrency(1).andWorkerConcurrency(1));

    for (int i = 0; i < 3; i++) {
      String id = "dynwf" + i;
      String input = "v" + i;
      dbos.startWorkflow(
          () -> serviceQ.simpleQWorkflow(input), new StartWorkflowOptions(id).withQueue("concQ"));
    }

    for (int i = 0; i < 3; i++) {
      var handle = dbos.retrieveWorkflow("dynwf" + i);
      assertEquals("v" + i + "v" + i, handle.getResult());
      assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
    }
  }

  @Test
  public void testListQueues() throws Exception {
    dbos.launch();

    dbos.registerQueue("q-list-1", QueueOptions.setConcurrency(1));
    dbos.registerQueue("q-list-2", QueueOptions.setConcurrency(2));
    dbos.registerQueue("q-list-3", QueueOptions.empty());

    var queues = dbos.listQueues();
    var names = queues.stream().map(Queue::name).toList();
    assertTrue(names.contains("q-list-1"));
    assertTrue(names.contains("q-list-2"));
    assertTrue(names.contains("q-list-3"));
    assertEquals(3, names.size());
  }

  @Test
  public void testDeleteQueue() throws Exception {
    dbos.launch();

    dbos.registerQueue("q-del", QueueOptions.setConcurrency(1));
    assertTrue(dbos.listQueues().stream().anyMatch(q -> q.name().equals("q-del")));

    boolean deleted = dbos.deleteQueue("q-del");
    assertTrue(deleted);
    assertFalse(dbos.listQueues().stream().anyMatch(q -> q.name().equals("q-del")));

    // deleting a non-existent queue returns false
    assertFalse(dbos.deleteQueue("q-never-existed"));
  }

  @Test
  public void aPerPartitionLimitRoundTrips() throws Exception {
    // The refusal 7a shipped is gone: the columns exist now, so the limits are stored and read
    // back rather than dropped.
    dbos.launch();

    dbos.registerQueue(
        "q-pp",
        QueueOptions.setPartitionConcurrency(4)
            .andPartitionWorkerConcurrency(2)
            .andPartitionRateLimit(5, Duration.ofSeconds(30)));

    var q = dbos.findQueue("q-pp").orElseThrow();
    assertEquals(4, q.partitionConcurrency());
    assertEquals(2, q.partitionWorkerConcurrency());
    assertEquals(5, q.partitionRateLimit().limit());
    assertEquals(Duration.ofSeconds(30), q.partitionRateLimit().period());
    assertTrue(q.isPartitioned(), "a per-partition limit partitions the queue");
    assertFalse(q.isLegacyPartitioned());
  }

  @Test
  // Reads the deprecated stored flag on purpose: it is the column under test.
  @SuppressWarnings("removal")
  public void thePartitionQueueColumnFollowsTheLimits() throws Exception {
    // The stored flag is derived on every write, in both directions. Other SDKs read this column
    // to decide whether to dequeue per partition, so a stale value is visible across languages.
    dbos.launch();

    dbos.registerQueue("q-derived", QueueOptions.setConcurrency(4));
    assertFalse(dbos.findQueue("q-derived").orElseThrow().partitioningEnabled());

    dbos.updateQueue("q-derived", QueueOptions.setPartitionConcurrency(2));
    assertTrue(
        dbos.findQueue("q-derived").orElseThrow().partitioningEnabled(),
        "gaining a partition limit sets the flag");

    dbos.updateQueue("q-derived", QueueOptions.setPartitionConcurrency(null));
    assertFalse(
        dbos.findQueue("q-derived").orElseThrow().partitioningEnabled(),
        "losing the last partition limit clears it again");
  }

  @Test
  public void anUpdateIsValidatedAgainstTheRowItWouldProduce() throws Exception {
    // A cross-field rule can only be checked against the values already stored, so the update is
    // applied to the current row and the result is validated before anything is written.
    dbos.launch();

    dbos.registerQueue("q-cross", QueueOptions.setConcurrency(2));

    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.updateQueue("q-cross", QueueOptions.setWorkerConcurrency(5)));

    var unchanged = dbos.findQueue("q-cross").orElseThrow();
    assertEquals(2, unchanged.concurrency());
    assertNull(unchanged.workerConcurrency(), "the rejected update must not have been written");

    // The same field is fine once the row it lands on allows it.
    dbos.updateQueue("q-cross", QueueOptions.setConcurrency(8).andWorkerConcurrency(5));
    var widened = dbos.findQueue("q-cross").orElseThrow();
    assertEquals(8, widened.concurrency());
    assertEquals(5, widened.workerConcurrency());
  }

  @Test
  public void registrationIsValidatedAgainstTheSameRules() throws Exception {
    // The cross-field rule guards both write paths, from call sites a few lines apart in
    // QueuesDAO. anUpdateIsValidatedAgainstTheRowItWouldProduce covers the update; this covers
    // the insert, so dropping either call fails a test rather than only one of them.
    dbos.launch();

    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.registerQueue("q-reg", QueueOptions.setConcurrency(2).andWorkerConcurrency(5)));
    assertTrue(dbos.findQueue("q-reg").isEmpty(), "the rejected queue must not have been written");

    dbos.registerQueue("q-reg", QueueOptions.setConcurrency(5).andWorkerConcurrency(2));
    assertEquals(2, dbos.findQueue("q-reg").orElseThrow().workerConcurrency());
  }

  @Test
  public void aQueueWideRateLimitMustBeWhole() throws Exception {
    // The other half of what the constructor cannot check. A stored row with a zero limit has to
    // stay loadable -- the constructor is the read path -- so the write paths are the only place
    // this rule is ever applied, on both of them.
    dbos.launch();

    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.registerQueue("q-rl", QueueOptions.setRateLimit(0, Duration.ofSeconds(1))));
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.registerQueue("q-rl", QueueOptions.setRateLimit(5, Duration.ZERO)));
    assertTrue(dbos.findQueue("q-rl").isEmpty(), "neither rejected queue may have been written");

    dbos.registerQueue("q-rl", QueueOptions.setRateLimit(5, Duration.ofSeconds(1)));
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.updateQueue("q-rl", QueueOptions.setRateLimit(0, Duration.ofSeconds(1))));
    assertEquals(
        5,
        dbos.findQueue("q-rl").orElseThrow().rateLimit().limit(),
        "the rejected update must not have been written");
  }

  @Test
  public void aRateLimitIsUpdatedAsAPairOrNotAtAll() throws Exception {
    // The UPDATE writes only the columns the caller supplied, so a half-set result cannot be read
    // as no limit: that would validate an unlimited queue and store one column of a limit, and a
    // later update supplying the other half would complete a live limit neither update checked.
    dbos.launch();
    dbos.registerQueue("q-half", QueueOptions.empty().andConcurrency(4));

    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.updateQueue("q-half", QueueOptions.empty().withRateLimitMax(Field.of(0))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            dbos.updateQueue(
                "q-half", QueueOptions.empty().withRateLimitPeriod(Field.of(Duration.ZERO))));
    assertNull(
        dbos.findQueue("q-half").orElseThrow().rateLimit(),
        "no half of a rate limit may have been written");

    // The same for the partition limits, where the derived partitioning flag would have gone out
    // with the half-written column, computed from a limit this saw as absent.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            dbos.updateQueue(
                "q-half", QueueOptions.empty().withPartitionRateLimitMax(Field.of(2))));
    var afterPartitionHalf = dbos.findQueue("q-half").orElseThrow();
    assertNull(afterPartitionHalf.partitionRateLimit());
    assertFalse(
        afterPartitionHalf.isPartitioned(),
        "a refused partition limit may not have partitioned the queue");

    // Both halves together are the supported way in, and one half of an existing limit may still
    // be changed on its own: the other half carries over from the row, so the pair stays whole.
    dbos.updateQueue("q-half", QueueOptions.setRateLimit(5, Duration.ofSeconds(1)));
    dbos.updateQueue("q-half", QueueOptions.empty().withRateLimitMax(Field.of(7)));
    var updated = dbos.findQueue("q-half").orElseThrow();
    assertEquals(7, updated.rateLimit().limit());
    assertEquals(Duration.ofSeconds(1), updated.rateLimit().period());

    // Clearing is a pair too: dropping only the max would leave the period behind in the row.
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.updateQueue("q-half", QueueOptions.empty().withRateLimitMax(Field.of(null))));
    assertEquals(7, dbos.findQueue("q-half").orElseThrow().rateLimit().limit());

    dbos.updateQueue("q-half", QueueOptions.empty().andRateLimit(null, null));
    assertNull(dbos.findQueue("q-half").orElseThrow().rateLimit());
  }

  @Test
  // Sets the deprecated flag on purpose: legacy partitioning is what this covers.
  @SuppressWarnings("removal")
  public void aLegacyPartitionedQueueKeepsItsMeaningAcrossAnUnrelatedUpdate() throws Exception {
    // partition_queue is derived, so a legacy queue reads back with the flag set. Feeding that
    // back in as-is would be indistinguishable from the caller asking for legacy partitioning,
    // which is why applyUpdate carries isLegacyPartitioned() rather than the stored column.
    dbos.launch();

    dbos.registerQueue("q-legacy", QueueOptions.setConcurrency(4).andPartitionQueue(true));
    assertTrue(dbos.findQueue("q-legacy").orElseThrow().isLegacyPartitioned());

    dbos.updateQueue("q-legacy", QueueOptions.setPollingInterval(Duration.ofSeconds(2)));

    var after = dbos.findQueue("q-legacy").orElseThrow();
    assertTrue(after.isLegacyPartitioned(), "still legacy, not promoted by its own stored flag");
    assertEquals(4, after.resolveLimits().partitionConcurrency());
    assertNull(after.resolveLimits().concurrency());
  }

  @Test
  // Sets the deprecated flag on purpose: legacy partitioning is what this covers.
  @SuppressWarnings("removal")
  public void aLegacyPartitionedQueueRefusesLimitUpdates() throws Exception {
    // The two modes disagree about what concurrency means, so an update may not carry a queue
    // between them. Without this, a legacy queue that gained a per-partition limit and then lost
    // it again would come back unpartitioned, and every enqueue with a partition key would fail.
    dbos.launch();

    dbos.registerQueue("q-legacy-lock", QueueOptions.setConcurrency(4).andPartitionQueue(true));

    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.updateQueue("q-legacy-lock", QueueOptions.setPartitionConcurrency(2)));
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.updateQueue("q-legacy-lock", QueueOptions.setConcurrency(9)));

    var after = dbos.findQueue("q-legacy-lock").orElseThrow();
    assertTrue(after.isLegacyPartitioned(), "neither rejected update may have been written");
    assertEquals(4, after.resolveLimits().partitionConcurrency());

    // Only its limits are frozen; everything else still updates.
    dbos.updateQueue("q-legacy-lock", QueueOptions.setPollingInterval(Duration.ofSeconds(2)));
    assertEquals(
        Duration.ofSeconds(2), dbos.findQueue("q-legacy-lock").orElseThrow().pollingInterval());
  }

  @Test
  // Sets the deprecated flag on purpose: refusing to set it is what this covers.
  @SuppressWarnings("removal")
  public void aQueuePartitionedByItsLimitsRefusesTheLegacyFlag() throws Exception {
    // The other direction. The flag is derived, so setting it on a queue that already partitions
    // by its limits could only mean a demotion to legacy enforcement the caller cannot have meant.
    dbos.launch();

    dbos.registerQueue("q-limits", QueueOptions.setPartitionConcurrency(2));

    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.updateQueue("q-limits", QueueOptions.setPartitionQueue(true)));

    var after = dbos.findQueue("q-limits").orElseThrow();
    assertFalse(after.isLegacyPartitioned());
    assertEquals(2, after.resolveLimits().partitionConcurrency());
  }

  @Test
  public void testUpdateQueue() throws Exception {
    dbos.launch();

    dbos.registerQueue("q-update", QueueOptions.setConcurrency(5));

    var before =
        dbos.listQueues().stream()
            .filter(x -> x.name().equals("q-update"))
            .findFirst()
            .orElseThrow();
    assertEquals(5, before.concurrency());

    dbos.updateQueue("q-update", QueueOptions.setConcurrency(10));

    var after =
        dbos.listQueues().stream()
            .filter(x -> x.name().equals("q-update"))
            .findFirst()
            .orElseThrow();
    assertEquals(10, after.concurrency());
  }

  @Test
  public void testRegisterQueueNeverUpdate() throws Exception {
    dbos.launch();

    dbos.registerQueue("q-conflict", QueueOptions.setConcurrency(5));

    // NEVER_UPDATE: second call should not overwrite
    dbos.registerQueue(
        "q-conflict", QueueOptions.setConcurrency(99), QueueConflictResolution.NEVER_UPDATE);

    var q =
        dbos.listQueues().stream()
            .filter(x -> x.name().equals("q-conflict"))
            .findFirst()
            .orElseThrow();
    assertEquals(5, q.concurrency());
  }

  @Test
  public void testRegisterQueueAlwaysUpdate() throws Exception {
    dbos.launch();

    dbos.registerQueue("q-always", QueueOptions.setConcurrency(5));

    // ALWAYS_UPDATE: second call should overwrite
    dbos.registerQueue(
        "q-always", QueueOptions.setConcurrency(99), QueueConflictResolution.ALWAYS_UPDATE);

    var q =
        dbos.listQueues().stream()
            .filter(x -> x.name().equals("q-always"))
            .findFirst()
            .orElseThrow();
    assertEquals(99, q.concurrency());
  }

  @Test
  public void testDynamicQueuePollingInterval() throws Exception {
    dbos.launch();

    var interval = Duration.ofSeconds(3);
    dbos.registerQueue("q-poll", QueueOptions.setPollingInterval(interval));

    var q =
        dbos.listQueues().stream().filter(x -> x.name().equals("q-poll")).findFirst().orElseThrow();
    assertEquals(interval, q.pollingInterval());
  }

  @Test
  public void testRegisterInternalQueueThrows() throws Exception {
    dbos.launch();

    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.registerQueue("_dbos_internal_queue", QueueOptions.empty()));
  }

  @Test
  public void testDeleteAndRecreateQueue() throws Exception {
    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();

    dbos.registerQueue("q-lifecycle", QueueOptions.setConcurrency(5));

    var h1 =
        dbos.startWorkflow(
            () -> serviceQ.simpleQWorkflow("first"),
            new StartWorkflowOptions("lc-wf1").withQueue("q-lifecycle"));
    assertEquals("firstfirst", h1.getResult());

    dbos.deleteQueue("q-lifecycle");
    assertFalse(dbos.listQueues().stream().anyMatch(x -> x.name().equals("q-lifecycle")));

    // Wait for the old listener to detect the deletion and remove itself from the
    // active-listener set. Without this wait the supervisor may not start a fresh
    // listener for the recreated queue (dbListeningQueues still contains the name).
    Thread.sleep(500);

    // Recreate with different config.
    dbos.registerQueue("q-lifecycle", QueueOptions.setConcurrency(2));
    var recreated =
        dbos.listQueues().stream()
            .filter(x -> x.name().equals("q-lifecycle"))
            .findFirst()
            .orElseThrow();
    assertEquals(2, recreated.concurrency());

    var h2 =
        dbos.startWorkflow(
            () -> serviceQ.simpleQWorkflow("second"),
            new StartWorkflowOptions("lc-wf2").withQueue("q-lifecycle"));
    assertEquals("secondsecond", h2.getResult());
  }

  @Test
  public void testDynamicConcurrencyTakesEffect() throws Exception {
    ConcurrencyTestServiceImpl impl = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = dbos.registerProxy(ConcurrencyTestService.class, impl);
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();

    // Start with concurrency=1 so only one workflow dequeues at a time.
    dbos.registerQueue("dyn-update-q", QueueOptions.setConcurrency(1));

    // Enqueue the first workflow and wait until it is running, so it deterministically occupies the
    // single concurrency slot before the others are enqueued. Otherwise any of the three could win
    // the slot and the ENQUEUED assertions below (which name h2 and h3 specifically) would race.
    var h1 =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(0),
            new StartWorkflowOptions("dyn-wf1").withQueue("dyn-update-q"));

    // Wait for exactly one workflow to be dequeued and start running.
    impl.wfSemaphore.acquire(1);

    var h2 =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(1),
            new StartWorkflowOptions("dyn-wf2").withQueue("dyn-update-q"));
    var h3 =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(2),
            new StartWorkflowOptions("dyn-wf3").withQueue("dyn-update-q"));

    // With concurrency=1 the other two should still be waiting.
    Thread.sleep(200);
    assertEquals(WorkflowState.ENQUEUED, h2.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED, h3.getStatus().status());

    // Bump concurrency. The runner reloads queue settings on its next poll and
    // should immediately dequeue the remaining two workflows.
    dbos.updateQueue("dyn-update-q", QueueOptions.setConcurrency(3));
    impl.wfSemaphore.acquire(2);

    // Release all blocked workflows and verify they complete successfully.
    impl.latch.countDown();
    assertEquals(0, h1.getResult());
    assertEquals(1, h2.getResult());
    assertEquals(2, h3.getResult());
  }

  @Test
  public void testDynamicQueueMapUpdatedOnRegister() throws Exception {
    dbos.launch();
    var qs = DBOSTestAccess.getQueueService(dbos);

    assertFalse(qs.findDynamicQueue("q-map-reg").isPresent());

    dbos.registerQueue("q-map-reg", QueueOptions.setConcurrency(5));
    awaitCondition(() -> qs.findDynamicQueue("q-map-reg").isPresent());

    assertEquals(5, qs.findDynamicQueue("q-map-reg").get().concurrency());
  }

  @Test
  public void testDynamicQueueMapUpdatedOnUpdate() throws Exception {
    dbos.launch();
    var qs = DBOSTestAccess.getQueueService(dbos);

    dbos.registerQueue("q-map-upd", QueueOptions.setConcurrency(5));
    awaitCondition(() -> qs.findDynamicQueue("q-map-upd").isPresent());

    dbos.updateQueue("q-map-upd", QueueOptions.setConcurrency(10));
    awaitCondition(
        () ->
            qs.findDynamicQueue("q-map-upd")
                .filter(q -> Integer.valueOf(10).equals(q.concurrency()))
                .isPresent());

    assertEquals(10, qs.findDynamicQueue("q-map-upd").get().concurrency());
  }

  @Test
  public void testDynamicQueueMapUpdatedOnDelete() throws Exception {
    dbos.launch();
    var qs = DBOSTestAccess.getQueueService(dbos);

    dbos.registerQueue("q-map-del", QueueOptions.empty());
    awaitCondition(() -> qs.findDynamicQueue("q-map-del").isPresent());

    dbos.deleteQueue("q-map-del");
    awaitCondition(() -> qs.findDynamicQueue("q-map-del").isEmpty());
  }

  @Test
  public void testRegisterQueueValidation() throws Exception {
    dbos.launch();

    // Zero or negative polling interval should fail.
    assertThrows(
        IllegalArgumentException.class,
        () ->
            dbos.registerQueue(
                "q-bad-poll", QueueOptions.setPollingInterval(Duration.ofSeconds(-1))));
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.registerQueue("q-bad-poll", QueueOptions.setPollingInterval(Duration.ZERO)));

    // Zero or negative concurrency should fail.
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.registerQueue("q-bad-conc", QueueOptions.setConcurrency(0)));
  }

  @Test
  public void testDedupeId() throws Exception {

    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    dbos.registerQueue("firstQueue", QueueOptions.empty());

    // pause queue service for test validation
    qs.pause();

    var options = new StartWorkflowOptions().withQueue("firstQueue");
    var dedupeId = "dedupeId";
    var h1 =
        dbos.startWorkflow(
            () -> serviceQ.simpleQWorkflow("abc"), options.withDeduplicationId(dedupeId));
    var s1 = h1.getStatus();
    assertEquals(s1.queueName(), "firstQueue");
    assertEquals(s1.deduplicationId(), dedupeId);

    // enqueue with different dedupe ID should be fine
    var dedupeId2 = "different-dedupeId";
    var h2 =
        dbos.startWorkflow(
            () -> serviceQ.simpleQWorkflow("def"), options.withDeduplicationId(dedupeId2));
    var s2 = h2.getStatus();
    assertEquals(s2.queueName(), "firstQueue");
    assertEquals(s2.deduplicationId(), dedupeId2);

    // enqueue with no dedupe ID should be fine
    var h3 = dbos.startWorkflow(() -> serviceQ.simpleQWorkflow("ghi"), options);
    var s3 = h3.getStatus();
    assertEquals(s3.queueName(), "firstQueue");
    assertNull(s3.deduplicationId());

    assertThrows(
        RuntimeException.class,
        () ->
            dbos.startWorkflow(
                () -> serviceQ.simpleQWorkflow("jkl"), options.withDeduplicationId(dedupeId)));

    // enable queue service to run
    qs.unpause();

    // wait for initial workflow with initial dedupe ID to finish
    h1.getResult();
    h2.getResult();
    h3.getResult();

    var h4 =
        dbos.startWorkflow(
            () -> serviceQ.simpleQWorkflow("jkl"), options.withDeduplicationId(dedupeId));
    h4.getResult();

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(4, rows.size());

    for (var row : rows) {
      assertEquals(WorkflowState.SUCCESS.name(), row.status());
      assertEquals("firstQueue", row.queueName());
      assertNull(row.deduplicationId());
    }
  }

  @Test
  public void testDedupeIdWithDelay() throws Exception {

    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    dbos.registerQueue("firstQueue", QueueOptions.empty());

    qs.pause();

    var dedupeId = "dedupeId";
    var options = new StartWorkflowOptions().withQueue("firstQueue").withDeduplicationId(dedupeId);
    var h1 =
        dbos.startWorkflow(
            () -> serviceQ.simpleQWorkflow("abc"), options.withDelay(Duration.ofHours(1)));
    var s1 = h1.getStatus();
    assertEquals(WorkflowState.DELAYED, s1.status());
    assertEquals(dedupeId, s1.deduplicationId());

    // Same dedupe ID should conflict even while DELAYED
    assertThrows(
        RuntimeException.class,
        () -> dbos.startWorkflow(() -> serviceQ.simpleQWorkflow("def"), options));

    // Clear the delay and run
    dbos.setWorkflowDelay(h1.workflowId(), Instant.now().minusSeconds(1));
    qs.unpause();
    h1.getResult();

    // After completion the dedupe ID is released — re-enqueue should succeed
    var h2 = dbos.startWorkflow(() -> serviceQ.simpleQWorkflow("ghi"), options);
    h2.getResult();
  }

  @Test
  public void testPriority() throws Exception {

    ServiceQImpl impl = new ServiceQImpl();
    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, impl);
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    dbos.registerQueue("firstQueue", QueueOptions.setConcurrency(1).andWorkerConcurrency(1));

    qs.pause();

    var o1 = new StartWorkflowOptions().withQueue("firstQueue").withPriority(100);
    var h1 = dbos.startWorkflow(() -> serviceQ.priorityWorkflow(100), o1);

    var o2 = new StartWorkflowOptions().withQueue("firstQueue").withPriority(50);
    var h2 = dbos.startWorkflow(() -> serviceQ.priorityWorkflow(50), o2);

    var o3 = new StartWorkflowOptions().withQueue("firstQueue").withPriority(10);
    var h3 = dbos.startWorkflow(() -> serviceQ.priorityWorkflow(10), o3);

    qs.unpause();

    h1.getResult();
    h2.getResult();
    h3.getResult();

    assertEquals(3, impl.queue.size());
    assertEquals(10, impl.queue.remove());
    assertEquals(50, impl.queue.remove());
    assertEquals(100, impl.queue.remove());
  }

  @Test
  public void negativePriorityIsRejected() throws Exception {
    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();
    dbos.registerQueue("prioQueue", QueueOptions.empty());

    // 0 is the default, so a negative priority would jump ahead of every workflow that set none.
    // The options refuse it as they are built, before anything can be enqueued.
    assertThrows(
        IllegalArgumentException.class,
        () -> new StartWorkflowOptions("wf-negative").withQueue("prioQueue").withPriority(-1));

    var zero = new StartWorkflowOptions("wf-zero").withQueue("prioQueue").withPriority(0);
    dbos.startWorkflow(() -> serviceQ.priorityWorkflow(0), zero).getResult();
  }

  @Test
  public void testQueuedMultipleWorkflows() throws Exception {

    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    dbos.registerQueue("firstQueue", QueueOptions.setConcurrency(1).andWorkerConcurrency(1));

    qs.pause();
    Thread.sleep(2000);

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;
      var input = "inputq" + i;
      dbos.startWorkflow(
          () -> serviceQ.simpleQWorkflow(input),
          new StartWorkflowOptions(id).withQueue("firstQueue"));
    }

    var input = new ListWorkflowsInput().withQueuesOnly(true).withLoadInput(true);
    List<WorkflowStatus> wfs = dbos.listWorkflows(input);

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;

      assertEquals(id, wfs.get(i).workflowId());
      assertEquals(WorkflowState.ENQUEUED, wfs.get(i).status());
    }

    qs.unpause();

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;

      var handle = dbos.retrieveWorkflow(id);
      assertEquals(id, handle.workflowId());
      String result = (String) handle.getResult();
      assertEquals("inputq" + i + "inputq" + i, result);
      assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
    }
  }

  @Test
  void testListQueuedWorkflow() throws Exception {

    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    dbos.registerQueue("firstQueue", QueueOptions.setConcurrency(1).andWorkerConcurrency(1));

    qs.pause();

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;
      var input = "inputq" + i;
      dbos.startWorkflow(
          () -> serviceQ.simpleQWorkflow(input),
          new StartWorkflowOptions(id).withQueue("firstQueue"));
      Thread.sleep(100);
    }

    var input = new ListWorkflowsInput().withQueuesOnly(true).withLoadInput(true);
    List<WorkflowStatus> wfs = dbos.listWorkflows(input);
    wfs.sort(
        (a, b) -> {
          return a.workflowId().compareTo(b.workflowId());
        });

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;

      assertEquals(id, wfs.get(i).workflowId());
      assertEquals(WorkflowState.ENQUEUED, wfs.get(i).status());
    }

    wfs = dbos.listWorkflows(input.withQueueName("abc"));
    assertEquals(0, wfs.size());

    wfs = dbos.listWorkflows(input.withQueueName("firstQueue"));
    assertEquals(5, wfs.size());

    wfs = dbos.listWorkflows(input.withEndTime(Instant.now().minus(10, ChronoUnit.SECONDS)));
    assertEquals(0, wfs.size());
  }

  @Test
  public void multipleQueues() throws Exception {

    ServiceQ serviceQ1 = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    ServiceI serviceI = dbos.registerProxy(ServiceI.class, new ServiceIImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    dbos.registerQueue("firstQueue", QueueOptions.setConcurrency(1).andWorkerConcurrency(1));
    dbos.registerQueue("secondQueue", QueueOptions.setConcurrency(1).andWorkerConcurrency(1));

    String id1 = "firstQ1234";
    String id2 = "second1234";

    var options1 = new StartWorkflowOptions(id1).withQueue("firstQueue");
    WorkflowHandle<String, ?> handle1 =
        dbos.startWorkflow(() -> serviceQ1.simpleQWorkflow("firstinput"), options1);

    var options2 = new StartWorkflowOptions(id2).withQueue("secondQueue");
    WorkflowHandle<Integer, ?> handle2 = dbos.startWorkflow(() -> serviceI.workflowI(25), options2);

    assertEquals(id1, handle1.workflowId());
    String result = handle1.getResult();
    assertEquals("firstQueue", handle1.getStatus().queueName());
    assertEquals("firstinputfirstinput", result);
    assertEquals(WorkflowState.SUCCESS, handle1.getStatus().status());

    assertEquals(id2, handle2.workflowId());
    Integer result2 = (Integer) handle2.getResult();
    assertEquals("secondQueue", handle2.getStatus().queueName());
    assertEquals(50, result2);
    assertEquals(WorkflowState.SUCCESS, handle2.getStatus().status());
  }

  @Test
  public void testLimiter() throws Exception {

    int limit = 5;
    double periodSec = 1.8;
    Duration period = Duration.ofMillis((long) (periodSec * 1000));

    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    dbos.registerQueue(
        "limitQueue",
        QueueOptions.setRateLimit(limit, period).andConcurrency(1).andWorkerConcurrency(1));
    Thread.sleep(1000);

    int numWaves = 3;
    int numTasks = numWaves * limit;
    List<WorkflowHandle<Double, ?>> handles = new ArrayList<>();
    List<Double> times = new ArrayList<>();

    for (int i = 0; i < numTasks; i++) {
      String id = "id" + i;
      var options = new StartWorkflowOptions(id).withQueue("limitQueue");
      WorkflowHandle<Double, ?> handle =
          dbos.startWorkflow(() -> serviceQ.limitWorkflow("abc", "123"), options);
      handles.add(handle);
    }

    for (WorkflowHandle<Double, ?> h : handles) {
      h.getResult();
      Long startedAt = h.getStatus().startedAtEpochMs();
      assertNotNull(startedAt, "workflow " + h.workflowId() + " has no start time");
      times.add(startedAt / 1000.0);
    }

    double periodTolerance = 0.5;

    // The limiter is a sliding window: it refuses to dequeue while `limit` workflows on the queue
    // have started within the last period. Task i and task i + limit are therefore at least a
    // period apart, because otherwise limit + 1 starts would fit in one window. This is a lower
    // bound, so a slow database pushes the gap away from the boundary rather than into it —
    // unlike an upper bound on how close consecutive tasks start, which measures the per-workflow
    // round trip rather than the limiter, and which CockroachDB was slow enough to miss.
    for (int i = 0; i + limit < numTasks; i++) {
      double diff = times.get(i + limit) - times.get(i);
      logger.info(String.format("Tasks %d and %d: Time diff %.3f", i, i + limit, diff));
      assertTrue(
          diff > periodSec - periodTolerance,
          String.format(
              "Only %d tasks may start per %.3fs, so tasks %d and %d should start at least %.3fs"
                  + " apart. Actual: %.3f",
              limit, periodSec, i, i + limit, periodSec - periodTolerance, diff));
    }
    logger.info("Verified rate limit spacing.");

    // ... and the limiter must not throttle harder than it is configured to. Each wave after the
    // first costs a period, so the starts span numWaves - 1 periods, plus however long it takes
    // to dispatch and run the tasks within a wave. Two further periods of allowance for that
    // still catches a limiter releasing half its configured rate.
    double maxSpan = (numWaves + 1) * periodSec;
    double span = times.get(numTasks - 1) - times.get(0);
    logger.info(String.format("Span of all %d starts: %.3f", numTasks, span));
    assertTrue(
        span < maxSpan,
        String.format(
            "%d tasks at %d per %.3fs should all start within %.3fs. Actual: %.3f",
            numTasks, limit, periodSec, maxSpan, span));
    for (WorkflowHandle<Double, ?> h : handles) {
      assertEquals(WorkflowState.SUCCESS, h.getStatus().status());
    }
  }

  @Test
  public void testWorkerConcurrency() throws Exception {

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
    var queueService = DBOSTestAccess.getQueueService(dbos);

    dbos.registerQueue("QwithWCLimit", QueueOptions.setConcurrency(3).andWorkerConcurrency(2));
    Queue qwithWCLimit = dbos.findQueue("QwithWCLimit").get();

    String executorId = dbosExecutor.executorId();
    String appVersion = dbosExecutor.appVersion();

    queueService.close();
    while (!queueService.isStopped()) {
      Thread.sleep(2000);
      logger.info("Waiting for queueService to stop");
    }

    var serArgs = SerializationUtil.serializeValue(new Object[] {"ORD-12345"}, null, null);
    var builder =
        new WorkflowStatusInternalBuilder()
            .workflowName("OrderProcessingWorkflow")
            .className("com.example.workflows.OrderWorkflow")
            .instanceName("prod-config")
            .authenticatedUser("user123@example.com")
            .assumedRole("admin")
            .authenticatedRoles("admin", "operator")
            .queueName("QwithWCLimit")
            .executorId(executorId)
            .appVersion(appVersion)
            .appId("order-app-123")
            .timeout(Duration.ofMillis(300000))
            .deadline(Instant.ofEpochMilli(System.currentTimeMillis() + 2400000))
            .priority(1)
            .inputs(serArgs.serializedValue());

    for (int i = 0; i < 4; i++) {
      String wfid = "id" + i;
      var status = builder.workflowId(wfid).deduplicationId("dedup" + i).build();
      systemDatabase.initWorkflowStatus(status, null);
    }

    var readBack = systemDatabase.listWorkflows(new ListWorkflowsInput("id0")).get(0);
    assertEquals(List.of("admin", "operator"), readBack.authenticatedRoles());

    List<String> idsToRun =
        systemDatabase.startQueuedWorkflows(qwithWCLimit, executorId, appVersion, null, 0, 0);

    assertEquals(2, idsToRun.size());

    // 2 are now in Pending; pass localRunningCount=2 to simulate in-memory tracking.
    // So no de queueing
    idsToRun =
        systemDatabase.startQueuedWorkflows(qwithWCLimit, executorId, appVersion, null, 2, 2);
    assertEquals(0, idsToRun.size());

    // mark the first 2 as success
    DBUtils.updateAllWorkflowStates(
        dataSource, WorkflowState.PENDING.name(), WorkflowState.SUCCESS.name());

    // next 2 get dequeued
    idsToRun =
        systemDatabase.startQueuedWorkflows(qwithWCLimit, executorId, appVersion, null, 0, 0);
    assertEquals(2, idsToRun.size());

    DBUtils.updateAllWorkflowStates(
        dataSource, WorkflowState.PENDING.name(), WorkflowState.SUCCESS.name());
    idsToRun =
        systemDatabase.startQueuedWorkflows(
            qwithWCLimit, Constants.DEFAULT_EXECUTORID, Constants.DEFAULT_APP_VERSION, null, 0, 0);
    assertEquals(0, idsToRun.size());
  }

  @Test
  public void testGlobalConcurrency() throws Exception {

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
    var queueService = DBOSTestAccess.getQueueService(dbos);

    dbos.registerQueue("QwithWCLimit", QueueOptions.setConcurrency(3).andWorkerConcurrency(2));
    Queue qwithWCLimit = dbos.findQueue("QwithWCLimit").get();

    String executorId = dbosExecutor.executorId();
    String appVersion = dbosExecutor.appVersion();

    queueService.close();
    while (!queueService.isStopped()) {
      Thread.sleep(2000);
      logger.info("Waiting for queueService to stop");
    }

    var builder =
        new WorkflowStatusInternalBuilder()
            .workflowName("OrderProcessingWorkflow")
            .className("com.example.workflows.OrderWorkflow")
            .instanceName("prod-config")
            .authenticatedUser("user123@example.com")
            .assumedRole("admin")
            .authenticatedRoles("admin", "operator")
            .queueName("QwithWCLimit")
            .executorId(executorId)
            .appVersion(appVersion)
            .appId("order-app-123")
            .timeout(Duration.ofMillis(300000))
            .deadline(Instant.ofEpochMilli(System.currentTimeMillis() + 2400000))
            .priority(1)
            .inputs("{\"orderId\":\"ORD-12345\"}");

    // executor1
    for (int i = 0; i < 2; i++) {
      String wfid = "id" + i;
      var status = builder.workflowId(wfid).deduplicationId("dedup" + i).build();
      systemDatabase.initWorkflowStatus(status, null);
    }

    // executor2
    String executor2 = "remote";
    for (int i = 2; i < 5; i++) {
      String wfid = "id" + i;
      var status =
          builder.workflowId(wfid).deduplicationId("dedup" + i).executorId(executor2).build();
      systemDatabase.initWorkflowStatus(status, null);

      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
    }

    List<String> idsToRun =
        systemDatabase.startQueuedWorkflows(qwithWCLimit, executorId, appVersion, null, 0, 0);
    // 0 because global concurrency limit is reached
    assertEquals(0, idsToRun.size());

    DBUtils.updateAllWorkflowStates(
        dataSource, WorkflowState.PENDING.name(), WorkflowState.SUCCESS.name());
    idsToRun =
        systemDatabase.startQueuedWorkflows(
            qwithWCLimit,
            // executorId,
            executor2,
            appVersion,
            null,
            0,
            0);
    assertEquals(2, idsToRun.size());
  }

  @Test
  public void testenQueueWF() throws Exception {

    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    dbos.registerQueue("firstQueue", QueueOptions.empty());

    String id = "q1234";

    var option = new StartWorkflowOptions(id).withQueue("firstQueue");
    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(() -> serviceQ.simpleQWorkflow("inputq"), option);

    assertEquals(id, handle.workflowId());
    String result = handle.getResult();
    assertEquals("inputqinputq", result);
  }

  @Test
  public void testQueueConcurrencyUnderRecovery() throws Exception {
    ConcurrencyTestServiceImpl impl = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = dbos.registerProxy(ConcurrencyTestService.class, impl);
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    dbos.registerQueue("test_queue", QueueOptions.setConcurrency(2));

    // Enqueue the two blocking workflows first and wait until both are running, so they
    // deterministically occupy the two concurrency slots before the noop workflow is enqueued.
    // Otherwise the noop could be dispatched into a slot first, run to completion, and the ENQUEUED
    // assertion below would race.
    var opt1 = new StartWorkflowOptions("wf1").withQueue("test_queue");
    var handle1 = dbos.startWorkflow(() -> service.blockedWorkflow(0), opt1);

    var opt2 = new StartWorkflowOptions("wf2").withQueue("test_queue");
    var handle2 = dbos.startWorkflow(() -> service.blockedWorkflow(1), opt2);

    // each call to blockedWorkflow releases the semaphore once,
    // so block waiting on both calls to release
    impl.wfSemaphore.acquire(2);

    var opt3 = new StartWorkflowOptions("wf3").withQueue("test_queue");
    var handle3 = dbos.startWorkflow(() -> service.noopWorkflow(2), opt3);

    Thread.sleep(200);
    assertEquals(2, impl.counter.get());
    assertEquals(WorkflowState.PENDING, handle1.getStatus().status());
    assertEquals(WorkflowState.PENDING, handle2.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED, handle3.getStatus().status());

    // update WF3 to appear as if it's from a different executor
    String sql =
        "UPDATE dbos.workflow_status SET status = ?, executor_id = ? where workflow_uuid = ?;";

    try (Connection connection = DBUtils.getConnection(dbosConfig);
        PreparedStatement pstmt = connection.prepareStatement(sql)) {

      pstmt.setString(1, WorkflowState.PENDING.name());
      pstmt.setString(2, "other");
      pstmt.setString(3, opt3.workflowId());

      // Execute the update and get the number of rows affected
      int rowsAffected = pstmt.executeUpdate();
      assertEquals(1, rowsAffected);
    }

    // Pause the listener before recovery so it can't race the ENQUEUED status checks below.
    qs.pause();

    // Recovering the executor wf3 now claims to belong to returns it to the queue, and leaves the
    // two workflows this executor is still running alone -- they are PENDING under "local", which
    // this sweep does not name, so they keep the two concurrency slots they are actually using.
    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    List<String> recovered = executor.recoverPendingWorkflows(List.of("other"));
    assertEquals(List.of(handle3.workflowId()), recovered);

    assertEquals(2, impl.counter.get());
    assertEquals(WorkflowState.PENDING, handle1.getStatus().status());
    assertEquals(WorkflowState.PENDING, handle2.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED, handle3.getStatus().status());

    qs.unpause();
    impl.latch.countDown();
    assertEquals(0, handle1.getResult());
    assertEquals(1, handle2.getResult());
    assertEquals(2, handle3.getResult());
    assertEquals("local", handle3.getStatus().executorId());
    // Only noopWorkflow leaves the counter alone, so this still being 2 means neither blocked
    // workflow was executed a second time by the recovery above.
    assertEquals(2, impl.counter.get());

    assertTrue(DBUtils.queueEntriesAreCleanedUp(dataSource));
  }

  @Test
  public void testListenQueue() throws Exception {
    var config = dbosConfig.withListenQueue("queueOne");
    try (var dbos = new DBOS(config)) {

      ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
      dbos.launch();

      var qs = DBOSTestAccess.getQueueService(dbos);
      qs.setSpeedupForTest();
      dbos.registerQueue("queueOne", QueueOptions.empty());
      dbos.registerQueue("queueTwo", QueueOptions.empty());

      var h2 =
          dbos.startWorkflow(
              () -> serviceQ.simpleQWorkflow("two"),
              new StartWorkflowOptions().withQueue("queueTwo"));
      var h1 =
          dbos.startWorkflow(
              () -> serviceQ.simpleQWorkflow("one"),
              new StartWorkflowOptions().withQueue("queueOne"));

      Thread.sleep(3000);
      assertEquals("oneone", h1.getResult());
      assertEquals(WorkflowState.ENQUEUED, h2.getStatus().status());
    }
  }

  @Test
  public void testCancellingQueuedWorkflows() throws Exception {
    ConcurrencyTestServiceImpl impl = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = dbos.registerProxy(ConcurrencyTestService.class, impl);
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    dbos.registerQueue("test_queue", QueueOptions.setConcurrency(1));

    // Enqueue the blocking workflow first and confirm it is running so it holds the single slot
    // before the regular workflow is enqueued; otherwise the regular one could be dispatched first.
    var blockedHandle =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(0),
            new StartWorkflowOptions("blocked").withQueue("test_queue"));
    impl.wfSemaphore.acquire(1);

    var regularHandle =
        dbos.startWorkflow(
            () -> service.noopWorkflow(42), new StartWorkflowOptions().withQueue("test_queue"));

    Thread.sleep(200);
    assertEquals(WorkflowState.PENDING, blockedHandle.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED, regularHandle.getStatus().status());

    dbos.cancelWorkflow("blocked");
    assertEquals(WorkflowState.CANCELLED, blockedHandle.getStatus().status());
    assertEquals(42, (int) regularHandle.getResult());

    impl.latch.countDown();
    assertTrue(DBUtils.queueEntriesAreCleanedUp(dataSource));
  }

  @Test
  public void testResumingQueuedWorkflows() throws Exception {
    ConcurrencyTestServiceImpl impl = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = dbos.registerProxy(ConcurrencyTestService.class, impl);
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    dbos.registerQueue("test_queue", QueueOptions.setConcurrency(1));

    // Enqueue the blocking workflow first and confirm it is running so it holds the single slot
    // before the regular workflow is enqueued; otherwise the regular one could be dispatched first.
    var blockedHandle =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(0), new StartWorkflowOptions().withQueue("test_queue"));
    impl.wfSemaphore.acquire(1);

    var regularHandle =
        dbos.startWorkflow(
            () -> service.noopWorkflow(99),
            new StartWorkflowOptions("resumable").withQueue("test_queue"));

    Thread.sleep(200);
    assertEquals(WorkflowState.ENQUEUED, regularHandle.getStatus().status());

    var resumedHandle = dbos.<Integer, Exception>resumeWorkflow("resumable");
    assertEquals(99, (int) resumedHandle.getResult());

    impl.latch.countDown();
    blockedHandle.getResult();
    assertTrue(DBUtils.queueEntriesAreCleanedUp(dataSource));
  }

  @Test
  public void testOneAtATimeWithWorkerConcurrency() throws Exception {
    ConcurrencyTestServiceImpl impl = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = dbos.registerProxy(ConcurrencyTestService.class, impl);
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    dbos.registerQueue("test_queue", QueueOptions.setWorkerConcurrency(1));

    // Enqueue the blocking workflow first and wait until it is actually running, so it
    // deterministically occupies the single worker slot before h2 exists. Otherwise the dispatcher
    // could pick up h2 first, run it to completion, and the ENQUEUED assertion below would race.
    var h1 =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(0), new StartWorkflowOptions().withQueue("test_queue"));
    impl.wfSemaphore.acquire(1);

    var h2 =
        dbos.startWorkflow(
            () -> service.noopWorkflow(1), new StartWorkflowOptions().withQueue("test_queue"));

    Thread.sleep(2000);
    assertEquals(WorkflowState.ENQUEUED, h2.getStatus().status());
    assertEquals(1, impl.counter.get());

    impl.latch.countDown();
    assertEquals(0, h1.getResult());
    assertEquals(1, h2.getResult());
    assertEquals(1, impl.counter.get());
    assertTrue(DBUtils.queueEntriesAreCleanedUp(dataSource));
  }

  @Test
  public void testTimeoutQueue() throws Exception {
    ConcurrencyTestServiceImpl impl = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = dbos.registerProxy(ConcurrencyTestService.class, impl);
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    dbos.registerQueue("test_queue", QueueOptions.setConcurrency(1));

    var h1 =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(0),
            new StartWorkflowOptions()
                .withQueue("test_queue")
                .withTimeout(500, TimeUnit.MILLISECONDS));
    var h2 =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(1),
            new StartWorkflowOptions()
                .withQueue("test_queue")
                .withTimeout(500, TimeUnit.MILLISECONDS));
    var normalHandle =
        dbos.startWorkflow(
            () -> service.noopWorkflow(42),
            new StartWorkflowOptions().withQueue("test_queue").withTimeout(10, TimeUnit.SECONDS));

    assertThrows(Exception.class, h1::getResult);
    assertThrows(Exception.class, h2::getResult);
    assertEquals(42, (int) normalHandle.getResult());
    assertTrue(DBUtils.queueEntriesAreCleanedUp(dataSource));
  }

  @Test
  public void testMultipleExecutorWorkerConcurrency() throws Exception {
    int workerConcurrency = 2;
    int globalConcurrency = workerConcurrency * 2;

    ConcurrencyTestServiceImpl impl1 = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = dbos.registerProxy(ConcurrencyTestService.class, impl1);
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    dbos.registerQueue(
        "multi_exec_queue",
        QueueOptions.setWorkerConcurrency(workerConcurrency).andConcurrency(globalConcurrency));

    for (int i = 0; i < workerConcurrency; i++) {
      final int fi = i;
      dbos.startWorkflow(
          () -> service.blockedWorkflow(fi),
          new StartWorkflowOptions().withQueue("multi_exec_queue"));
    }
    impl1.wfSemaphore.acquire(workerConcurrency);

    ConcurrencyTestServiceImpl impl2 = new ConcurrencyTestServiceImpl();
    try (var dbos2 = new DBOS(dbosConfig.withExecutorId("executor2"))) {
      ConcurrencyTestService service2 = dbos2.registerProxy(ConcurrencyTestService.class, impl2);
      dbos2.launch();

      var qs2 = DBOSTestAccess.getQueueService(dbos2);
      qs2.setSpeedupForTest();
      dbos2.registerQueue(
          "multi_exec_queue",
          QueueOptions.setWorkerConcurrency(workerConcurrency).andConcurrency(globalConcurrency));

      List<WorkflowHandle<Integer, ?>> handles2 = new ArrayList<>();
      for (int i = 0; i < workerConcurrency; i++) {
        final int fi = i;
        handles2.add(
            dbos2.startWorkflow(
                () -> service2.blockedWorkflow(fi),
                new StartWorkflowOptions().withQueue("multi_exec_queue")));
      }
      impl2.wfSemaphore.acquire(workerConcurrency);
      for (var h : handles2) {
        assertEquals("executor2", h.getStatus().executorId());
      }

      var extra =
          dbos2.startWorkflow(
              () -> service2.noopWorkflow(99),
              new StartWorkflowOptions().withQueue("multi_exec_queue"));
      Thread.sleep(2000);
      assertEquals(WorkflowState.ENQUEUED, extra.getStatus().status());

      impl2.latch.countDown();
      for (var h : handles2) {
        h.getResult();
      }
      assertEquals(99, (int) extra.getResult());
    }

    impl1.latch.countDown();
    assertTrue(DBUtils.queueEntriesAreCleanedUp(dataSource));
  }

  @Test
  public void testQueueChildWorkflow() throws Exception {
    // A workflow can enqueue child workflows on a queue and await their results.
    // With concurrency=3 and 4 children, the 4th waits for a slot.
    QueueChildServiceImpl impl = new QueueChildServiceImpl(dbos);
    QueueChildService service = dbos.registerProxy(QueueChildService.class, impl);
    impl.setSelf(service);
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    dbos.registerQueue("child_queue", QueueOptions.setConcurrency(3));

    var handle =
        dbos.startWorkflow(() -> service.parentWorkflow("a", "b"), new StartWorkflowOptions());
    assertEquals("adbdadbd", handle.getResult());
    assertTrue(DBUtils.queueEntriesAreCleanedUp(dataSource));
  }

  @Test
  public void testQueueDeduplication() throws Exception {
    ConcurrencyTestServiceImpl impl = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = dbos.registerProxy(ConcurrencyTestService.class, impl);
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();
    qs.pause();
    dbos.registerQueue("test_queue", QueueOptions.empty());

    String deduplicationId = "my-dedup-id";
    String wfid1 = "wf-dedup-1";

    // Enqueue with a deduplication ID.
    var h1 =
        dbos.startWorkflow(
            () -> service.noopWorkflow(1),
            new StartWorkflowOptions(wfid1)
                .withQueue("test_queue")
                .withDeduplicationId(deduplicationId));
    assertEquals(deduplicationId, h1.getStatus().deduplicationId());
    assertEquals(WorkflowState.ENQUEUED, h1.getStatus().status());

    // Same dedup ID with a different workflow ID → exception.
    assertThrows(
        DBOSQueueDuplicatedException.class,
        () ->
            dbos.startWorkflow(
                () -> service.noopWorkflow(2),
                new StartWorkflowOptions()
                    .withQueue("test_queue")
                    .withDeduplicationId(deduplicationId)));

    // Re-enqueue the same workflow ID → idempotent, no exception.
    var h1again =
        dbos.startWorkflow(
            () -> service.noopWorkflow(1),
            new StartWorkflowOptions(wfid1)
                .withQueue("test_queue")
                .withDeduplicationId(deduplicationId));
    assertEquals(wfid1, h1again.workflowId());

    // Different dedup ID is fine.
    var h2 =
        dbos.startWorkflow(
            () -> service.noopWorkflow(3),
            new StartWorkflowOptions()
                .withQueue("test_queue")
                .withDeduplicationId("other-dedup-id"));

    // Unpause and let both run.
    qs.unpause();
    assertEquals(1, (int) h1.getResult());
    assertEquals(3, (int) h2.getResult());

    // After completion the same dedup ID can be reused.
    var h3 =
        dbos.startWorkflow(
            () -> service.noopWorkflow(4),
            new StartWorkflowOptions()
                .withQueue("test_queue")
                .withDeduplicationId(deduplicationId));
    assertEquals(4, (int) h3.getResult());

    assertTrue(DBUtils.queueEntriesAreCleanedUp(dataSource));
  }

  private static void awaitCondition(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 2000;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline)
        throw new AssertionError("Condition not met within 2s");
      Thread.sleep(50);
    }
  }
}
