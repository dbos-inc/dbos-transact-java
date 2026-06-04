package dev.dbos.transact.queue;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.WorkflowStatusInternalBuilder;
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
  public void testStaticAndDynamicQueueSameName() throws Exception {
    // Static queue registered pre-launch.
    var staticQ = new Queue("q-shared").withConcurrency(3);
    dbos.registerQueue(staticQ);
    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.setSpeedupForTest();

    // Register same name as a dynamic queue — supervisor should ignore the DB entry.
    dbos.registerQueue("q-shared", QueueOptions.setConcurrency(99));

    // Workflow still executes — static listener handles it.
    var handle =
        dbos.startWorkflow(
            () -> serviceQ.simpleQWorkflow("shared"),
            new StartWorkflowOptions().withQueue("q-shared"));
    assertEquals("sharedshared", handle.getResult());
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
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

    var h1 =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(0),
            new StartWorkflowOptions("dyn-wf1").withQueue("dyn-update-q"));
    var h2 =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(1),
            new StartWorkflowOptions("dyn-wf2").withQueue("dyn-update-q"));
    var h3 =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(2),
            new StartWorkflowOptions("dyn-wf3").withQueue("dyn-update-q"));

    // Wait for exactly one workflow to be dequeued and start running.
    impl.wfSemaphore.acquire(1);

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
    dbos.registerQueue(
        "firstQueue",
        QueueOptions.setPriorityEnabled(true).andConcurrency(1).andWorkerConcurrency(1));

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
      double result = h.getResult();
      logger.info(String.valueOf(result));
      times.add(result);
    }

    double waveTolerance = 1.0;
    for (int wave = 0; wave < numWaves; wave++) {
      for (int i = wave * limit; i < (wave + 1) * limit - 1; i++) {
        double diff = times.get(i + 1) - times.get(i);
        logger.info(String.format("Wave %d, Task %d-%d: Time diff %.3f", wave, i, i + 1, diff));
        assertTrue(
            diff < waveTolerance,
            String.format(
                "Wave %d: Tasks %d and %d should start close together. Diff: %.3f",
                wave, i, i + 1, diff));
      }
    }
    logger.info("Verified intra-wave timing.");

    double periodTolerance = 0.5;
    for (int wave = 0; wave < numWaves - 1; wave++) {
      double startOfNextWave = times.get(limit * (wave + 1));
      double startOfCurrentWave = times.get(limit * wave);
      double gap = startOfNextWave - startOfCurrentWave;
      logger.info(String.format("Gap between Wave %d and %d: %.3f", wave, wave + 1, gap));
      assertTrue(
          gap > periodSec - periodTolerance,
          String.format(
              "Gap between wave %d and %d should be at least %.3f. Actual: %.3f",
              wave, wave + 1, periodSec - periodTolerance, gap));
      assertTrue(
          gap < periodSec + periodTolerance,
          String.format(
              "Gap between wave %d and %d should be at most %.3f. Actual: %.3f",
              wave, wave + 1, periodSec + periodTolerance, gap));
    }

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
            .authenticatedRoles(new String[] {"admin", "operator"})
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
      systemDatabase.initWorkflowStatus(status, null, false, false);
    }

    var readBack = systemDatabase.listWorkflows(new ListWorkflowsInput("id0")).get(0);
    assertArrayEquals(new String[] {"admin", "operator"}, readBack.authenticatedRoles());

    List<String> idsToRun =
        systemDatabase.startQueuedWorkflows(qwithWCLimit, executorId, appVersion, null, 0);

    assertEquals(2, idsToRun.size());

    // 2 are now in Pending; pass localRunningCount=2 to simulate in-memory tracking.
    // So no de queueing
    idsToRun = systemDatabase.startQueuedWorkflows(qwithWCLimit, executorId, appVersion, null, 2);
    assertEquals(0, idsToRun.size());

    // mark the first 2 as success
    DBUtils.updateAllWorkflowStates(
        dataSource, WorkflowState.PENDING.name(), WorkflowState.SUCCESS.name());

    // next 2 get dequeued
    idsToRun = systemDatabase.startQueuedWorkflows(qwithWCLimit, executorId, appVersion, null, 0);
    assertEquals(2, idsToRun.size());

    DBUtils.updateAllWorkflowStates(
        dataSource, WorkflowState.PENDING.name(), WorkflowState.SUCCESS.name());
    idsToRun =
        systemDatabase.startQueuedWorkflows(
            qwithWCLimit, Constants.DEFAULT_EXECUTORID, Constants.DEFAULT_APP_VERSION, null, 0);
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
            .authenticatedRoles(new String[] {"admin", "operator"})
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
      systemDatabase.initWorkflowStatus(status, null, false, false);
    }

    // executor2
    String executor2 = "remote";
    for (int i = 2; i < 5; i++) {
      String wfid = "id" + i;
      var status =
          builder.workflowId(wfid).deduplicationId("dedup" + i).executorId(executor2).build();
      systemDatabase.initWorkflowStatus(status, null, false, false);

      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
    }

    List<String> idsToRun =
        systemDatabase.startQueuedWorkflows(qwithWCLimit, executorId, appVersion, null, 0);
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

    var opt1 = new StartWorkflowOptions("wf1").withQueue("test_queue");
    var handle1 = dbos.startWorkflow(() -> service.blockedWorkflow(0), opt1);

    var opt2 = new StartWorkflowOptions("wf2").withQueue("test_queue");
    var handle2 = dbos.startWorkflow(() -> service.blockedWorkflow(1), opt2);

    var opt3 = new StartWorkflowOptions("wf3").withQueue("test_queue");
    var handle3 = dbos.startWorkflow(() -> service.noopWorkflow(2), opt3);

    // each call to blockedWorkflow releases the semaphore once,
    // so block waiting on both calls to release
    impl.wfSemaphore.acquire(2);

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

    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    List<WorkflowHandle<?, ?>> otherHandles = executor.recoverPendingWorkflows(List.of("other"));
    assertEquals(WorkflowState.PENDING, handle1.getStatus().status());
    assertEquals(WorkflowState.PENDING, handle2.getStatus().status());
    assertEquals(1, otherHandles.size());
    assertEquals(otherHandles.get(0).workflowId(), handle3.workflowId());
    assertEquals(WorkflowState.ENQUEUED, handle3.getStatus().status());

    // Pause the listener before recovery so it can't race the ENQUEUED status checks below.
    qs.pause();
    List<WorkflowHandle<?, ?>> localHandles = executor.recoverPendingWorkflows(List.of("local"));
    assertEquals(2, localHandles.size());
    List<String> expectedWorkflowIds = List.of(handle1.workflowId(), handle2.workflowId());
    assertTrue(expectedWorkflowIds.contains(localHandles.get(0).workflowId()));
    assertTrue(expectedWorkflowIds.contains(localHandles.get(1).workflowId()));

    assertEquals(2, impl.counter.get());
    // Recovery sets back to enqueued.
    //   The enqueued run will get skipped (first run is still blocked)
    assertEquals(WorkflowState.ENQUEUED, handle1.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED, handle2.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED, handle3.getStatus().status());

    qs.unpause();
    impl.latch.countDown();
    assertEquals(0, handle1.getResult());
    assertEquals(1, handle2.getResult());
    assertEquals(2, handle3.getResult());
    assertEquals("local", handle3.getStatus().executorId());

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

  private static void awaitCondition(BooleanSupplier condition) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 2000;
    while (!condition.getAsBoolean()) {
      if (System.currentTimeMillis() > deadline)
        throw new AssertionError("Condition not met within 2s");
      Thread.sleep(50);
    }
  }
}
