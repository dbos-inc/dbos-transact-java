package dev.dbos.transact.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests for the static (in-memory) queue registration path. All queues here are registered via
 * {@code dbos.registerQueue(Queue)} before launch, which exercises the pre-launch static listener
 * code path. See {@link QueuesTest} for the equivalent tests using database-backed dynamic queues.
 */
public class StaticQueuesTest {

  private static final Logger logger = LoggerFactory.getLogger(StaticQueuesTest.class);

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
  public void testQueuedWorkflow() throws Exception {

    Queue firstQ = new Queue("firstQueue").withConcurrency(1).withWorkerConcurrency(1);
    dbos.registerQueue(firstQ);

    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    String id = "q1234";
    dbos.startWorkflow(
        () -> serviceQ.simpleQWorkflow("inputq"), new StartWorkflowOptions(id).withQueue(firstQ));

    var handle = dbos.retrieveWorkflow(id);
    assertEquals(id, handle.workflowId());
    String result = (String) handle.getResult();
    assertEquals("inputqinputq", result);
  }

  @Test
  public void testDedupeId() throws Exception {

    Queue firstQ = new Queue("firstQueue");
    dbos.registerQueue(firstQ);

    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    // pause queue service for test validation
    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    var options = new StartWorkflowOptions().withQueue(firstQ);
    var dedupeId = "dedupeId";
    var h1 =
        dbos.startWorkflow(
            () -> serviceQ.simpleQWorkflow("abc"), options.withDeduplicationId(dedupeId));
    var s1 = h1.getStatus();
    assertEquals(s1.queueName(), firstQ.name());
    assertEquals(s1.deduplicationId(), dedupeId);

    // enqueue with different dedupe ID should be fine
    var dedupeId2 = "different-dedupeId";
    var h2 =
        dbos.startWorkflow(
            () -> serviceQ.simpleQWorkflow("def"), options.withDeduplicationId(dedupeId2));
    var s2 = h2.getStatus();
    assertEquals(s2.queueName(), firstQ.name());
    assertEquals(s2.deduplicationId(), dedupeId2);

    // enqueue with no dedupe ID should be fine
    var h3 = dbos.startWorkflow(() -> serviceQ.simpleQWorkflow("ghi"), options);
    var s3 = h3.getStatus();
    assertEquals(s3.queueName(), firstQ.name());
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

    Queue firstQ = new Queue("firstQueue");
    dbos.registerQueue(firstQ);

    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    var dedupeId = "dedupeId";
    var options = new StartWorkflowOptions().withQueue(firstQ).withDeduplicationId(dedupeId);
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

    Queue firstQ =
        new Queue("firstQueue")
            .withPriorityEnabled(true)
            .withConcurrency(1)
            .withWorkerConcurrency(1);
    dbos.registerQueue(firstQ);

    ServiceQImpl impl = new ServiceQImpl();
    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, impl);

    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    var o1 = new StartWorkflowOptions().withQueue(firstQ).withPriority(100);
    var h1 = dbos.startWorkflow(() -> serviceQ.priorityWorkflow(100), o1);

    var o2 = new StartWorkflowOptions().withQueue(firstQ).withPriority(50);
    var h2 = dbos.startWorkflow(() -> serviceQ.priorityWorkflow(50), o2);

    var o3 = new StartWorkflowOptions().withQueue(firstQ).withPriority(10);
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

    Queue firstQ = new Queue("firstQueue").withConcurrency(1).withWorkerConcurrency(1);
    dbos.registerQueue(firstQ);
    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());

    dbos.launch();

    var queueService = DBOSTestAccess.getQueueService(dbos);
    queueService.pause();
    Thread.sleep(2000);

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;
      var input = "inputq" + i;
      dbos.startWorkflow(
          () -> serviceQ.simpleQWorkflow(input), new StartWorkflowOptions(id).withQueue(firstQ));
    }

    var input = new ListWorkflowsInput().withQueuesOnly(true).withLoadInput(true);
    List<WorkflowStatus> wfs = dbos.listWorkflows(input);

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;

      assertEquals(id, wfs.get(i).workflowId());
      assertEquals(WorkflowState.ENQUEUED, wfs.get(i).status());
    }

    queueService.unpause();

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

    Queue firstQ = new Queue("firstQueue").withConcurrency(1).withWorkerConcurrency(1);
    dbos.registerQueue(firstQ);
    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());

    dbos.launch();
    var queueService = DBOSTestAccess.getQueueService(dbos);

    queueService.pause();

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;
      var input = "inputq" + i;
      dbos.startWorkflow(
          () -> serviceQ.simpleQWorkflow(input), new StartWorkflowOptions(id).withQueue(firstQ));
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

    Queue firstQ = new Queue("firstQueue").withConcurrency(1).withWorkerConcurrency(1);
    Queue secondQ = new Queue("secondQueue").withConcurrency(1).withWorkerConcurrency(1);
    dbos.registerQueues(firstQ, secondQ);
    ServiceQ serviceQ1 = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
    ServiceI serviceI = dbos.registerProxy(ServiceI.class, new ServiceIImpl());

    dbos.launch();

    String id1 = "firstQ1234";
    String id2 = "second1234";

    var options1 = new StartWorkflowOptions(id1).withQueue(firstQ);
    WorkflowHandle<String, ?> handle1 =
        dbos.startWorkflow(() -> serviceQ1.simpleQWorkflow("firstinput"), options1);

    var options2 = new StartWorkflowOptions(id2).withQueue(secondQ);
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

    Queue limitQ =
        new Queue("limitQueue")
            .withRateLimit(limit, period)
            .withConcurrency(1)
            .withWorkerConcurrency(1);
    dbos.registerQueue(limitQ);

    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());

    dbos.launch();
    var queueService = DBOSTestAccess.getQueueService(dbos);
    queueService.setSpeedupForTest();
    Thread.sleep(1000);

    int numWaves = 3;
    int numTasks = numWaves * limit;
    List<WorkflowHandle<Double, ?>> handles = new ArrayList<>();
    List<Double> times = new ArrayList<>();

    for (int i = 0; i < numTasks; i++) {
      String id = "id" + i;
      var options = new StartWorkflowOptions(id).withQueue(limitQ);
      WorkflowHandle<Double, ?> handle =
          dbos.startWorkflow(() -> serviceQ.limitWorkflow("abc", "123"), options);
      handles.add(handle);
    }

    for (WorkflowHandle<Double, ?> h : handles) {
      double result = h.getResult();
      logger.info(String.valueOf(result));
      times.add(result);
    }

    // CockroachDB's slower transaction throughput can spread tasks across a wider window
    double waveTolerance = PgContainer.USE_COCKROACH_DB ? 3.0 : 1.0;
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

    Queue qwithWCLimit =
        new Queue("QwithWCLimit").withConcurrency(1).withWorkerConcurrency(2).withConcurrency(3);
    dbos.registerQueue(qwithWCLimit);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
    var queueService = DBOSTestAccess.getQueueService(dbos);

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
      systemDatabase.initWorkflowStatus(status, null, false, false);
    }

    var readBack = systemDatabase.listWorkflows(new ListWorkflowsInput("id0")).get(0);
    assertEquals(List.of("admin", "operator"), readBack.authenticatedRoles());

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

    Queue qwithWCLimit =
        new Queue("QwithWCLimit").withConcurrency(1).withWorkerConcurrency(2).withConcurrency(3);
    dbos.registerQueue(qwithWCLimit);
    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
    var queueService = DBOSTestAccess.getQueueService(dbos);

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

    Queue firstQ = new Queue("firstQueue");
    dbos.registerQueue(firstQ);

    ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());

    dbos.launch();

    String id = "q1234";

    var option = new StartWorkflowOptions(id).withQueue(firstQ);
    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(() -> serviceQ.simpleQWorkflow("inputq"), option);

    assertEquals(id, handle.workflowId());
    String result = handle.getResult();
    assertEquals("inputqinputq", result);
  }

  @Test
  public void testQueueConcurrencyUnderRecovery() throws Exception {
    Queue queue = new Queue("test_queue").withConcurrency(2);
    dbos.registerQueue(queue);

    ConcurrencyTestServiceImpl impl = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = dbos.registerProxy(ConcurrencyTestService.class, impl);

    dbos.launch();

    var opt1 = new StartWorkflowOptions("wf1").withQueue(queue);
    var handle1 = dbos.startWorkflow(() -> service.blockedWorkflow(0), opt1);

    var opt2 = new StartWorkflowOptions("wf2").withQueue(queue);
    var handle2 = dbos.startWorkflow(() -> service.blockedWorkflow(1), opt2);

    var opt3 = new StartWorkflowOptions("wf3").withQueue(queue);
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

    impl.latch.countDown();
    assertEquals(0, handle1.getResult());
    assertEquals(1, handle2.getResult());
    assertEquals(2, handle3.getResult());
    assertEquals("local", handle3.getStatus().executorId());

    assertTrue(DBUtils.queueEntriesAreCleanedUp(dataSource));
  }

  @Test
  public void testCancellingQueuedWorkflows() throws Exception {
    // Cancelling a PENDING workflow should free the concurrency slot so the queued workflow runs.
    Queue queue = new Queue("test_queue").withConcurrency(1);
    dbos.registerQueue(queue);

    ConcurrencyTestServiceImpl impl = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = dbos.registerProxy(ConcurrencyTestService.class, impl);
    dbos.launch();

    var blockedHandle =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(0), new StartWorkflowOptions("blocked").withQueue(queue));
    var regularHandle =
        dbos.startWorkflow(
            () -> service.noopWorkflow(42), new StartWorkflowOptions().withQueue(queue));

    impl.wfSemaphore.acquire(1);
    assertEquals(WorkflowState.PENDING, blockedHandle.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED, regularHandle.getStatus().status());

    dbos.cancelWorkflow("blocked");
    assertEquals(WorkflowState.CANCELLED, blockedHandle.getStatus().status());
    assertEquals(42, (int) regularHandle.getResult());

    impl.latch.countDown(); // let the blocked thread exit cleanly
    assertTrue(DBUtils.queueEntriesAreCleanedUp(dataSource));
  }

  @Test
  public void testResumingQueuedWorkflows() throws Exception {
    // Resuming an ENQUEUED workflow bypasses the queue concurrency limit and runs it immediately.
    Queue queue = new Queue("test_queue").withConcurrency(1);
    dbos.registerQueue(queue);

    ConcurrencyTestServiceImpl impl = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = dbos.registerProxy(ConcurrencyTestService.class, impl);
    dbos.launch();

    var blockedHandle =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(0), new StartWorkflowOptions().withQueue(queue));
    var regularHandle =
        dbos.startWorkflow(
            () -> service.noopWorkflow(99), new StartWorkflowOptions("resumable").withQueue(queue));

    impl.wfSemaphore.acquire(1);
    assertEquals(WorkflowState.ENQUEUED, regularHandle.getStatus().status());

    var resumedHandle = dbos.<Integer, Exception>resumeWorkflow("resumable");
    assertEquals(99, (int) resumedHandle.getResult());

    impl.latch.countDown();
    blockedHandle.getResult();
    assertTrue(DBUtils.queueEntriesAreCleanedUp(dataSource));
  }

  @Test
  public void testOneAtATimeWithWorkerConcurrency() throws Exception {
    // workerConcurrency=1: second workflow must stay ENQUEUED until first completes.
    Queue queue = new Queue("test_queue").withWorkerConcurrency(1);
    dbos.registerQueue(queue);

    ConcurrencyTestServiceImpl impl = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = dbos.registerProxy(ConcurrencyTestService.class, impl);
    dbos.launch();

    var h1 =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(0), new StartWorkflowOptions().withQueue(queue));
    var h2 =
        dbos.startWorkflow(
            () -> service.noopWorkflow(1), new StartWorkflowOptions().withQueue(queue));

    impl.wfSemaphore.acquire(1);
    Thread.sleep(2000); // let several poll cycles run
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
    // Workflows with short timeouts should be cancelled; a normal workflow should succeed.
    Queue queue = new Queue("test_queue").withConcurrency(1);
    dbos.registerQueue(queue);

    ConcurrencyTestServiceImpl impl = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = dbos.registerProxy(ConcurrencyTestService.class, impl);
    dbos.launch();

    var h1 =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(0),
            new StartWorkflowOptions().withQueue(queue).withTimeout(500, TimeUnit.MILLISECONDS));
    var h2 =
        dbos.startWorkflow(
            () -> service.blockedWorkflow(1),
            new StartWorkflowOptions().withQueue(queue).withTimeout(500, TimeUnit.MILLISECONDS));
    var normalHandle =
        dbos.startWorkflow(
            () -> service.noopWorkflow(42),
            new StartWorkflowOptions().withQueue(queue).withTimeout(10, TimeUnit.SECONDS));

    assertThrows(Exception.class, h1::getResult);
    assertThrows(Exception.class, h2::getResult);
    assertEquals(42, (int) normalHandle.getResult());
    assertTrue(DBUtils.queueEntriesAreCleanedUp(dataSource));
  }

  @Test
  public void testMultipleExecutorWorkerConcurrency() throws Exception {
    // Two DBOS instances share a queue. Each respects workerConcurrency independently;
    // together they respect the global concurrency cap.
    int workerConcurrency = 2;
    int globalConcurrency = workerConcurrency * 2;

    Queue queue =
        new Queue("multi_exec_queue")
            .withWorkerConcurrency(workerConcurrency)
            .withConcurrency(globalConcurrency);
    dbos.registerQueue(queue);

    ConcurrencyTestServiceImpl impl1 = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = dbos.registerProxy(ConcurrencyTestService.class, impl1);
    dbos.launch();

    // Fill exec1 to its local workerConcurrency limit.
    for (int i = 0; i < workerConcurrency; i++) {
      final int fi = i;
      dbos.startWorkflow(
          () -> service.blockedWorkflow(fi), new StartWorkflowOptions().withQueue(queue));
    }
    impl1.wfSemaphore.acquire(workerConcurrency);

    // Start exec2 against the same database; exec1 is full so exec2 picks up new work.
    ConcurrencyTestServiceImpl impl2 = new ConcurrencyTestServiceImpl();
    try (var dbos2 = new DBOS(dbosConfig.withExecutorId("executor2"))) {
      ConcurrencyTestService service2 = dbos2.registerProxy(ConcurrencyTestService.class, impl2);
      dbos2.registerQueue(queue);
      dbos2.launch();

      List<WorkflowHandle<Integer, ?>> handles2 = new ArrayList<>();
      for (int i = 0; i < workerConcurrency; i++) {
        final int fi = i;
        handles2.add(
            dbos2.startWorkflow(
                () -> service2.blockedWorkflow(fi), new StartWorkflowOptions().withQueue(queue)));
      }
      impl2.wfSemaphore.acquire(workerConcurrency);
      for (var h : handles2) {
        assertEquals("executor2", h.getStatus().executorId());
      }

      // Global limit reached: one more workflow must stay ENQUEUED.
      var extra =
          dbos2.startWorkflow(
              () -> service2.noopWorkflow(99), new StartWorkflowOptions().withQueue(queue));
      Thread.sleep(2000);
      assertEquals(WorkflowState.ENQUEUED, extra.getStatus().status());

      // Releasing exec2 frees global slots; the extra workflow should now run.
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
  public void testListenQueue() throws Exception {
    var config = dbosConfig.withListenQueue("queueOne");
    try (var dbos = new DBOS(config)) {

      Queue queueOne = new Queue("queueOne");
      Queue queueTwo = new Queue("queueTwo");
      dbos.registerQueues(queueOne, queueTwo);

      ServiceQ serviceQ = dbos.registerProxy(ServiceQ.class, new ServiceQImpl());
      dbos.launch();

      var h2 =
          dbos.startWorkflow(
              () -> serviceQ.simpleQWorkflow("two"), new StartWorkflowOptions(queueTwo));
      var h1 =
          dbos.startWorkflow(
              () -> serviceQ.simpleQWorkflow("one"), new StartWorkflowOptions(queueOne));

      Thread.sleep(3000);
      assertEquals("oneone", h1.getResult());
      assertEquals(WorkflowState.ENQUEUED, h2.getStatus().status());
    }
  }
}
