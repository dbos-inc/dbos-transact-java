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
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class QueuesTest {

  private static final Logger logger = LoggerFactory.getLogger(QueuesTest.class);

  private static DBOSConfig dbosConfig;
  private HikariDataSource dataSource;

  @BeforeAll
  static void onetimeSetup() throws Exception {
    QueuesTest.dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys");
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    dataSource = SystemDatabase.createDataSource(dbosConfig);

    DBOS.reinitialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dataSource.close();
    DBOS.shutdown();
  }

  @Test
  public void testQueuedWorkflow() throws Exception {

    Queue firstQ = new Queue("firstQueue").withConcurrency(1).withWorkerConcurrency(1);
    DBOS.registerQueue(firstQ);

    ServiceQ serviceQ = DBOS.registerWorkflows(ServiceQ.class, new ServiceQImpl());
    DBOS.launch();

    String id = "q1234";
    DBOS.startWorkflow(
        () -> serviceQ.simpleQWorkflow("inputq"), new StartWorkflowOptions(id).withQueue(firstQ));

    var handle = DBOS.retrieveWorkflow(id);
    assertEquals(id, handle.workflowId());
    String result = (String) handle.getResult();
    assertEquals("inputqinputq", result);
  }

  @Test
  public void testDedupeId() throws Exception {

    Queue firstQ = new Queue("firstQueue");
    DBOS.registerQueue(firstQ);

    ServiceQ serviceQ = DBOS.registerWorkflows(ServiceQ.class, new ServiceQImpl());
    DBOS.launch();

    // pause queue service for test validation
    var qs = DBOSTestAccess.getQueueService();
    qs.pause();

    var options = new StartWorkflowOptions().withQueue(firstQ);
    var dedupeId = "dedupeId";
    var h1 =
        DBOS.startWorkflow(
            () -> serviceQ.simpleQWorkflow("abc"), options.withDeduplicationId(dedupeId));
    var s1 = h1.getStatus();
    assertEquals(s1.queueName(), firstQ.name());
    assertEquals(s1.deduplicationId(), dedupeId);

    // enqueue with different dedupe ID should be fine
    var dedupeId2 = "different-dedupeId";
    var h2 =
        DBOS.startWorkflow(
            () -> serviceQ.simpleQWorkflow("def"), options.withDeduplicationId(dedupeId2));
    var s2 = h2.getStatus();
    assertEquals(s2.queueName(), firstQ.name());
    assertEquals(s2.deduplicationId(), dedupeId2);

    // enqueue with no dedupe ID should be fine
    var h3 = DBOS.startWorkflow(() -> serviceQ.simpleQWorkflow("ghi"), options);
    var s3 = h3.getStatus();
    assertEquals(s3.queueName(), firstQ.name());
    assertNull(s3.deduplicationId());

    assertThrows(
        RuntimeException.class,
        () ->
            DBOS.startWorkflow(
                () -> serviceQ.simpleQWorkflow("jkl"), options.withDeduplicationId(dedupeId)));

    // enable queue service to run
    qs.unpause();

    // wait for initial workflow with initial dedupe ID to finish
    h1.getResult();
    h2.getResult();
    h3.getResult();

    var h4 =
        DBOS.startWorkflow(
            () -> serviceQ.simpleQWorkflow("jkl"), options.withDeduplicationId(dedupeId));
    h4.getResult();

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(4, rows.size());

    for (var row : rows) {
      assertEquals("SUCCESS", row.status());
      assertEquals("firstQueue", row.queueName());
      assertNull(row.deduplicationId());
    }
  }

  @Test
  public void testPriority() throws Exception {

    Queue firstQ =
        new Queue("firstQueue")
            .withPriorityEnabled(true)
            .withConcurrency(1)
            .withWorkerConcurrency(1);
    DBOS.registerQueue(firstQ);

    ServiceQImpl impl = new ServiceQImpl();
    ServiceQ serviceQ = DBOS.registerWorkflows(ServiceQ.class, impl);

    DBOS.launch();

    var qs = DBOSTestAccess.getQueueService();
    qs.pause();

    var o1 = new StartWorkflowOptions().withQueue(firstQ).withPriority(100);
    var h1 = DBOS.startWorkflow(() -> serviceQ.priorityWorkflow(100), o1);

    var o2 = new StartWorkflowOptions().withQueue(firstQ).withPriority(50);
    var h2 = DBOS.startWorkflow(() -> serviceQ.priorityWorkflow(50), o2);

    var o3 = new StartWorkflowOptions().withQueue(firstQ).withPriority(10);
    var h3 = DBOS.startWorkflow(() -> serviceQ.priorityWorkflow(10), o3);

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
    DBOS.registerQueue(firstQ);
    ServiceQ serviceQ = DBOS.registerWorkflows(ServiceQ.class, new ServiceQImpl());

    DBOS.launch();

    var queueService = DBOSTestAccess.getQueueService();
    queueService.pause();
    Thread.sleep(2000);

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;
      var input = "inputq" + i;
      DBOS.startWorkflow(
          () -> serviceQ.simpleQWorkflow(input), new StartWorkflowOptions(id).withQueue(firstQ));
    }

    var input = new ListWorkflowsInput().withQueuesOnly(true).withLoadInput(true);
    List<WorkflowStatus> wfs = DBOS.listWorkflows(input);

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;

      assertEquals(id, wfs.get(i).workflowId());
      assertEquals(WorkflowState.ENQUEUED.name(), wfs.get(i).status());
    }

    queueService.unpause();

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;

      var handle = DBOS.retrieveWorkflow(id);
      assertEquals(id, handle.workflowId());
      String result = (String) handle.getResult();
      assertEquals("inputq" + i + "inputq" + i, result);
      assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());
    }
  }

  @Test
  void testListQueuedWorkflow() throws Exception {

    Queue firstQ = new Queue("firstQueue").withConcurrency(1).withWorkerConcurrency(1);
    DBOS.registerQueue(firstQ);
    ServiceQ serviceQ = DBOS.registerWorkflows(ServiceQ.class, new ServiceQImpl());

    DBOS.launch();
    var queueService = DBOSTestAccess.getQueueService();

    queueService.pause();

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;
      var input = "inputq" + i;
      DBOS.startWorkflow(
          () -> serviceQ.simpleQWorkflow(input), new StartWorkflowOptions(id).withQueue(firstQ));
      Thread.sleep(100);
    }

    var input = new ListWorkflowsInput().withQueuesOnly(true).withLoadInput(true);
    List<WorkflowStatus> wfs = DBOS.listWorkflows(input);
    wfs.sort(
        (a, b) -> {
          return a.workflowId().compareTo(b.workflowId());
        });

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;

      assertEquals(id, wfs.get(i).workflowId());
      assertEquals(WorkflowState.ENQUEUED.name(), wfs.get(i).status());
    }

    wfs = DBOS.listWorkflows(input.withQueueName("abc"));
    assertEquals(0, wfs.size());

    wfs = DBOS.listWorkflows(input.withQueueName("firstQueue"));
    assertEquals(5, wfs.size());

    wfs =
        DBOS.listWorkflows(input.withStartTime(OffsetDateTime.now().minus(10, ChronoUnit.SECONDS)));
    assertEquals(5, wfs.size());

    wfs =
        DBOS.listWorkflows(input.withStartTime(OffsetDateTime.now().plus(10, ChronoUnit.SECONDS)));
    assertEquals(0, wfs.size());

    wfs = DBOS.listWorkflows(input.withEndTime(OffsetDateTime.now()));
    assertEquals(5, wfs.size());

    wfs = DBOS.listWorkflows(input.withEndTime(OffsetDateTime.now().minus(10, ChronoUnit.SECONDS)));
    assertEquals(0, wfs.size());
  }

  @Test
  public void multipleQueues() throws Exception {

    Queue firstQ = new Queue("firstQueue").withConcurrency(1).withWorkerConcurrency(1);
    DBOS.registerQueue(firstQ);
    ServiceQ serviceQ1 = DBOS.registerWorkflows(ServiceQ.class, new ServiceQImpl());

    Queue secondQ = new Queue("secondQueue").withConcurrency(1).withWorkerConcurrency(1);
    DBOS.registerQueue(secondQ);
    ServiceI serviceI = DBOS.registerWorkflows(ServiceI.class, new ServiceIImpl());

    DBOS.launch();

    String id1 = "firstQ1234";
    String id2 = "second1234";

    var options1 = new StartWorkflowOptions(id1).withQueue(firstQ);
    WorkflowHandle<String, ?> handle1 =
        DBOS.startWorkflow(() -> serviceQ1.simpleQWorkflow("firstinput"), options1);

    var options2 = new StartWorkflowOptions(id2).withQueue(secondQ);
    WorkflowHandle<Integer, ?> handle2 = DBOS.startWorkflow(() -> serviceI.workflowI(25), options2);

    assertEquals(id1, handle1.workflowId());
    String result = handle1.getResult();
    assertEquals("firstQueue", handle1.getStatus().queueName());
    assertEquals("firstinputfirstinput", result);
    assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().status());

    assertEquals(id2, handle2.workflowId());
    Integer result2 = (Integer) handle2.getResult();
    assertEquals("secondQueue", handle2.getStatus().queueName());
    assertEquals(50, result2);
    assertEquals(WorkflowState.SUCCESS.name(), handle2.getStatus().status());
  }

  @Test
  public void testLimiter() throws Exception {

    int limit = 5;
    double period = 1.8; //

    Queue limitQ =
        new Queue("limitQueue")
            .withRateLimit(limit, period)
            .withConcurrency(1)
            .withWorkerConcurrency(1);
    DBOS.registerQueue(limitQ);

    ServiceQ serviceQ = DBOS.registerWorkflows(ServiceQ.class, new ServiceQImpl());

    DBOS.launch();
    var queueService = DBOSTestAccess.getQueueService();
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
          DBOS.startWorkflow(() -> serviceQ.limitWorkflow("abc", "123"), options);
      handles.add(handle);
    }

    for (WorkflowHandle<Double, ?> h : handles) {
      double result = h.getResult();
      logger.info(String.valueOf(result));
      times.add(result);
    }

    double waveTolerance = 0.5;
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
          gap > period - periodTolerance,
          String.format(
              "Gap between wave %d and %d should be at least %.3f. Actual: %.3f",
              wave, wave + 1, period - periodTolerance, gap));
      assertTrue(
          gap < period + periodTolerance,
          String.format(
              "Gap between wave %d and %d should be at most %.3f. Actual: %.3f",
              wave, wave + 1, period + periodTolerance, gap));
    }

    for (WorkflowHandle<Double, ?> h : handles) {
      assertEquals(WorkflowState.SUCCESS.name(), h.getStatus().status());
    }
  }

  @Test
  public void testWorkerConcurrency() throws Exception {

    Queue qwithWCLimit =
        new Queue("QwithWCLimit").withConcurrency(1).withWorkerConcurrency(2).withConcurrency(3);
    DBOS.registerQueue(qwithWCLimit);

    DBOS.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor();
    var queueService = DBOSTestAccess.getQueueService();

    String executorId = dbosExecutor.executorId();
    String appVersion = dbosExecutor.appVersion();

    queueService.stop();
    while (!queueService.isStopped()) {
      Thread.sleep(2000);
      logger.info("Waiting for queueService to stop");
    }

    var builder =
        WorkflowStatusInternal.builder()
            .name("OrderProcessingWorkflow")
            .className("com.example.workflows.OrderWorkflow")
            .instanceName("prod-config")
            .authenticatedUser("user123@example.com")
            .assumedRole("admin")
            .authenticatedRoles("admin,operator")
            .output("{\"result\":\"success\"}")
            .createdAt(System.currentTimeMillis() - 3600000)
            .updatedAt(System.currentTimeMillis())
            .queueName("QwithWCLimit")
            .executorId(executorId)
            .appVersion(appVersion)
            .appId("order-app-123")
            .recoveryAttempts(0L)
            .timeoutMs(300000l)
            .deadlineEpochMs(System.currentTimeMillis() + 2400000)
            .priority(1)
            .inputs("{\"orderId\":\"ORD-12345\"}");

    for (int i = 0; i < 4; i++) {
      String wfid = "id" + i;
      var status =
          builder
              .workflowId(wfid)
              .status(WorkflowState.ENQUEUED)
              .deduplicationId("dedup" + i)
              .build();
      systemDatabase.initWorkflowStatus(status, null, false, false);
    }

    List<String> idsToRun =
        systemDatabase.getAndStartQueuedWorkflows(qwithWCLimit, executorId, appVersion, null);

    assertEquals(2, idsToRun.size());

    // run the same above 2 are in Pending.
    // So no de queueing
    idsToRun =
        systemDatabase.getAndStartQueuedWorkflows(qwithWCLimit, executorId, appVersion, null);
    assertEquals(0, idsToRun.size());

    // mark the first 2 as success
    DBUtils.updateAllWorkflowStates(
        dataSource, WorkflowState.PENDING.name(), WorkflowState.SUCCESS.name());

    // next 2 get dequeued
    idsToRun =
        systemDatabase.getAndStartQueuedWorkflows(qwithWCLimit, executorId, appVersion, null);
    assertEquals(2, idsToRun.size());

    DBUtils.updateAllWorkflowStates(
        dataSource, WorkflowState.PENDING.name(), WorkflowState.SUCCESS.name());
    idsToRun =
        systemDatabase.getAndStartQueuedWorkflows(
            qwithWCLimit, Constants.DEFAULT_EXECUTORID, Constants.DEFAULT_APP_VERSION, null);
    assertEquals(0, idsToRun.size());
  }

  @Test
  public void testGlobalConcurrency() throws Exception {

    Queue qwithWCLimit =
        new Queue("QwithWCLimit").withConcurrency(1).withWorkerConcurrency(2).withConcurrency(3);
    DBOS.registerQueue(qwithWCLimit);
    DBOS.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor();
    var queueService = DBOSTestAccess.getQueueService();

    String executorId = dbosExecutor.executorId();
    String appVersion = dbosExecutor.appVersion();

    queueService.stop();
    while (!queueService.isStopped()) {
      Thread.sleep(2000);
      logger.info("Waiting for queueService to stop");
    }

    var builder =
        WorkflowStatusInternal.builder()
            .name("OrderProcessingWorkflow")
            .className("com.example.workflows.OrderWorkflow")
            .instanceName("prod-config")
            .authenticatedUser("user123@example.com")
            .assumedRole("admin")
            .authenticatedRoles("admin,operator")
            .output("{\"result\":\"success\"}")
            .createdAt(System.currentTimeMillis() - 3600000)
            .updatedAt(System.currentTimeMillis())
            .queueName("QwithWCLimit")
            .executorId(executorId)
            .appVersion(appVersion)
            .appId("order-app-123")
            .recoveryAttempts(0L)
            .timeoutMs(300000l)
            .deadlineEpochMs(System.currentTimeMillis() + 2400000)
            .priority(1)
            .inputs("{\"orderId\":\"ORD-12345\"}");

    // executor1
    for (int i = 0; i < 2; i++) {
      String wfid = "id" + i;
      var status =
          builder
              .workflowId(wfid)
              .status(WorkflowState.ENQUEUED)
              .deduplicationId("dedup" + i)
              .build();
      systemDatabase.initWorkflowStatus(status, null, false, false);
    }

    // executor2
    String executor2 = "remote";
    for (int i = 2; i < 5; i++) {

      String wfid = "id" + i;
      var status =
          builder
              .workflowId(wfid)
              .status(WorkflowState.PENDING)
              .deduplicationId("dedup" + i)
              .executorId(executor2)
              .build();
      systemDatabase.initWorkflowStatus(status, null, false, false);
    }

    List<String> idsToRun =
        systemDatabase.getAndStartQueuedWorkflows(qwithWCLimit, executorId, appVersion, null);
    // 0 because global concurrency limit is reached
    assertEquals(0, idsToRun.size());

    DBUtils.updateAllWorkflowStates(
        dataSource, WorkflowState.PENDING.name(), WorkflowState.SUCCESS.name());
    idsToRun =
        systemDatabase.getAndStartQueuedWorkflows(
            qwithWCLimit,
            // executorId,
            executor2,
            appVersion,
            null);
    assertEquals(2, idsToRun.size());
  }

  @Test
  public void testenQueueWF() throws Exception {

    Queue firstQ = new Queue("firstQueue");
    DBOS.registerQueue(firstQ);

    ServiceQ serviceQ = DBOS.registerWorkflows(ServiceQ.class, new ServiceQImpl());

    DBOS.launch();

    String id = "q1234";

    var option = new StartWorkflowOptions(id).withQueue(firstQ);
    WorkflowHandle<String, ?> handle =
        DBOS.startWorkflow(() -> serviceQ.simpleQWorkflow("inputq"), option);

    assertEquals(id, handle.workflowId());
    String result = handle.getResult();
    assertEquals("inputqinputq", result);
  }

  @Test
  public void testQueueConcurrencyUnderRecovery() throws Exception {
    Queue queue = new Queue("test_queue").withConcurrency(2);
    DBOS.registerQueue(queue);

    ConcurrencyTestServiceImpl impl = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service = DBOS.registerWorkflows(ConcurrencyTestService.class, impl);

    DBOS.launch();

    var opt1 = new StartWorkflowOptions("wf1").withQueue(queue);
    var handle1 = DBOS.startWorkflow(() -> service.blockedWorkflow(0), opt1);

    var opt2 = new StartWorkflowOptions("wf2").withQueue(queue);
    var handle2 = DBOS.startWorkflow(() -> service.blockedWorkflow(1), opt2);

    var opt3 = new StartWorkflowOptions("wf3").withQueue(queue);
    var handle3 = DBOS.startWorkflow(() -> service.noopWorkflow(2), opt3);

    // each call to blockedWorkflow releases the semaphore once,
    // so block waiting on both calls to release
    impl.wfSemaphore.acquire(2);

    assertEquals(2, impl.counter.get());
    assertEquals(WorkflowState.PENDING.toString(), handle1.getStatus().status());
    assertEquals(WorkflowState.PENDING.toString(), handle2.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED.toString(), handle3.getStatus().status());

    // update WF3 to appear as if it's from a different executor
    String sql =
        "UPDATE dbos.workflow_status SET status = ?, executor_id = ? where workflow_uuid = ?;";

    try (Connection connection = DBUtils.getConnection(dbosConfig);
        PreparedStatement pstmt = connection.prepareStatement(sql)) {

      pstmt.setString(1, WorkflowState.PENDING.toString());
      pstmt.setString(2, "other");
      pstmt.setString(3, opt3.workflowId());

      // Execute the update and get the number of rows affected
      int rowsAffected = pstmt.executeUpdate();
      assertEquals(1, rowsAffected);
    }

    var executor = DBOSTestAccess.getDbosExecutor();
    List<WorkflowHandle<?, ?>> otherHandles = executor.recoverPendingWorkflows(List.of("other"));
    assertEquals(WorkflowState.PENDING.toString(), handle1.getStatus().status());
    assertEquals(WorkflowState.PENDING.toString(), handle2.getStatus().status());
    assertEquals(1, otherHandles.size());
    assertEquals(otherHandles.get(0).workflowId(), handle3.workflowId());
    assertEquals(WorkflowState.ENQUEUED.toString(), handle3.getStatus().status());

    List<WorkflowHandle<?, ?>> localHandles = executor.recoverPendingWorkflows(List.of("local"));
    assertEquals(2, localHandles.size());
    List<String> expectedWorkflowIds = List.of(handle1.workflowId(), handle2.workflowId());
    assertTrue(expectedWorkflowIds.contains(localHandles.get(0).workflowId()));
    assertTrue(expectedWorkflowIds.contains(localHandles.get(1).workflowId()));

    assertEquals(2, impl.counter);
    // Recovery sets back to enqueued.
    //   The enqueued run will get skipped (first run is still blocked)
    assertEquals(WorkflowState.ENQUEUED.toString(), handle1.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED.toString(), handle2.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED.toString(), handle3.getStatus().status());

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
    DBOS.reinitialize(config);

    Queue queueOne = new Queue("queueOne");
    Queue queueTwo = new Queue("queueTwo");
    DBOS.registerQueue(queueOne);
    DBOS.registerQueue(queueTwo);

    ServiceQ serviceQ = DBOS.registerWorkflows(ServiceQ.class, new ServiceQImpl());
    DBOS.launch();

    var h2 =
        DBOS.startWorkflow(
            () -> serviceQ.simpleQWorkflow("two"), new StartWorkflowOptions(queueTwo));
    var h1 =
        DBOS.startWorkflow(
            () -> serviceQ.simpleQWorkflow("one"), new StartWorkflowOptions(queueOne));

    Thread.sleep(3000);
    assertEquals("oneone", h1.getResult());
    assertEquals("ENQUEUED", h2.getStatus().status());
  }
}
