package dev.dbos.transact.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
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
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class QueuesTest {

  Logger logger = LoggerFactory.getLogger(QueuesTest.class);

  private static DBOSConfig dbosConfig;
  private static DataSource dataSource;
  private DBOS dbos;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    QueuesTest.dbosConfig =
        new DBOSConfig.Builder()
            .appName("systemdbtest")
            .databaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .dbUser("postgres")
            .maximumPoolSize(2)
            .build();
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    dataSource = SystemDatabase.createDataSource(dbosConfig);

    dbos = DBOS.initialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dbos.shutdown();
  }

  @Test
  public void testQueuedWorkflow() throws Exception {

    Queue firstQ = dbos.Queue("firstQueue").concurrency(1).workerConcurrency(1).build();

    ServiceQ serviceQ =
        dbos.<ServiceQ>Workflow()
            .interfaceClass(ServiceQ.class)
            .implementation(new ServiceQImpl())
            .build();

    dbos.launch();

    String id = "q1234";
    dbos.startWorkflow(
        () -> serviceQ.simpleQWorkflow("inputq"), new StartWorkflowOptions(id).withQueue(firstQ));

    var handle = dbos.retrieveWorkflow(id);
    assertEquals(id, handle.getWorkflowId());
    String result = (String) handle.getResult();
    assertEquals("inputqinputq", result);
  }

  @Test
  public void testDedupeId() throws Exception {

    Queue firstQ = dbos.Queue("firstQueue").concurrency(1).workerConcurrency(1).build();

    ServiceQ serviceQ =
        dbos.<ServiceQ>Workflow()
            .interfaceClass(ServiceQ.class)
            .implementation(new ServiceQImpl())
            .build();

    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    var options = new StartWorkflowOptions().withQueue(firstQ, "dedupe");
    dbos.startWorkflow(() -> serviceQ.simpleQWorkflow("inputq"), options);

    assertThrows(
        RuntimeException.class,
        () -> dbos.startWorkflow(() -> serviceQ.simpleQWorkflow("id"), options));
  }

  @Test
  public void testPriority() throws Exception {

    Queue firstQ =
        dbos.Queue("firstQueue").priorityEnabled(true).concurrency(1).workerConcurrency(1).build();

    ServiceQImpl impl = new ServiceQImpl();
    ServiceQ serviceQ =
        dbos.<ServiceQ>Workflow().interfaceClass(ServiceQ.class).implementation(impl).build();

    dbos.launch();

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    var o1 = new StartWorkflowOptions().withQueue(firstQ, 100);
    var h1 = dbos.startWorkflow(() -> serviceQ.priorityWorkflow(100), o1);

    var o2 = new StartWorkflowOptions().withQueue(firstQ, 50);
    dbos.startWorkflow(() -> serviceQ.priorityWorkflow(50), o2);

    var o3 = new StartWorkflowOptions().withQueue(firstQ, 10);
    dbos.startWorkflow(() -> serviceQ.priorityWorkflow(10), o3);

    qs.unpause();

    h1.getResult();

    assertEquals(3, impl.queue.size());
    assertEquals(10, impl.queue.remove());
    assertEquals(50, impl.queue.remove());
    assertEquals(100, impl.queue.remove());
  }

  @Test
  public void testQueuedMultipleWorkflows() throws Exception {

    Queue firstQ = dbos.Queue("firstQueue").concurrency(1).workerConcurrency(1).build();

    ServiceQ serviceQ =
        dbos.<ServiceQ>Workflow()
            .interfaceClass(ServiceQ.class)
            .implementation(new ServiceQImpl())
            .build();

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

    List<WorkflowStatus> wfs = dbos.listQueuedWorkflows(new ListQueuedWorkflowsInput(), true);

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;

      assertEquals(id, wfs.get(i).workflowId());
      assertEquals(WorkflowState.ENQUEUED.name(), wfs.get(i).status());
    }

    queueService.unpause();

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;

      var handle = dbos.retrieveWorkflow(id);
      assertEquals(id, handle.getWorkflowId());
      String result = (String) handle.getResult();
      assertEquals("inputq" + i + "inputq" + i, result);
      assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());
    }
  }

  @Test
  void testListQueuedWorkflow() throws Exception {

    Queue firstQ = dbos.Queue("firstQueue").concurrency(1).workerConcurrency(1).build();

    ServiceQ serviceQ =
        dbos.<ServiceQ>Workflow()
            .interfaceClass(ServiceQ.class)
            .implementation(new ServiceQImpl())
            .build();
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

    List<WorkflowStatus> wfs = dbos.listQueuedWorkflows(new ListQueuedWorkflowsInput(), true);
    wfs.sort(
        (a, b) -> {
          return a.workflowId().compareTo(b.workflowId());
        });

    for (int i = 0; i < 5; i++) {
      String id = "wfid" + i;

      assertEquals(id, wfs.get(i).workflowId());
      assertEquals(WorkflowState.ENQUEUED.name(), wfs.get(i).status());
    }

    ListQueuedWorkflowsInput input = new ListQueuedWorkflowsInput();
    input.setQueueName("abc");

    wfs = dbos.listQueuedWorkflows(input, true);
    assertEquals(0, wfs.size());

    input = new ListQueuedWorkflowsInput();
    input.setQueueName("firstQueue");

    wfs = dbos.listQueuedWorkflows(input, true);
    assertEquals(5, wfs.size());

    input = new ListQueuedWorkflowsInput();
    input.setStartTime(OffsetDateTime.now().minus(10, ChronoUnit.SECONDS));

    wfs = dbos.listQueuedWorkflows(input, true);
    assertEquals(5, wfs.size());

    input = new ListQueuedWorkflowsInput();
    input.setStartTime(OffsetDateTime.now().plus(10, ChronoUnit.SECONDS));

    wfs = dbos.listQueuedWorkflows(input, true);
    assertEquals(0, wfs.size());

    input = new ListQueuedWorkflowsInput();
    input.setEndTime(OffsetDateTime.now());
    wfs = dbos.listQueuedWorkflows(input, true);
    assertEquals(5, wfs.size());

    input = new ListQueuedWorkflowsInput();
    input.setEndTime(OffsetDateTime.now().minus(10, ChronoUnit.SECONDS));
    wfs = dbos.listQueuedWorkflows(input, true);
    assertEquals(0, wfs.size());
  }

  @Test
  public void multipleQueues() throws Exception {

    Queue firstQ = dbos.Queue("firstQueue").concurrency(1).workerConcurrency(1).build();

    ServiceQ serviceQ1 =
        dbos.<ServiceQ>Workflow()
            .interfaceClass(ServiceQ.class)
            .implementation(new ServiceQImpl())
            .build();

    Queue secondQ = dbos.Queue("secondQueue").concurrency(1).workerConcurrency(1).build();

    ServiceI serviceI =
        dbos.<ServiceI>Workflow()
            .interfaceClass(ServiceI.class)
            .implementation(new ServiceIImpl())
            .build();

    dbos.launch();

    String id1 = "firstQ1234";
    String id2 = "second1234";

    var options1 = new StartWorkflowOptions(id1).withQueue(firstQ);
    WorkflowHandle<String, ?> handle1 =
        dbos.startWorkflow(() -> serviceQ1.simpleQWorkflow("firstinput"), options1);

    var options2 = new StartWorkflowOptions(id2).withQueue(secondQ);
    WorkflowHandle<Integer, ?> handle2 = dbos.startWorkflow(() -> serviceI.workflowI(25), options2);

    assertEquals(id1, handle1.getWorkflowId());
    String result = handle1.getResult();
    assertEquals("firstQueue", handle1.getStatus().queueName());
    assertEquals("firstinputfirstinput", result);
    assertEquals(WorkflowState.SUCCESS.name(), handle1.getStatus().status());

    assertEquals(id2, handle2.getWorkflowId());
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
        dbos.Queue("limitQueue").limit(limit, period).concurrency(1).workerConcurrency(1).build();

    ServiceQ serviceQ =
        dbos.<ServiceQ>Workflow()
            .interfaceClass(ServiceQ.class)
            .implementation(new ServiceQImpl())
            .build();

    dbos.launch();

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
        dbos.Queue("QwithWCLimit").concurrency(1).workerConcurrency(2).concurrency(3).build();

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
    var queueService = DBOSTestAccess.getQueueService(dbos);

    String executorId = dbosExecutor.executorId();
    String appVersion = dbosExecutor.appVersion();

    queueService.stop();
    while (!queueService.isStopped()) {
      Thread.sleep(2000);
      logger.info("Waiting for queueService to stop");
    }

    WorkflowStatusInternal wfStatusInternal =
        new WorkflowStatusInternal(
            "xxx",
            WorkflowState.SUCCESS,
            "OrderProcessingWorkflow",
            "com.example.workflows.OrderWorkflow",
            "prod-config",
            "user123@example.com",
            "admin",
            "admin,operator",
            "{\"result\":\"success\"}",
            null,
            System.currentTimeMillis() - 3600000,
            System.currentTimeMillis(),
            "QwithWCLimit",
            executorId,
            appVersion,
            "order-app-123",
            0,
            300000l,
            System.currentTimeMillis() + 2400000,
            "dedup-112233",
            1,
            "{\"orderId\":\"ORD-12345\"}");

    for (int i = 0; i < 4; i++) {

      try (Connection conn = dataSource.getConnection()) {

        String wfid = "id" + i;
        wfStatusInternal.setWorkflowUUID(wfid);
        wfStatusInternal.setStatus(WorkflowState.ENQUEUED);
        wfStatusInternal.setDeduplicationId("dedup" + i);
        systemDatabase.insertWorkflowStatus(conn, wfStatusInternal);
      }
    }

    List<String> idsToRun =
        systemDatabase.getAndStartQueuedWorkflows(qwithWCLimit, executorId, appVersion);

    assertEquals(2, idsToRun.size());

    // run the same above 2 are in Pending.
    // So no de queueing
    idsToRun = systemDatabase.getAndStartQueuedWorkflows(qwithWCLimit, executorId, appVersion);
    assertEquals(0, idsToRun.size());

    // mark the first 2 as success
    DBUtils.updateAllWorkflowStates(
        dataSource, WorkflowState.PENDING.name(), WorkflowState.SUCCESS.name());

    // next 2 get dequeued
    idsToRun = systemDatabase.getAndStartQueuedWorkflows(qwithWCLimit, executorId, appVersion);
    assertEquals(2, idsToRun.size());

    DBUtils.updateAllWorkflowStates(
        dataSource, WorkflowState.PENDING.name(), WorkflowState.SUCCESS.name());
    idsToRun =
        systemDatabase.getAndStartQueuedWorkflows(
            qwithWCLimit, Constants.DEFAULT_EXECUTORID, Constants.DEFAULT_APP_VERSION);
    assertEquals(0, idsToRun.size());
  }

  @Test
  public void testGlobalConcurrency() throws Exception {

    Queue qwithWCLimit =
        dbos.Queue("QwithWCLimit").concurrency(1).workerConcurrency(2).concurrency(3).build();
    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
    var queueService = DBOSTestAccess.getQueueService(dbos);

    String executorId = dbosExecutor.executorId();
    String appVersion = dbosExecutor.appVersion();

    queueService.stop();
    while (!queueService.isStopped()) {
      Thread.sleep(2000);
      logger.info("Waiting for queueService to stop");
    }

    WorkflowStatusInternal wfStatusInternal =
        new WorkflowStatusInternal(
            "xxx",
            WorkflowState.SUCCESS,
            "OrderProcessingWorkflow",
            "com.example.workflows.OrderWorkflow",
            "prod-config",
            "user123@example.com",
            "admin",
            "admin,operator",
            "{\"result\":\"success\"}",
            null,
            System.currentTimeMillis() - 3600000,
            System.currentTimeMillis(),
            "QwithWCLimit",
            executorId,
            appVersion,
            "order-app-123",
            0,
            300000l,
            System.currentTimeMillis() + 2400000,
            "dedup-112233",
            1,
            "{\"orderId\":\"ORD-12345\"}");

    // executor1
    for (int i = 0; i < 2; i++) {

      try (Connection conn = dataSource.getConnection()) {

        String wfid = "id" + i;
        wfStatusInternal.setWorkflowUUID(wfid);
        wfStatusInternal.setStatus(WorkflowState.ENQUEUED);
        wfStatusInternal.setDeduplicationId("dedup" + i);
        systemDatabase.insertWorkflowStatus(conn, wfStatusInternal);
      }
    }

    // executor2

    String executor2 = "remote";
    for (int i = 2; i < 5; i++) {

      try (Connection conn = dataSource.getConnection()) {

        String wfid = "id" + i;
        wfStatusInternal.setWorkflowUUID(wfid);
        wfStatusInternal.setStatus(WorkflowState.PENDING);
        wfStatusInternal.setDeduplicationId("dedup" + i);
        wfStatusInternal.setExecutorId(executor2);
        systemDatabase.insertWorkflowStatus(conn, wfStatusInternal);
      }
    }

    List<String> idsToRun =
        systemDatabase.getAndStartQueuedWorkflows(qwithWCLimit, executorId, appVersion);
    // 0 because global concurrency limit is reached
    assertEquals(0, idsToRun.size());

    DBUtils.updateAllWorkflowStates(
        dataSource, WorkflowState.PENDING.name(), WorkflowState.SUCCESS.name());
    idsToRun =
        systemDatabase.getAndStartQueuedWorkflows(
            qwithWCLimit,
            // executorId,
            executor2,
            appVersion);
    assertEquals(2, idsToRun.size());
  }

  @Test
  public void testenQueueWF() throws Exception {

    Queue firstQ = dbos.Queue("firstQueue").build();

    ServiceQ serviceQ =
        dbos.<ServiceQ>Workflow()
            .interfaceClass(ServiceQ.class)
            .implementation(new ServiceQImpl())
            .build();

    dbos.launch();

    String id = "q1234";

    var option = new StartWorkflowOptions(id).withQueue(firstQ);
    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(() -> serviceQ.simpleQWorkflow("inputq"), option);

    assertEquals(id, handle.getWorkflowId());
    String result = handle.getResult();
    assertEquals("inputqinputq", result);
  }

  @Test
  public void testQueueConcurrencyUnderRecovery() throws Exception {
    Queue queue = dbos.Queue("test_queue").concurrency(2).build();

    ConcurrencyTestServiceImpl impl = new ConcurrencyTestServiceImpl();
    ConcurrencyTestService service =
        dbos.<ConcurrencyTestService>Workflow()
            .interfaceClass(ConcurrencyTestService.class)
            .implementation(impl)
            .build();

    dbos.launch();

    var opt1 = new StartWorkflowOptions("wf1").withQueue(queue);
    var handle1 = dbos.startWorkflow(() -> service.blockedWorkflow(0), opt1);

    var opt2 = new StartWorkflowOptions("wf2").withQueue(queue);
    var handle2 = dbos.startWorkflow(() -> service.blockedWorkflow(1), opt2);

    var opt3 = new StartWorkflowOptions("wf3").withQueue(queue);
    var handle3 = dbos.startWorkflow(() -> service.noopWorkflow(2), opt3);

    for (Semaphore e : impl.wfSemaphores) {
      e.acquire();
      e.drainPermits();
    }

    assertEquals(2, impl.counter);
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

    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    List<WorkflowHandle<?, ?>> otherHandles = executor.recoverPendingWorkflows(List.of("other"));
    assertEquals(WorkflowState.PENDING.toString(), handle1.getStatus().status());
    assertEquals(WorkflowState.PENDING.toString(), handle2.getStatus().status());
    assertEquals(1, otherHandles.size());
    assertEquals(otherHandles.get(0).getWorkflowId(), handle3.getWorkflowId());
    assertEquals(WorkflowState.ENQUEUED.toString(), handle3.getStatus().status());

    List<WorkflowHandle<?, ?>> localHandles = executor.recoverPendingWorkflows(List.of("local"));
    assertEquals(2, localHandles.size());
    List<String> expectedWorkflowIds = List.of(handle1.getWorkflowId(), handle2.getWorkflowId());
    assertTrue(expectedWorkflowIds.contains(localHandles.get(0).getWorkflowId()));
    assertTrue(expectedWorkflowIds.contains(localHandles.get(1).getWorkflowId()));

    for (int i = 0; i < impl.wfSemaphores.size(); i++) {
      logger.info("acquire {} semaphore", i);
      impl.wfSemaphores.get(i).acquire();
    }

    assertEquals(4, impl.counter);
    assertEquals(WorkflowState.PENDING.toString(), handle1.getStatus().status());
    assertEquals(WorkflowState.PENDING.toString(), handle2.getStatus().status());
    assertEquals(WorkflowState.ENQUEUED.toString(), handle3.getStatus().status());

    impl.latch.countDown();
    assertEquals(0, handle1.getResult());
    assertEquals(1, handle2.getResult());
    assertEquals(2, handle3.getResult());
    assertEquals("local", handle3.getStatus().executorId());

    assertTrue(DBUtils.queueEntriesAreCleanedUp(dataSource));
  }
}
