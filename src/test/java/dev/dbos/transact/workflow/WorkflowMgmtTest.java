package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class WorkflowMgmtTest {

  private static final Logger logger = LoggerFactory.getLogger(WorkflowMgmtTest.class);

  private static DBOSConfig dbosConfig;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    WorkflowMgmtTest.dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withDbUser("postgres")
            .withMaximumPoolSize(2);
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);

    DBOS.reinitialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    DBOS.shutdown();
  }

  @Test
  public void asyncCancelResumeTest() throws Exception {

    CountDownLatch mainLatch = new CountDownLatch(1);
    CountDownLatch workLatch = new CountDownLatch(1);

    MgmtService mgmtService =
        DBOS.registerWorkflows(MgmtService.class, new MgmtServiceImpl(mainLatch, workLatch));
    mgmtService.setMgmtService(mgmtService);

    DBOS.launch();

    String workflowId = "wfid1";
    var options = new StartWorkflowOptions(workflowId);
    int result;
    WorkflowHandle<Integer, ?> h =
        DBOS.startWorkflow(() -> mgmtService.simpleWorkflow(23), options);

    mainLatch.await();
    DBOS.cancelWorkflow(workflowId);
    workLatch.countDown();

    assertEquals(1, mgmtService.getStepsExecuted());
    assertEquals(WorkflowState.CANCELLED.name(), h.getStatus().status());

    WorkflowHandle<Integer, ?> handle = DBOS.resumeWorkflow(workflowId);

    result = handle.getResult();
    assertEquals(23, result);
    assertEquals(3, mgmtService.getStepsExecuted());

    // resume again

    handle = DBOS.resumeWorkflow(workflowId);

    result = handle.getResult();
    assertEquals(23, result);
    assertEquals(3, mgmtService.getStepsExecuted());
    h = DBOS.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), h.getStatus().status());

    logger.info("Test completed");
  }

  @Test
  public void queuedCancelResumeTest() throws Exception {

    CountDownLatch mainLatch = new CountDownLatch(1);
    CountDownLatch workLatch = new CountDownLatch(1);

    MgmtService mgmtService =
        DBOS.registerWorkflows(MgmtService.class, new MgmtServiceImpl(mainLatch, workLatch));
    mgmtService.setMgmtService(mgmtService);

    Queue myqueue = new Queue("myqueue");
    DBOS.registerQueue(myqueue);

    DBOS.launch();

    String workflowId = "wfid1";
    var options = new StartWorkflowOptions(workflowId).withQueue(myqueue);
    int result;
    DBOS.startWorkflow(() -> mgmtService.simpleWorkflow(23), options);

    mainLatch.await();
    DBOS.cancelWorkflow(workflowId);
    workLatch.countDown();

    assertEquals(1, mgmtService.getStepsExecuted());
    var h = DBOS.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.CANCELLED.name(), h.getStatus().status());

    WorkflowHandle<Integer, ?> handle = DBOS.resumeWorkflow(workflowId);

    result = handle.getResult();
    assertEquals(23, result);
    assertEquals(3, mgmtService.getStepsExecuted());

    // resume again

    handle = DBOS.resumeWorkflow(workflowId);

    result = handle.getResult();
    assertEquals(23, result);
    assertEquals(3, mgmtService.getStepsExecuted());
    h = DBOS.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), h.getStatus().status());

    logger.info("Test completed");
  }

  @Test
  public void syncCancelResumeTest() throws Exception {

    CountDownLatch mainLatch = new CountDownLatch(1);
    CountDownLatch workLatch = new CountDownLatch(1);

    MgmtService mgmtService =
        DBOS.registerWorkflows(MgmtService.class, new MgmtServiceImpl(mainLatch, workLatch));
    mgmtService.setMgmtService(mgmtService);

    DBOS.launch();

    ExecutorService e = Executors.newFixedThreadPool(2);
    String workflowId = "wfid1";

    CountDownLatch testLatch = new CountDownLatch(2);

    e.submit(
        () -> {
          WorkflowOptions options = new WorkflowOptions(workflowId);

          try {
            try (var o = options.setContext()) {
              mgmtService.simpleWorkflow(23);
            }
          } catch (Exception t) {
            assertTrue(t instanceof DBOSAwaitedWorkflowCancelledException);
          }

          assertEquals(1, mgmtService.getStepsExecuted());
          testLatch.countDown();
        });

    e.submit(
        () -> {
          try {
            mainLatch.await();
            DBOS.cancelWorkflow(workflowId);
            workLatch.countDown();
            testLatch.countDown();

          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            logger.error("syncCancelResumeTest interrupted", ie);
          }
        });

    testLatch.await();

    WorkflowHandle<Integer, ?> handle = DBOS.resumeWorkflow(workflowId);

    int result = handle.getResult();
    assertEquals(23, result);
    assertEquals(3, mgmtService.getStepsExecuted());

    // resume again

    handle = DBOS.resumeWorkflow(workflowId);

    result = (Integer) handle.getResult();
    assertEquals(23, result);
    assertEquals(3, mgmtService.getStepsExecuted());

    logger.info("Test completed");
  }

  @Test
  public void forkNonExistent() {

    DBOS.launch();

    try {
      ForkOptions options = new ForkOptions();
      DBOS.forkWorkflow("12345", 2, options);
      fail("An exception should have been thrown");
    } catch (Exception t) {
      logger.info(t.getClass().getName());
      assertTrue(t instanceof DBOSNonExistentWorkflowException);
    }
  }

  @Test
  public void testFork() throws SQLException {

    ForkServiceImpl impl = new ForkServiceImpl();

    ForkService forkService = DBOS.registerWorkflows(ForkService.class, impl);
    forkService.setForkService(forkService);

    DBOS.launch();

    String workflowId = "wfid1";
    WorkflowOptions options = new WorkflowOptions(workflowId);
    String result;
    try (var o = options.setContext()) {
      result = forkService.simpleWorkflow("hello");
    }

    assertEquals("hellohello", result);
    WorkflowHandle<String, ?> handle = DBOS.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.step3Count);
    assertEquals(1, impl.step4Count);
    assertEquals(1, impl.step5Count);

    logger.info("First execution done starting fork");

    ForkOptions foptions = new ForkOptions();
    WorkflowHandle<String, SQLException> rstatHandle = DBOS.forkWorkflow(workflowId, 0, foptions);
    result = rstatHandle.getResult();
    assertEquals("hellohello", result);
    assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().status());
    assertNotEquals(rstatHandle.getWorkflowId(), workflowId);

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(2, impl.step3Count);
    assertEquals(2, impl.step4Count);
    assertEquals(2, impl.step5Count);

    List<StepInfo> steps = DBOS.listWorkflowSteps(rstatHandle.getWorkflowId());
    assertEquals(5, steps.size());

    logger.info("first fork done . starting 2nd fork ");

    rstatHandle = DBOS.forkWorkflow(workflowId, 2, foptions);
    result = rstatHandle.getResult();
    assertEquals("hellohello", result);
    assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().status());
    assertNotEquals(rstatHandle.getWorkflowId(), workflowId);

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(3, impl.step3Count);
    assertEquals(3, impl.step4Count);
    assertEquals(3, impl.step5Count);

    logger.info("Second fork done . starting 3rd fork ");

    rstatHandle = DBOS.forkWorkflow(workflowId, 4, foptions);
    result = rstatHandle.getResult();
    assertEquals("hellohello", result);
    assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().status());
    assertNotEquals(rstatHandle.getWorkflowId(), workflowId);

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(3, impl.step3Count);
    assertEquals(3, impl.step4Count);
    assertEquals(4, impl.step5Count);
  }

  @Test
  public void testParentChildFork() throws SQLException {

    ForkServiceImpl impl = new ForkServiceImpl();

    ForkService forkService = DBOS.registerWorkflows(ForkService.class, impl);
    forkService.setForkService(forkService);

    DBOS.launch();

    String workflowId = "wfid1";
    WorkflowOptions options = new WorkflowOptions(workflowId);
    String result;
    try (var o = options.setContext()) {
      result = forkService.parentChild("hello");
    }

    assertEquals("hellohello", result);
    var handle = DBOS.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.child1Count);
    assertEquals(1, impl.child2Count);
    assertEquals(1, impl.step5Count);

    List<StepInfo> stepsRun0 = DBOS.listWorkflowSteps(workflowId);
    assertEquals(5, stepsRun0.size());

    logger.info("First execution done starting fork");

    ForkOptions foptions = new ForkOptions("f1");
    WorkflowHandle<String, SQLException> rstatHandle = DBOS.forkWorkflow(workflowId, 0, foptions);
    result = rstatHandle.getResult();
    assertEquals("hellohello", result);
    assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().status());
    assertEquals(rstatHandle.getWorkflowId(), "f1");

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(1, impl.child1Count);
    assertEquals(1, impl.child2Count);
    assertEquals(2, impl.step5Count);

    List<StepInfo> steps = DBOS.listWorkflowSteps(rstatHandle.getWorkflowId());
    assertEquals(5, steps.size());

    assertTrue(stepsRun0.get(2).childWorkflowId().equals(steps.get(2).childWorkflowId()));
    assertTrue(stepsRun0.get(3).childWorkflowId().equals(steps.get(3).childWorkflowId()));

    logger.info("First execution done starting 2nd fork");

    foptions = new ForkOptions("f2");
    rstatHandle = DBOS.forkWorkflow(workflowId, 3, foptions);
    result = rstatHandle.getResult();
    assertEquals("hellohello", result);
    assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().status());
    assertEquals(rstatHandle.getWorkflowId(), "f2");

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(1, impl.child1Count);
    assertEquals(1, impl.child2Count);
    assertEquals(3, impl.step5Count);

    steps = DBOS.listWorkflowSteps(rstatHandle.getWorkflowId());
    assertEquals(5, steps.size());

    logger.info(stepsRun0.get(2).childWorkflowId());
    logger.info(steps.get(2).childWorkflowId());
    assertTrue(stepsRun0.get(2).childWorkflowId().equals(steps.get(2).childWorkflowId()));
    assertTrue(stepsRun0.get(3).childWorkflowId().equals(steps.get(3).childWorkflowId()));

    logger.info("2nd execution done starting 3nd fork");

    foptions = new ForkOptions("f3");
    rstatHandle = DBOS.forkWorkflow(workflowId, 4, foptions);
    result = rstatHandle.getResult();
    assertEquals("hellohello", result);
    assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().status());
    assertEquals(rstatHandle.getWorkflowId(), "f3");

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(1, impl.child1Count);
    assertEquals(1, impl.child2Count);
    assertEquals(4, impl.step5Count);

    steps = DBOS.listWorkflowSteps(rstatHandle.getWorkflowId());
    assertEquals(5, steps.size());

    assertTrue(stepsRun0.get(2).childWorkflowId().equals(steps.get(2).childWorkflowId()));
    assertTrue(stepsRun0.get(3).childWorkflowId().equals(steps.get(3).childWorkflowId()));

    logger.info("First execution done starting 2nd fork");
  }

  @Test
  public void testParentChildAsyncFork() throws SQLException {

    ForkServiceImpl impl = new ForkServiceImpl();

    ForkService forkService = DBOS.registerWorkflows(ForkService.class, impl);
    forkService.setForkService(forkService);

    DBOS.launch();

    String workflowId = "wfid1";
    WorkflowOptions options = new WorkflowOptions(workflowId);
    String result;
    try (var o = options.setContext()) {
      result = forkService.parentChildAsync("hello");
    }

    assertEquals("hellohello", result);
    var handle = DBOS.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.child1Count);
    assertEquals(1, impl.child2Count);
    assertEquals(1, impl.step5Count);

    List<StepInfo> stepsRun0 = DBOS.listWorkflowSteps(workflowId);
    assertEquals(5, stepsRun0.size());

    logger.info("First execution done starting fork");

    ForkOptions foptions = new ForkOptions();
    WorkflowHandle<?, SQLException> rstatHandle = DBOS.forkWorkflow(workflowId, 3, foptions);
    result = (String) rstatHandle.getResult();

    assertEquals("hellohello", result);
    assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().status());
    assertNotEquals(rstatHandle.getWorkflowId(), workflowId);

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.child1Count);
    assertEquals(1, impl.child2Count); // 1 because the wf already executed even if we
    // did not copy the step
    assertEquals(2, impl.step5Count);

    List<StepInfo> steps = DBOS.listWorkflowSteps(rstatHandle.getWorkflowId());
    assertEquals(5, steps.size());

    assertTrue(stepsRun0.get(2).childWorkflowId().equals(steps.get(2).childWorkflowId()));
    assertTrue(stepsRun0.get(3).childWorkflowId().equals(steps.get(3).childWorkflowId()));
  }

  @Test
  void garbageCollection() throws Exception {
    int numWorkflows = 10;

    GCServiceImpl impl = new GCServiceImpl();
    GCService gcService = DBOS.registerWorkflows(GCService.class, impl);
    gcService.setGCService(gcService);

    DBOS.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase();

    // Start one blocked workflow and 10 normal workflows
    WorkflowHandle<String, ?> handle = DBOS.startWorkflow(() -> gcService.gcBlockedWorkflow());
    for (int i = 0; i < numWorkflows; i++) {
      int result = gcService.testWorkflow(i);
      assertEquals(i, result);
    }

    // Garbage collect all but one completed workflow
    List<WorkflowStatus> statusList = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(11, statusList.size());
    systemDatabase.garbageCollect(null, 1L);
    statusList = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(2, statusList.size());
    assertEquals(handle.getWorkflowId(), statusList.get(0).workflowId());

    // Garbage collect all completed workflows
    systemDatabase.garbageCollect(System.currentTimeMillis(), null);
    statusList = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, statusList.size());
    assertEquals(handle.getWorkflowId(), statusList.get(0).workflowId());

    // Finish the blocked workflow, garbage collect everything
    impl.gcLatch.countDown();
    assertEquals(handle.getWorkflowId(), handle.getResult());
    systemDatabase.garbageCollect(System.currentTimeMillis(), null);
    statusList = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(0, statusList.size());

    // Verify GC runs without errors on an empty table
    systemDatabase.garbageCollect(null, 1L);

    // Run workflows, wait, run them again
    for (int i = 0; i < numWorkflows; i++) {
      int result = gcService.testWorkflow(i);
      assertEquals(i, result);
    }

    Thread.sleep(1000L);

    for (int i = 0; i < numWorkflows; i++) {
      int result = gcService.testWorkflow(i);
      assertEquals(i, result);
    }

    // GC the first half, verify only half were GC'ed
    systemDatabase.garbageCollect(System.currentTimeMillis() - 1000, null);
    statusList = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(numWorkflows, statusList.size());
  }

  @Test
  void globalTimeout() throws Exception {
    int numWorkflows = 10;

    GCServiceImpl impl = new GCServiceImpl();
    GCService gcService = DBOS.registerWorkflows(GCService.class, impl);
    gcService.setGCService(gcService);

    DBOS.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor();

    List<WorkflowHandle<String, ?>> handles = new ArrayList<>();
    for (int i = 0; i < numWorkflows; i++) {
      handles.add(DBOS.startWorkflow(() -> gcService.timeoutBlockedWorkflow()));
    }

    Thread.sleep(1000L);

    // Wait one second, start one final workflow, then timeout all workflows started
    // more than one second ago
    WorkflowHandle<String, ?> finalHandle =
        DBOS.startWorkflow(() -> gcService.timeoutBlockedWorkflow());

    dbosExecutor.globalTimeout(System.currentTimeMillis() - 1000);
    for (var handle : handles) {
      assertEquals(WorkflowState.CANCELLED.toString(), handle.getStatus().status());
    }
    impl.timeoutLatch.countDown();
    assertEquals(finalHandle.getWorkflowId(), finalHandle.getResult());
  }
}
