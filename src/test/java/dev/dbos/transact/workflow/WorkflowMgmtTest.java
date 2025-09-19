package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.AwaitedWorkflowCancelledException;
import dev.dbos.transact.exceptions.NonExistentWorkflowException;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Disabled
public class WorkflowMgmtTest {

  Logger logger = LoggerFactory.getLogger(WorkflowMgmtTest.class);

  private static DBOSConfig dbosConfig;
  private DBOS dbos;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    WorkflowMgmtTest.dbosConfig =
        new DBOSConfig.Builder()
            .name("systemdbtest")
            .url("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .dbUser("postgres")
            .dbHost("localhost")
            .dbPort(5432)
            .sysDbName("dbos_java_sys")
            .maximumPoolSize(2)
            .build();
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);

    dbos = DBOS.initialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dbos.shutdown();
  }

  @Test
  public void asyncCancelResumeTest() throws Exception {

    CountDownLatch mainLatch = new CountDownLatch(1);
    CountDownLatch workLatch = new CountDownLatch(1);

    MgmtService mgmtService =
        dbos.<MgmtService>Workflow()
            .interfaceClass(MgmtService.class)
            .implementation(new MgmtServiceImpl(mainLatch, workLatch))
            .build();
    mgmtService.setMgmtService(mgmtService);

    dbos.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    String workflowId = "wfid1";
    var options = new StartWorkflowOptions(workflowId);
    int result;
    WorkflowHandle<Integer> h = dbos.startWorkflow(() -> mgmtService.simpleWorkflow(23), options);

    mainLatch.await();
    dbos.cancelWorkflow(workflowId);
    workLatch.countDown();

    assertEquals(1, mgmtService.getStepsExecuted());
    // WorkflowHandle h = dbosExecutor.retrieveWorkflow(workflowId) ;
    assertEquals(WorkflowState.CANCELLED.name(), h.getStatus().getStatus());

    WorkflowHandle<Integer> handle = dbos.resumeWorkflow(workflowId);

    result = handle.getResult();
    assertEquals(23, result);
    assertEquals(3, mgmtService.getStepsExecuted());

    // resume again

    handle = dbos.resumeWorkflow(workflowId);

    result = handle.getResult();
    assertEquals(23, result);
    assertEquals(3, mgmtService.getStepsExecuted());
    h = dbosExecutor.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), h.getStatus().getStatus());

    logger.info("Test completed");
  }

  @Test
  public void queuedCancelResumeTest() throws Exception {

    CountDownLatch mainLatch = new CountDownLatch(1);
    CountDownLatch workLatch = new CountDownLatch(1);

    MgmtService mgmtService =
        dbos.<MgmtService>Workflow()
            .interfaceClass(MgmtService.class)
            .implementation(new MgmtServiceImpl(mainLatch, workLatch))
            .build();
    mgmtService.setMgmtService(mgmtService);

    Queue myqueue = dbos.Queue("myqueue").build();

    dbos.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    String workflowId = "wfid1";
    var options = new StartWorkflowOptions(workflowId).withQueue(myqueue);
    int result;
    dbos.startWorkflow(() -> mgmtService.simpleWorkflow(23), options);

    mainLatch.await();
    dbos.cancelWorkflow(workflowId);
    workLatch.countDown();

    assertEquals(1, mgmtService.getStepsExecuted());
    WorkflowHandle h = dbosExecutor.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.CANCELLED.name(), h.getStatus().getStatus());

    WorkflowHandle<Integer> handle = dbos.resumeWorkflow(workflowId);

    result = handle.getResult();
    assertEquals(23, result);
    assertEquals(3, mgmtService.getStepsExecuted());

    // resume again

    handle = dbos.resumeWorkflow(workflowId);

    result = handle.getResult();
    assertEquals(23, result);
    assertEquals(3, mgmtService.getStepsExecuted());
    h = dbosExecutor.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), h.getStatus().getStatus());

    logger.info("Test completed");
  }

  @Test
  public void syncCancelResumeTest() throws Exception {

    CountDownLatch mainLatch = new CountDownLatch(1);
    CountDownLatch workLatch = new CountDownLatch(1);

    MgmtService mgmtService =
        dbos.<MgmtService>Workflow()
            .interfaceClass(MgmtService.class)
            .implementation(new MgmtServiceImpl(mainLatch, workLatch))
            .build();
    mgmtService.setMgmtService(mgmtService);

    dbos.launch();

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
            assertTrue(t instanceof AwaitedWorkflowCancelledException);
          }

          assertEquals(1, mgmtService.getStepsExecuted());
          testLatch.countDown();
        });

    e.submit(
        () -> {
          try {
            mainLatch.await();
            dbos.cancelWorkflow(workflowId);
            workLatch.countDown();
            testLatch.countDown();

          } catch (InterruptedException ie) {
            logger.error("syncCancelResumeTest interrupted", ie);
          }
        });

    testLatch.await();

    WorkflowHandle<Integer> handle = dbos.resumeWorkflow(workflowId);

    int result = handle.getResult();
    assertEquals(23, result);
    assertEquals(3, mgmtService.getStepsExecuted());

    // resume again

    handle = dbos.resumeWorkflow(workflowId);

    result = (Integer) handle.getResult();
    assertEquals(23, result);
    assertEquals(3, mgmtService.getStepsExecuted());

    logger.info("Test completed");
  }

  @Test
  public void forkNonExistent() {

    dbos.launch();

    try {
      ForkOptions options = new ForkOptions.Builder().build();
      WorkflowHandle<String> rstatHandle = dbos.forkWorkflow("12345", 2, options);
      fail("An exception should have been thrown");
    } catch (Exception t) {
      logger.info(t.getClass().getName());
      assertTrue(t instanceof NonExistentWorkflowException);
    }
  }

  @Test
  public void testFork() throws SQLException {

    ForkServiceImpl impl = new ForkServiceImpl();

    ForkService forkService =
        dbos.<ForkService>Workflow().interfaceClass(ForkService.class).implementation(impl).build();
    forkService.setForkService(forkService);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    String workflowId = "wfid1";
    WorkflowOptions options = new WorkflowOptions(workflowId);
    String result;
    try (var o = options.setContext()) {
      result = forkService.simpleWorkflow("hello");
    }

    assertEquals("hellohello", result);
    WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.step3Count);
    assertEquals(1, impl.step4Count);
    assertEquals(1, impl.step5Count);

    logger.info("First execution done starting fork");

    ForkOptions foptions = new ForkOptions.Builder().build();
    WorkflowHandle<String> rstatHandle = dbos.forkWorkflow(workflowId, 0, foptions);
    result = rstatHandle.getResult();
    assertEquals("hellohello", result);
    assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().getStatus());
    assertTrue(rstatHandle.getWorkflowId() != workflowId);

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(2, impl.step3Count);
    assertEquals(2, impl.step4Count);
    assertEquals(2, impl.step5Count);

    List<StepInfo> steps = systemDatabase.listWorkflowSteps(rstatHandle.getWorkflowId());
    assertEquals(5, steps.size());

    logger.info("first fork done . starting 2nd fork ");

    rstatHandle = dbos.forkWorkflow(workflowId, 2, foptions);
    result = rstatHandle.getResult();
    assertEquals("hellohello", result);
    assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().getStatus());
    assertTrue(rstatHandle.getWorkflowId() != workflowId);

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(3, impl.step3Count);
    assertEquals(3, impl.step4Count);
    assertEquals(3, impl.step5Count);

    logger.info("Second fork done . starting 3rd fork ");

    rstatHandle = dbos.forkWorkflow(workflowId, 4, foptions);
    result = rstatHandle.getResult();
    assertEquals("hellohello", result);
    assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().getStatus());
    assertTrue(rstatHandle.getWorkflowId() != workflowId);

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(3, impl.step3Count);
    assertEquals(3, impl.step4Count);
    assertEquals(4, impl.step5Count);
  }

  @Test
  public void testParentChildFork() throws SQLException {

    ForkServiceImpl impl = new ForkServiceImpl();

    ForkService forkService =
        dbos.<ForkService>Workflow().interfaceClass(ForkService.class).implementation(impl).build();
    forkService.setForkService(forkService);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    String workflowId = "wfid1";
    WorkflowOptions options = new WorkflowOptions(workflowId);
    String result;
    try (var o = options.setContext()) {
      result = forkService.parentChild("hello");
    }

    assertEquals("hellohello", result);
    WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.child1Count);
    assertEquals(1, impl.child2Count);
    assertEquals(1, impl.step5Count);

    List<StepInfo> stepsRun0 = systemDatabase.listWorkflowSteps(workflowId);
    assertEquals(5, stepsRun0.size());

    logger.info("First execution done starting fork");

    ForkOptions foptions = new ForkOptions.Builder().forkedWorkflowId("f1").build();
    WorkflowHandle<String> rstatHandle = dbos.forkWorkflow(workflowId, 0, foptions);
    result = rstatHandle.getResult();
    assertEquals("hellohello", result);
    assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().getStatus());
    assertEquals(rstatHandle.getWorkflowId(), "f1");

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(1, impl.child1Count);
    assertEquals(1, impl.child2Count);
    assertEquals(2, impl.step5Count);

    List<StepInfo> steps = systemDatabase.listWorkflowSteps(rstatHandle.getWorkflowId());
    assertEquals(5, steps.size());

    assertTrue(stepsRun0.get(2).getChildWorkflowId().equals(steps.get(2).getChildWorkflowId()));
    assertTrue(stepsRun0.get(3).getChildWorkflowId().equals(steps.get(3).getChildWorkflowId()));

    logger.info("First execution done starting 2nd fork");

    foptions = new ForkOptions.Builder().forkedWorkflowId("f2").build();
    rstatHandle = dbos.forkWorkflow(workflowId, 3, foptions);
    result = rstatHandle.getResult();
    assertEquals("hellohello", result);
    assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().getStatus());
    assertEquals(rstatHandle.getWorkflowId(), "f2");

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(1, impl.child1Count);
    assertEquals(1, impl.child2Count);
    assertEquals(3, impl.step5Count);

    steps = systemDatabase.listWorkflowSteps(rstatHandle.getWorkflowId());
    assertEquals(5, steps.size());

    logger.info(stepsRun0.get(2).getChildWorkflowId());
    logger.info(steps.get(2).getChildWorkflowId());
    assertTrue(stepsRun0.get(2).getChildWorkflowId().equals(steps.get(2).getChildWorkflowId()));
    assertTrue(stepsRun0.get(3).getChildWorkflowId().equals(steps.get(3).getChildWorkflowId()));

    logger.info("2nd execution done starting 3nd fork");

    foptions = new ForkOptions.Builder().forkedWorkflowId("f3").build();
    rstatHandle = dbos.forkWorkflow(workflowId, 4, foptions);
    result = rstatHandle.getResult();
    assertEquals("hellohello", result);
    assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().getStatus());
    assertEquals(rstatHandle.getWorkflowId(), "f3");

    assertEquals(2, impl.step1Count);
    assertEquals(2, impl.step2Count);
    assertEquals(1, impl.child1Count);
    assertEquals(1, impl.child2Count);
    assertEquals(4, impl.step5Count);

    steps = systemDatabase.listWorkflowSteps(rstatHandle.getWorkflowId());
    assertEquals(5, steps.size());

    assertTrue(stepsRun0.get(2).getChildWorkflowId().equals(steps.get(2).getChildWorkflowId()));
    assertTrue(stepsRun0.get(3).getChildWorkflowId().equals(steps.get(3).getChildWorkflowId()));

    logger.info("First execution done starting 2nd fork");
  }

  @Test
  public void testParentChildAsyncFork() throws SQLException {

    ForkServiceImpl impl = new ForkServiceImpl();

    ForkService forkService =
        dbos.<ForkService>Workflow().interfaceClass(ForkService.class).implementation(impl).build();
    forkService.setForkService(forkService);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    String workflowId = "wfid1";
    WorkflowOptions options = new WorkflowOptions(workflowId);
    String result;
    try (var o = options.setContext()) {
      result = forkService.parentChildAsync("hello");
    }

    assertEquals("hellohello", result);
    WorkflowHandle<?> handle = dbosExecutor.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.child1Count);
    assertEquals(1, impl.child2Count);
    assertEquals(1, impl.step5Count);

    List<StepInfo> stepsRun0 = systemDatabase.listWorkflowSteps(workflowId);
    assertEquals(5, stepsRun0.size());

    logger.info("First execution done starting fork");

    ForkOptions foptions = new ForkOptions.Builder().build();
    WorkflowHandle<?> rstatHandle = dbos.forkWorkflow(workflowId, 3, foptions);
    result = (String) rstatHandle.getResult();

    assertEquals("hellohello", result);
    assertEquals(WorkflowState.SUCCESS.name(), rstatHandle.getStatus().getStatus());
    assertTrue(rstatHandle.getWorkflowId() != workflowId);

    assertEquals(1, impl.step1Count);
    assertEquals(1, impl.step2Count);
    assertEquals(1, impl.child1Count);
    assertEquals(1, impl.child2Count); // 1 because the wf already executed even if we
    // did not copy the step
    assertEquals(2, impl.step5Count);

    List<StepInfo> steps = systemDatabase.listWorkflowSteps(rstatHandle.getWorkflowId());
    assertEquals(5, steps.size());

    assertTrue(stepsRun0.get(2).getChildWorkflowId().equals(steps.get(2).getChildWorkflowId()));
    assertTrue(stepsRun0.get(3).getChildWorkflowId().equals(steps.get(3).getChildWorkflowId()));
  }

  @Test
  void garbageCollection() throws Exception {
    int numWorkflows = 10;

    GCServiceImpl impl = new GCServiceImpl(dbos);
    GCService gcService =
        dbos.<GCService>Workflow().interfaceClass(GCService.class).implementation(impl).build();
    gcService.setGCService(gcService);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    // Start one blocked workflow and 10 normal workflows
    WorkflowHandle<String> handle = dbos.startWorkflow(() -> gcService.gcBlockedWorkflow());
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
    assertEquals(handle.getWorkflowId(), statusList.get(0).getWorkflowId());

    // Garbage collect all completed workflows
    systemDatabase.garbageCollect(System.currentTimeMillis(), null);
    statusList = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, statusList.size());
    assertEquals(handle.getWorkflowId(), statusList.get(0).getWorkflowId());

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

    GCServiceImpl impl = new GCServiceImpl(dbos);
    GCService gcService =
        dbos.<GCService>Workflow().interfaceClass(GCService.class).implementation(impl).build();
    gcService.setGCService(gcService);

    dbos.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    List<WorkflowHandle<String>> handles = new ArrayList<>();
    for (int i = 0; i < numWorkflows; i++) {
      handles.add(dbos.startWorkflow(() -> gcService.timeoutBlockedWorkflow()));
    }

    Thread.sleep(1000L);

    // Wait one second, start one final workflow, then timeout all workflows started
    // more than one second ago
    WorkflowHandle<String> finalHandle =
        dbos.startWorkflow(() -> gcService.timeoutBlockedWorkflow());

    dbosExecutor.globalTimeout(System.currentTimeMillis() - 1000);
    for (WorkflowHandle<?> handle : handles) {
      assertEquals(WorkflowState.CANCELLED.toString(), handle.getStatus().getStatus());
    }
    impl.timeoutLatch.countDown();
    assertEquals(finalHandle.getWorkflowId(), finalHandle.getResult());
  }
}
