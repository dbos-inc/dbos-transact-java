package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.exceptions.DBOSWorkflowFunctionNotFoundException;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.*;

import java.util.List;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.junitpioneer.jupiter.RetryingTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DBOSExecutorTest {

  private static final Logger logger = LoggerFactory.getLogger(DBOSExecutorTest.class);

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose HikariDataSource dataSource;

  @BeforeEach
  void setUp() {
    dbosConfig = pgContainer.dbosConfig();
    dataSource = pgContainer.dataSource();
  }

  private ExecutingService register(DBOS dbos) {
    var impl = new ExecutingServiceImpl(dbos);
    var service = dbos.registerProxy(ExecutingService.class, impl);
    impl.setSelf(service);
    return service;
  }

  private static void awaitStepCount(DBOS dbos, String wfid, int expected, int timeoutMs)
      throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMs;
    while (System.currentTimeMillis() < deadline) {
      if (dbos.listWorkflowSteps(wfid).size() == expected) return;
      Thread.sleep(50);
    }
    assertEquals(expected, dbos.listWorkflowSteps(wfid).size());
  }

  /**
   * The default configuration has no custom serializer, so nothing may name one directly. The
   * debouncer records a terminal ERROR through here when it cannot start the user workflow, and
   * throwing instead leaves the caller's handle polling a row that never appears.
   */
  @Test
  public void recordsErrorForUnstartedWorkflowWithoutACustomSerializer() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      dbos.launch();
      assertNull(dbosConfig.serializer());

      var workflowId = UUID.randomUUID().toString();
      DBOSTestAccess.getDbosExecutor(dbos)
          .recordErrorForUnstartedWorkflow(
              workflowId,
              "missingWorkflow",
              "MissingService",
              null,
              new Object[] {"arg"},
              new DBOSWorkflowFunctionNotFoundException(workflowId, "missingWorkflow"));

      var status = dbos.retrieveWorkflow(workflowId).getStatus();
      assertEquals(WorkflowState.ERROR, status.status());
      assertEquals(SerializationUtil.NATIVE, status.serialization());
    }
  }

  @Test
  @EnabledForJreRange(min = JRE.JAVA_21)
  public void virtualThreadPoolJava21() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      dbos.launch();
      assertFalse(DBOSTestAccess.getDbosExecutor(dbos).usingThreadPoolExecutor());
    }
  }

  @Test
  public void virtualThreadPoolJDK21OrLater() throws Exception {
    int jdk = Runtime.version().feature();
    Assumptions.assumeTrue(jdk >= 21, "Skipping: requires JDK 21 or later, got " + jdk);
    try (var dbos = new DBOS(dbosConfig)) {
      dbos.launch();
      assertFalse(DBOSTestAccess.getDbosExecutor(dbos).usingThreadPoolExecutor());
    }
  }

  @Test
  @DisabledForJreRange(min = JRE.JAVA_21)
  public void threadPoolJava17() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      dbos.launch();
      assertTrue(DBOSTestAccess.getDbosExecutor(dbos).usingThreadPoolExecutor());
    }
  }

  @Test
  public void threadPoolJDK20OrEarlier() throws Exception {
    int jdk = Runtime.version().feature();
    Assumptions.assumeTrue(jdk < 21, "Skipping: requires JDK 20 or earlier, got " + jdk);
    try (var dbos = new DBOS(dbosConfig)) {
      dbos.launch();
      assertTrue(DBOSTestAccess.getDbosExecutor(dbos).usingThreadPoolExecutor());
    }
  }

  @Test
  void executeWorkflowById() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      ExecutingService executingService = register(dbos);
      dbos.launch();

      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

      String result = null;

      String wfid = "wf-123";
      try (var _i = new WorkflowOptions(wfid).setContext()) {
        result = executingService.workflowMethod("test-item");
      }

      assertEquals("test-itemtest-item", result);

      List<WorkflowStatus> wfs = dbos.listWorkflows(null);
      assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());

      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());

      var handle = dbosExecutor.executeWorkflowById(wfid);

      result = (String) handle.getResult();
      assertEquals("test-itemtest-item", result);
      assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());

      wfs = dbos.listWorkflows(null);
      assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());
    }
  }

  @Test
  void executeWorkflowByIdNonExistent() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      register(dbos);
      dbos.launch();

      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

      boolean error = false;
      try {
        dbosExecutor.executeWorkflowById("wf-124");
      } catch (Exception e) {
        error = true;
        assertTrue(
            e instanceof DBOSNonExistentWorkflowException,
            "Expected NonExistentWorkflowException but got " + e.getClass().getName());
      }

      assertTrue(error);
    }
  }

  @Test
  void workflowFunctionNotfound() throws Exception {
    String wfid = "wf-123";

    try (var dbos1 = new DBOS(dbosConfig)) {
      ExecutingService executingService = register(dbos1);
      dbos1.launch();

      String result = null;
      try (var id = new WorkflowOptions(wfid).setContext()) {
        result = executingService.workflowMethod("test-item");
      }
      assertEquals("test-itemtest-item", result);

      List<WorkflowStatus> wfs = dbos1.listWorkflows(null);
      assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());
    }

    // Re-launch without registering workflows
    try (var dbos2 = new DBOS(dbosConfig)) {
      dbos2.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos2);

      boolean error = false;
      try {
        dbosExecutor.executeWorkflowById(wfid);
      } catch (Exception e) {
        error = true;
        assertTrue(
            e instanceof DBOSWorkflowFunctionNotFoundException,
            "Expected WorkflowFunctionNotfoundException but got " + e.getClass().getName());
      }
      assertTrue(error);
    }
  }

  /**
   * Running a workflow method that was never registered reports it as not found. No workflow is
   * created, so there is no ID, and nothing is written.
   */
  @Test
  void runUnregisteredWorkflowIsNotFound() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      dbos.launch();

      var method = ExecutingServiceImpl.class.getMethod("workflowMethod", String.class);
      var wfTag = method.getAnnotation(Workflow.class);
      var e =
          assertThrows(
              DBOSWorkflowFunctionNotFoundException.class,
              () ->
                  dbos.integration()
                      .runWorkflow(
                          new ExecutingServiceImpl(dbos), null, method, new Object[] {"x"}, wfTag));
      assertNull(e.workflowId());
      assertTrue(e.workflowName().startsWith("workflowMethod/"), e.workflowName());
      assertEquals(
          "Workflow function %s does not exist.".formatted(e.workflowName()), e.getMessage());
      assertTrue(DBUtils.getWorkflowRows(dataSource).isEmpty());
    }
  }

  @Test
  public void executeWithStep() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      ExecutingService executingService = register(dbos);
      dbos.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

      String result = null;

      String wfid = "wf-123";
      try (var id = new WorkflowOptions(wfid).setContext()) {
        result = executingService.workflowMethodWithStep("test-item");
      }

      assertEquals("test-itemstepOnestepTwo", result);

      List<WorkflowStatus> wfs = dbos.listWorkflows(null);
      assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());

      List<StepInfo> steps = dbos.listWorkflowSteps(wfid);
      assertEquals(2, steps.size());

      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
      DBUtils.deleteAllStepOutputs(dataSource, wfid);
      awaitStepCount(dbos, wfid, 0, 2000);

      WorkflowHandle<String, ?> handle = dbosExecutor.executeWorkflowById(wfid);

      result = handle.getResult();
      assertEquals("test-itemstepOnestepTwo", result);
      assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());

      wfs = dbos.listWorkflows(null);
      assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());
      awaitStepCount(dbos, wfid, 2, 2000);
    }
  }

  @Test
  public void ReExecuteWithStepTwoOnly() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      var impl = new ExecutingServiceImpl(dbos);
      var proxy = dbos.registerProxy(ExecutingService.class, impl);
      impl.setSelf(proxy);

      dbos.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

      String result = null;

      String wfid = "wf-123";
      try (var id = new WorkflowOptions(wfid).setContext()) {
        result = proxy.workflowMethodWithStep("test-item");
      }

      assertEquals("test-itemstepOnestepTwo", result);
      assertEquals(1, impl.step1Count);
      assertEquals(1, impl.step2Count);

      List<WorkflowStatus> wfs = dbos.listWorkflows(null);
      assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());

      List<StepInfo> steps = dbos.listWorkflowSteps(wfid);
      assertEquals(2, steps.size());

      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
      DBUtils.deleteStepOutput(dataSource, wfid, 1);
      awaitStepCount(dbos, wfid, 1, 2000);

      WorkflowHandle<String, ?> handle = dbosExecutor.executeWorkflowById(wfid);

      result = handle.getResult();
      assertEquals("test-itemstepOnestepTwo", result);
      assertEquals(1, impl.step1Count);
      assertEquals(2, impl.step2Count);

      assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());

      wfs = dbos.listWorkflows(null);
      assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());
      awaitStepCount(dbos, wfid, 2, 2000);
    }
  }

  @Test
  public void sleep() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      ExecutingService executingService = register(dbos);
      dbos.launch();

      String wfid = "wf-123";
      long start = System.currentTimeMillis();
      try (var id = new WorkflowOptions(wfid).setContext()) {
        executingService.sleepingWorkflow(2);
      }

      long duration = System.currentTimeMillis() - start;
      logger.info("Duration {}", duration);
      assertTrue(duration >= 2000, "the sleep must not return early");
      // Wide enough to catch only a repeated sleep: a tighter bound measures the round trip
      // around the sleep rather than the sleep, and reddens on whichever CI job is slowest.
      assertTrue(duration < 4000, "the sleep must be served once, not repeated");

      List<StepInfo> steps = dbos.listWorkflowSteps(wfid);

      assertEquals("DBOS.sleep", steps.get(0).functionName());
    }
  }

  @RetryingTest(3)
  public void sleepRecovery() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      ExecutingService executingService = register(dbos);
      dbos.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

      String wfid = UUID.randomUUID().toString();
      try (var id = new WorkflowOptions(wfid).setContext()) {
        executingService.sleepingWorkflow(.002f);
      }

      List<StepInfo> steps = dbos.listWorkflowSteps(wfid);

      assertEquals("DBOS.sleep", steps.get(0).functionName());

      // let us set the state to PENDING and increase the sleep time
      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
      long currenttime = System.currentTimeMillis();
      long newEndtime = (currenttime + 2000);

      String endTimeAsJson =
          SerializationUtil.serializeValue(newEndtime, null, null).serializedValue();

      DBUtils.updateStepEndTime(dataSource, wfid, steps.get(0).functionId(), endTimeAsJson);

      long starttime = System.currentTimeMillis();
      var h = dbosExecutor.executeWorkflowById(wfid);
      h.getResult();

      long duration = System.currentTimeMillis() - starttime;
      assertTrue(duration >= 1000 && duration < 3500); // Relaxed for CI
    }
  }
}
