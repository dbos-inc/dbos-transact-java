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

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledForJreRange;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.condition.JRE;
import org.junitpioneer.jupiter.RetryingTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
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
    var service = dbos.registerWorkflows(ExecutingService.class, impl);
    impl.setSelf(service);
    return service;
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
  @EnabledIfEnvironmentVariable(named = "JDKVERSION", matches = "21|25")
  public void virtualThreadPoolJDK21And25() throws Exception {
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
  @EnabledIfEnvironmentVariable(named = "JDKVERSION", matches = "17|17\\..*")
  public void threadPoolJDK17() throws Exception {
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

      List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());
      assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());

      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());

      var handle = dbosExecutor.executeWorkflowById(wfid, true, false);

      result = (String) handle.getResult();
      assertEquals("test-itemtest-item", result);
      assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());

      wfs = dbos.listWorkflows(new ListWorkflowsInput());
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
        dbosExecutor.executeWorkflowById("wf-124", false, false);
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

      List<WorkflowStatus> wfs = dbos1.listWorkflows(new ListWorkflowsInput());
      assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());
    }

    // Re-launch without registering workflows
    try (var dbos2 = new DBOS(dbosConfig)) {
      dbos2.launch();
      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos2);

      boolean error = false;
      try {
        dbosExecutor.executeWorkflowById(wfid, false, false);
      } catch (Exception e) {
        error = true;
        assertTrue(
            e instanceof DBOSWorkflowFunctionNotFoundException,
            "Expected WorkflowFunctionNotfoundException but got " + e.getClass().getName());
      }
      assertTrue(error);
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

      List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());
      assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());

      List<StepInfo> steps = dbos.listWorkflowSteps(wfid);
      assertEquals(2, steps.size());

      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
      DBUtils.deleteAllStepOutputs(dataSource, wfid);
      steps = dbos.listWorkflowSteps(wfid);
      assertEquals(0, steps.size());

      WorkflowHandle<String, ?> handle = dbosExecutor.executeWorkflowById(wfid, true, false);

      result = handle.getResult();
      assertEquals("test-itemstepOnestepTwo", result);
      assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());

      wfs = dbos.listWorkflows(new ListWorkflowsInput());
      assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());
      steps = dbos.listWorkflowSteps(wfid);
      assertEquals(2, steps.size());
    }
  }

  @Test
  public void ReExecuteWithStepTwoOnly() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      var impl = new ExecutingServiceImpl(dbos);
      var proxy = dbos.registerWorkflows(ExecutingService.class, impl);
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

      List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());
      assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());

      List<StepInfo> steps = dbos.listWorkflowSteps(wfid);
      assertEquals(2, steps.size());

      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
      DBUtils.deleteStepOutput(dataSource, wfid, 1);
      steps = dbos.listWorkflowSteps(wfid);
      assertEquals(1, steps.size());

      WorkflowHandle<String, ?> handle = dbosExecutor.executeWorkflowById(wfid, true, false);

      result = handle.getResult();
      assertEquals("test-itemstepOnestepTwo", result);
      assertEquals(1, impl.step1Count);
      assertEquals(2, impl.step2Count);

      assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());

      wfs = dbos.listWorkflows(new ListWorkflowsInput());
      assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());
      steps = dbos.listWorkflowSteps(wfid);
      assertEquals(2, steps.size());
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
      assertTrue(duration >= 2000);
      assertTrue(duration < 2400); // Relaxed a bit for CI

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

      String wfid = "wf-123";
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
      var h = dbosExecutor.executeWorkflowById(wfid, true, false);
      h.getResult();

      long duration = System.currentTimeMillis() - starttime;
      assertTrue(duration >= 1000 && duration < 3500); // Relaxed for CI
    }
  }
}
