package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.RealBaseTest;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.exceptions.DBOSWorkflowFunctionNotFoundException;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.*;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = TimeUnit.MINUTES)
class DBOSExecutorTest extends RealBaseTest {

  private static DataSource dataSource;

  @BeforeEach
  void setUp() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    DBOSExecutorTest.dataSource = SystemDatabase.createDataSource(dbosConfig);

    DBOS.reinitialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    DBOS.shutdown();
  }

  @Test
  void executeWorkflowById() throws Exception {

    ExecutingService executingService =
        DBOS.registerWorkflows(ExecutingService.class, new ExecutingServiceImpl());
    DBOS.launch();

    var dbosExecutor = DBOSTestAccess.getDbosExecutor();

    String result = null;

    String wfid = "wf-123";
    try (var _i = new WorkflowOptions(wfid).setContext()) {
      result = executingService.workflowMethod("test-item");
    }

    assertEquals("test-itemtest-item", result);

    List<WorkflowStatus> wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).status(), WorkflowState.SUCCESS.name());

    DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());

    var handle = dbosExecutor.executeWorkflowById(wfid);

    result = (String) handle.getResult();
    assertEquals("test-itemtest-item", result);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());

    wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).status(), WorkflowState.SUCCESS.name());
  }

  @Test
  void executeWorkflowByIdNonExistent() throws Exception {

    ExecutingService executingService =
        DBOS.registerWorkflows(ExecutingService.class, new ExecutingServiceImpl());

    DBOS.launch();

    var dbosExecutor = DBOSTestAccess.getDbosExecutor();

    String result = null;

    String wfid = "wf-123";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      result = executingService.workflowMethod("test-item");
    }

    assertEquals("test-itemtest-item", result);

    List<WorkflowStatus> wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).status(), WorkflowState.SUCCESS.name());

    boolean error = false;
    try {
      dbosExecutor.executeWorkflowById("wf-124");
    } catch (Exception e) {
      error = true;
      assert e instanceof DBOSNonExistentWorkflowException
          : "Expected NonExistentWorkflowException but got " + e.getClass().getName();
    }

    assertTrue(error);
  }

  @Test
  void workflowFunctionNotfound() throws Exception {

    ExecutingService executingService =
        DBOS.registerWorkflows(ExecutingService.class, new ExecutingServiceImpl());
    DBOS.launch();

    String result = null;

    String wfid = "wf-123";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      result = executingService.workflowMethod("test-item");
    }

    assertEquals("test-itemtest-item", result);

    List<WorkflowStatus> wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).status(), WorkflowState.SUCCESS.name());

    DBOS.shutdown();
    DBOSTestAccess.clearRegistry(); // clear out the registry
    DBOS.launch(); // restart dbos
    var dbosExecutor = DBOSTestAccess.getDbosExecutor();

    boolean error = false;
    try {
      dbosExecutor.executeWorkflowById(wfid);
    } catch (Exception e) {
      error = true;
      assert e instanceof DBOSWorkflowFunctionNotFoundException
          : "Expected WorkflowFunctionNotfoundException but got " + e.getClass().getName();
    }

    assertTrue(error);
  }

  @Test
  public void executeWithStep() throws Exception {

    ExecutingService executingService =
        DBOS.registerWorkflows(ExecutingService.class, new ExecutingServiceImpl());
    DBOS.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor();

    // Needed to call the step
    executingService.setExecutingService(executingService);

    String result = null;

    String wfid = "wf-123";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      result = executingService.workflowMethodWithStep("test-item");
    }

    assertEquals("test-itemstepOnestepTwo", result);

    List<WorkflowStatus> wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).status(), WorkflowState.SUCCESS.name());

    List<StepInfo> steps = DBOS.listWorkflowSteps(wfid);
    assertEquals(2, steps.size());

    DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
    DBUtils.deleteAllStepOutputs(dataSource, wfid);
    steps = DBOS.listWorkflowSteps(wfid);
    assertEquals(0, steps.size());

    WorkflowHandle<String, ?> handle = dbosExecutor.executeWorkflowById(wfid);

    result = handle.getResult();
    assertEquals("test-itemstepOnestepTwo", result);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());

    wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).status(), WorkflowState.SUCCESS.name());
    steps = DBOS.listWorkflowSteps(wfid);
    assertEquals(2, steps.size());
  }

  @Test
  public void ReExecuteWithStepTwoOnly() throws Exception {

    ExecutingService executingService =
        DBOS.registerWorkflows(ExecutingService.class, new ExecutingServiceImpl());
    DBOS.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor();

    // Needed to call the step
    executingService.setExecutingService(executingService);

    String result = null;

    String wfid = "wf-123";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      result = executingService.workflowMethodWithStep("test-item");
    }

    assertEquals("test-itemstepOnestepTwo", result);
    assertEquals(1, ExecutingServiceImpl.step1Count);
    assertEquals(1, ExecutingServiceImpl.step2Count);

    List<WorkflowStatus> wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).status(), WorkflowState.SUCCESS.name());

    List<StepInfo> steps = DBOS.listWorkflowSteps(wfid);
    assertEquals(2, steps.size());

    DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
    DBUtils.deleteStepOutput(dataSource, wfid, 1);
    steps = DBOS.listWorkflowSteps(wfid);
    assertEquals(1, steps.size());

    WorkflowHandle<String, ?> handle = dbosExecutor.executeWorkflowById(wfid);

    result = handle.getResult();
    assertEquals("test-itemstepOnestepTwo", result);
    assertEquals(1, ExecutingServiceImpl.step1Count);
    assertEquals(2, ExecutingServiceImpl.step2Count);

    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());

    wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).status(), WorkflowState.SUCCESS.name());
    steps = DBOS.listWorkflowSteps(wfid);
    assertEquals(2, steps.size());
  }

  @Test
  public void sleep() throws SQLException {

    ExecutingService executingService =
        DBOS.registerWorkflows(ExecutingService.class, new ExecutingServiceImpl());
    DBOS.launch();

    // Needed to call the step
    executingService.setExecutingService(executingService);

    String wfid = "wf-123";
    long start = System.currentTimeMillis();
    try (var id = new WorkflowOptions(wfid).setContext()) {
      executingService.sleepingWorkflow(2);
    }

    long duration = System.currentTimeMillis() - start;
    System.out.println("Duration " + duration);
    assertTrue(duration >= 2000);
    assertTrue(duration < 2200);

    List<StepInfo> steps = DBOS.listWorkflowSteps(wfid);

    assertEquals("DBOS.sleep", steps.get(0).functionName());
  }

  @Test
  public void sleepRecovery() throws Exception {

    ExecutingService executingService =
        DBOS.registerWorkflows(ExecutingService.class, new ExecutingServiceImpl());
    DBOS.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor();

    // Needed to call the step
    executingService.setExecutingService(executingService);

    String wfid = "wf-123";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      executingService.sleepingWorkflow(.002f);
    }

    List<StepInfo> steps = DBOS.listWorkflowSteps(wfid);

    assertEquals("DBOS.sleep", steps.get(0).functionName());

    // let us set the state to PENDING and increase the sleep time
    DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
    long currenttime = System.currentTimeMillis();
    double newEndtime = (currenttime + 2000);

    String endTimeAsJson = JSONUtil.serialize(newEndtime);

    DBUtils.updateStepEndTime(dataSource, wfid, steps.get(0).functionId(), endTimeAsJson);

    long starttime = System.currentTimeMillis();
    var h = dbosExecutor.executeWorkflowById(wfid);
    h.getResult();

    long duration = System.currentTimeMillis() - starttime;
    assertTrue(duration >= 1000 && duration < 3000);
  }
}
