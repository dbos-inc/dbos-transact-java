package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.NonExistentWorkflowException;
import dev.dbos.transact.exceptions.WorkflowFunctionNotFoundException;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.*;

import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DBOSExecutorTest {

  private static DBOSConfig dbosConfig;
  private DBOS dbos;
  private static DataSource dataSource;

  @BeforeAll
  public static void onetimeBefore() throws SQLException {
    DBOSExecutorTest.dbosConfig =
        new DBOSConfig.Builder()
            .name("systemdbtest")
            .dbHost("localhost")
            .dbPort(5432)
            .dbUser("postgres")
            .sysDbName("dbos_java_sys")
            .maximumPoolSize(2)
            .build();
  }

  @BeforeEach
  void setUp() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    DBOSExecutorTest.dataSource = SystemDatabase.createDataSource(dbosConfig);

    dbos = DBOS.initialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dbos.shutdown();
  }

  @Test
  void executeWorkflowById() throws Exception {

    ExecutingService executingService =
        dbos.<ExecutingService>Workflow()
            .interfaceClass(ExecutingService.class)
            .implementation(new ExecutingServiceImpl())
            .build();

    dbos.launch();

    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    String result = null;

    String wfid = "wf-123";
    try (SetWorkflowID id = new SetWorkflowID(wfid)) {
      result = executingService.workflowMethod("test-item");
    }

    assertEquals("test-itemtest-item", result);

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).getStatus(), WorkflowState.SUCCESS.name());

    DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());

    var handle = dbosExecutor.executeWorkflowById(wfid);

    result = (String) handle.getResult();
    assertEquals("test-itemtest-item", result);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());

    wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).getStatus(), WorkflowState.SUCCESS.name());
  }

  @Test
  void executeWorkflowByIdNonExistent() throws Exception {

    ExecutingService executingService =
        dbos.<ExecutingService>Workflow()
            .interfaceClass(ExecutingService.class)
            .implementation(new ExecutingServiceImpl())
            .build();

    dbos.launch();

    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    String result = null;

    String wfid = "wf-123";
    try (SetWorkflowID id = new SetWorkflowID(wfid)) {
      result = executingService.workflowMethod("test-item");
    }

    assertEquals("test-itemtest-item", result);

    List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).getStatus(), WorkflowState.SUCCESS.name());

    boolean error = false;
    try {
      var handle = dbosExecutor.executeWorkflowById("wf-124");
    } catch (Exception e) {
      error = true;
      assert e instanceof NonExistentWorkflowException
          : "Expected NonExistentWorkflowException but got " + e.getClass().getName();
    }

    assertTrue(error);
  }

  @Test
  void workflowFunctionNotfound() throws Exception {

    ExecutingService executingService =
        dbos.<ExecutingService>Workflow()
            .interfaceClass(ExecutingService.class)
            .implementation(new ExecutingServiceImpl())
            .build();
    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    String result = null;

    String wfid = "wf-123";
    try (SetWorkflowID id = new SetWorkflowID(wfid)) {
      result = executingService.workflowMethod("test-item");
    }

    assertEquals("test-itemtest-item", result);

    List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).getStatus(), WorkflowState.SUCCESS.name());

    dbos.shutdown();
    DBOSTestAccess.clearRegistry(dbos); // clear out the registry
    dbos.launch(); // restart dbos
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    boolean error = false;
    try {
      dbosExecutor.executeWorkflowById(wfid);
    } catch (Exception e) {
      error = true;
      assert e instanceof WorkflowFunctionNotFoundException
          : "Expected WorkflowFunctionNotfoundException but got " + e.getClass().getName();
    }

    assertTrue(error);
  }

  @Test
  public void executeWithStep() throws Exception {

    ExecutingService executingService =
        dbos.<ExecutingService>Workflow()
            .interfaceClass(ExecutingService.class)
            .implementation(new ExecutingServiceImpl())
            .build();
    dbos.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    // Needed to call the step
    executingService.setExecutingService(executingService);

    String result = null;

    String wfid = "wf-123";
    try (SetWorkflowID id = new SetWorkflowID(wfid)) {
      result = executingService.workflowMethodWithStep("test-item");
    }

    assertEquals("test-itemstepOnestepTwo", result);

    List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).getStatus(), WorkflowState.SUCCESS.name());

    List<StepInfo> steps = systemDatabase.listWorkflowSteps(wfid);
    assertEquals(2, steps.size());

    DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
    DBUtils.deleteAllStepOutputs(dataSource, wfid);
    steps = systemDatabase.listWorkflowSteps(wfid);
    assertEquals(0, steps.size());

    WorkflowHandle<String, ?> handle = dbosExecutor.executeWorkflowById(wfid);

    result = handle.getResult();
    assertEquals("test-itemstepOnestepTwo", result);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());

    wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).getStatus(), WorkflowState.SUCCESS.name());
    steps = systemDatabase.listWorkflowSteps(wfid);
    assertEquals(2, steps.size());
  }

  @Test
  public void ReExecuteWithStepTwoOnly() throws Exception {

    ExecutingService executingService =
        dbos.<ExecutingService>Workflow()
            .interfaceClass(ExecutingService.class)
            .implementation(new ExecutingServiceImpl())
            .build();
    dbos.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    // Needed to call the step
    executingService.setExecutingService(executingService);

    String result = null;

    String wfid = "wf-123";
    try (SetWorkflowID id = new SetWorkflowID(wfid)) {
      result = executingService.workflowMethodWithStep("test-item");
    }

    assertEquals("test-itemstepOnestepTwo", result);
    assertEquals(1, ExecutingServiceImpl.step1Count);
    assertEquals(1, ExecutingServiceImpl.step2Count);

    List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).getStatus(), WorkflowState.SUCCESS.name());

    List<StepInfo> steps = systemDatabase.listWorkflowSteps(wfid);
    assertEquals(2, steps.size());

    DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
    DBUtils.deleteStepOutput(dataSource, wfid, 1);
    steps = systemDatabase.listWorkflowSteps(wfid);
    assertEquals(1, steps.size());

    WorkflowHandle<String, ?> handle = dbosExecutor.executeWorkflowById(wfid);

    result = handle.getResult();
    assertEquals("test-itemstepOnestepTwo", result);
    assertEquals(1, ExecutingServiceImpl.step1Count);
    assertEquals(2, ExecutingServiceImpl.step2Count);

    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());

    wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.get(0).getStatus(), WorkflowState.SUCCESS.name());
    steps = systemDatabase.listWorkflowSteps(wfid);
    assertEquals(2, steps.size());
  }

  @Test
  public void sleep() throws SQLException {

    ExecutingService executingService =
        dbos.<ExecutingService>Workflow()
            .interfaceClass(ExecutingService.class)
            .implementation(new ExecutingServiceImpl())
            .build();
    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    // Needed to call the step
    executingService.setExecutingService(executingService);

    String wfid = "wf-123";
    long start = System.currentTimeMillis();
    try (SetWorkflowID id = new SetWorkflowID(wfid)) {
      executingService.sleepingWorkflow(2);
    }

    long duration = System.currentTimeMillis() - start;
    System.out.println("Duration " + duration);
    assertTrue(duration >= 2000);
    assertTrue(duration < 2200);

    List<StepInfo> steps = systemDatabase.listWorkflowSteps(wfid);

    assertEquals("DBOS.sleep", steps.get(0).getFunctionName());
  }

  @Test
  public void sleepRecovery() throws Exception {

    ExecutingService executingService =
        dbos.<ExecutingService>Workflow()
            .interfaceClass(ExecutingService.class)
            .implementation(new ExecutingServiceImpl())
            .build();
    dbos.launch();
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    // Needed to call the step
    executingService.setExecutingService(executingService);

    String wfid = "wf-123";
    try (SetWorkflowID id = new SetWorkflowID(wfid)) {
      executingService.sleepingWorkflow(.002f);
    }

    List<StepInfo> steps = systemDatabase.listWorkflowSteps(wfid);

    assertEquals("DBOS.sleep", steps.get(0).getFunctionName());

    // let us set the state to PENDING and increase the sleep time
    DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
    long currenttime = System.currentTimeMillis();
    double newEndtime = (currenttime + 2000) / 1000;

    String endTimeAsJson = JSONUtil.serialize(newEndtime);

    DBUtils.updateStepEndTime(dataSource, wfid, steps.get(0).getFunctionId(), endTimeAsJson);

    long starttime = System.currentTimeMillis();
    var h = dbosExecutor.executeWorkflowById(wfid);
    h.getResult();

    long duration = System.currentTimeMillis() - starttime;
    assertTrue(duration >= 1000);
  }
}
