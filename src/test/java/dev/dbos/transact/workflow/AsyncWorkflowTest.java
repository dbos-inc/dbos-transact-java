package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.context.SetWorkflowOptions;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.DBOSAppException;
import dev.dbos.transact.exceptions.SerializableException;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.*;

public class AsyncWorkflowTest {

  private static DBOSConfig dbosConfig;
  private DBOS dbos;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    AsyncWorkflowTest.dbosConfig =
        new DBOSConfig.Builder()
            .name("systemdbtest")
            .dbHost("localhost")
            .dbPort(5432)
            .dbUser("postgres")
            .sysDbName("dbos_java_sys")
            .maximumPoolSize(2)
            .runAdminServer()
            .adminAwaitOnStart(false)
            .build();
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);

    dbos = DBOS.initialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws SQLException, Exception {
    dbos.shutdown();
  }

  @Test
  public void setWorkflowId() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .async()
            .build();

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    String wfid = "wf-123";
    try (SetWorkflowID id = new SetWorkflowID(wfid)) {
      simpleService.workWithString("test-item");
    }

    WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow(wfid);
    ;
    String result = (String) handle.getResult();
    assertEquals("Processed: test-item", result);
    assertEquals("wf-123", handle.getWorkflowId());
    assertEquals("SUCCESS", handle.getStatus().getStatus());

    List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).getName(), "workWithString");
    assertEquals("wf-123", wfs.get(0).getWorkflowId());
  }

  @Test
  public void sameWorkflowId() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .async()
            .build();

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    SimpleServiceImpl.executionCount = 0;

    String wfid = "wf-123";
    try (SetWorkflowID id = new SetWorkflowID(wfid)) {
      simpleService.workWithString("test-item");
    }

    WorkflowHandle<String> handle = dbosExecutor.retrieveWorkflow(wfid);
    String result = (String) handle.getResult();
    assertEquals("Processed: test-item", result);
    assertEquals("wf-123", handle.getWorkflowId());

    List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).getName(), "workWithString");
    assertEquals("wf-123", wfs.get(0).getWorkflowId());

    try (SetWorkflowID id = new SetWorkflowID("wf-123")) {
      simpleService.workWithString("test-item");
    }
    handle = dbosExecutor.retrieveWorkflow(wfid);
    result = (String) handle.getResult();
    assertEquals(1, SimpleServiceImpl.executionCount);
    // TODO fix deser has quotes assertEquals("Processed: test-item", result);
    assertEquals("wf-123", handle.getWorkflowId());

    wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals("wf-123", wfs.get(0).getWorkflowId());

    String wfid2 = "wf-124";
    try (SetWorkflowID id = new SetWorkflowID(wfid2)) {
      simpleService.workWithString("test-item");
    }

    handle = dbosExecutor.retrieveWorkflow(wfid2);
    result = (String) handle.getResult();
    assertEquals("wf-124", handle.getWorkflowId());

    assertEquals(2, SimpleServiceImpl.executionCount);
    wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(2, wfs.size());
    assertEquals("wf-124", wfs.get(1).getWorkflowId());
  }

  @Test
  public void workflowWithError() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    WorkflowHandle<Void> handle = null;
    String wfid = "abc";
    try (SetWorkflowID id = new SetWorkflowID(wfid)) {
      handle =
          dbos.startWorkflow(
              () -> {
                simpleService.workWithError();
                return null;
              });
    }

    try {
      handle.getResult();
    } catch (DBOSAppException e) {
      assertEquals("Exception of type java.lang.Exception", e.getMessage());
      SerializableException se = e.original;
      assertEquals("DBOS Test error", se.message);
    }
    List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).getName(), "workError");
    assertNotNull(wfs.get(0).getWorkflowId());
    assertEquals(wfs.get(0).getWorkflowId(), handle.getWorkflowId());
    assertEquals(WorkflowState.ERROR.name(), handle.getStatus().getStatus());
  }

  @Test
  public void childWorkflowWithoutSet() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    simpleService.setSimpleService(simpleService);

    WorkflowHandle<String> handle = null;
    WorkflowOptions options = new WorkflowOptions.Builder("wf-123456").build();
    try (SetWorkflowOptions o = new SetWorkflowOptions(options)) {
      handle = dbos.startWorkflow(() -> simpleService.parentWorkflowWithoutSet("123"));
    }

    System.out.println(handle.getResult());

    List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());

    assertEquals(2, wfs.size());
    assertEquals("wf-123456", wfs.get(0).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).getStatus());

    assertEquals("wf-123456-0", wfs.get(1).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).getStatus());

    List<StepInfo> steps = systemDatabase.listWorkflowSteps("wf-123456");
    assertEquals(1, steps.size());
    assertEquals("wf-123456-0", steps.get(0).getChildWorkflowId());
    assertEquals(0, steps.get(0).getFunctionId());
    assertEquals("childWorkflow", steps.get(0).getFunctionName());
  }

  @Test
  public void multipleChildren() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    simpleService.setSimpleService(simpleService);

    WorkflowOptions options = new WorkflowOptions.Builder("wf-123456").build();

    WorkflowHandle<String> handle = null;
    try (SetWorkflowOptions o = new SetWorkflowOptions(options)) {
      handle = dbos.startWorkflow(() -> simpleService.WorkflowWithMultipleChildren("123"));
    }

    assertEquals("123abcdefghi", handle.getResult());

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());

    assertEquals(4, wfs.size());
    assertEquals("wf-123456", wfs.get(0).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).getStatus());

    assertEquals("child1", wfs.get(1).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).getStatus());

    assertEquals("child2", wfs.get(2).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(2).getStatus());

    assertEquals("child3", wfs.get(3).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(3).getStatus());

    List<StepInfo> steps = systemDatabase.listWorkflowSteps("wf-123456");
    assertEquals(3, steps.size());
    assertEquals("child1", steps.get(0).getChildWorkflowId());
    assertEquals(0, steps.get(0).getFunctionId());
    assertEquals("childWorkflow", steps.get(0).getFunctionName());

    assertEquals("child2", steps.get(1).getChildWorkflowId());
    assertEquals(1, steps.get(1).getFunctionId());
    assertEquals("childWorkflow2", steps.get(1).getFunctionName());

    assertEquals("child3", steps.get(2).getChildWorkflowId());
    assertEquals(2, steps.get(2).getFunctionId());
    assertEquals("childWorkflow3", steps.get(2).getFunctionName());
  }

  @Test
  public void nestedChildren() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    simpleService.setSimpleService(simpleService);

    WorkflowOptions options = new WorkflowOptions.Builder("wf-123456").build();
    WorkflowHandle<String> handle = null;
    try (SetWorkflowOptions id = new SetWorkflowOptions(options)) {
      handle = dbos.startWorkflow(() -> simpleService.grandParent("123"));
    }

    assertEquals("p-c-gc-123", handle.getResult());

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());

    assertEquals(3, wfs.size());
    assertEquals("wf-123456", wfs.get(0).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).getStatus());

    assertEquals("child4", wfs.get(1).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).getStatus());

    assertEquals("child5", wfs.get(2).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(2).getStatus());

    List<StepInfo> steps = systemDatabase.listWorkflowSteps("wf-123456");
    assertEquals(1, steps.size());
    assertEquals("child4", steps.get(0).getChildWorkflowId());
    assertEquals(0, steps.get(0).getFunctionId());
    assertEquals("childWorkflow4", steps.get(0).getFunctionName());

    steps = systemDatabase.listWorkflowSteps("child4");
    assertEquals(1, steps.size());
    assertEquals("child5", steps.get(0).getChildWorkflowId());
    assertEquals(0, steps.get(0).getFunctionId());
    assertEquals("grandchildWorkflow", steps.get(0).getFunctionName());
  }

  @Test
  public void startWorkflowClosure() {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();

    String wfid = "wf-123";
    WorkflowHandle<String> handle = null;
    WorkflowOptions options = new WorkflowOptions.Builder(wfid).build();
    try (SetWorkflowID id = new SetWorkflowID(wfid)) {
      handle = dbos.startWorkflow(() -> simpleService.workWithString("test-item"));
    }

    String result = handle.getResult();
    assertEquals("Processed: test-item", result);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());
  }
}
