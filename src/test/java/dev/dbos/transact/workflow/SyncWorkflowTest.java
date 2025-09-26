package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.*;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class SyncWorkflowTest {

  private static DBOSConfig dbosConfig;
  private DBOS dbos;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    SyncWorkflowTest.dbosConfig =
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
  void afterEachTest() throws Exception {
    dbos.shutdown();
  }

  @Test
  public void workflowWithOneInput() throws SQLException {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();

    String result = simpleService.workWithString("test-item");
    assertEquals("Processed: test-item", result);

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).getName(), "workWithString");
    assertNotNull(wfs.get(0).getWorkflowId());
    assertEquals("test-item", wfs.get(0).getInput()[0]);
    assertEquals("Processed: test-item", wfs.get(0).getOutput());
  }

  @Test
  public void workflowWithError() throws SQLException {
    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();

    var e = assertThrows(Exception.class, () -> simpleService.workWithError());
    assertEquals("DBOS Test error", e.getMessage());

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).getName(), "workError");
    assertEquals("java.lang.Exception", wfs.get(0).getError().className());
    assertEquals("DBOS Test error", wfs.get(0).getError().message());
    assertNotNull(wfs.get(0).getWorkflowId());
  }

  @Test
  public void setWorkflowId() throws SQLException {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();

    String result = null;

    String wfid = "wf-123";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      result = simpleService.workWithString("test-item");
    }

    assertEquals("Processed: test-item", result);

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).getName(), "workWithString");
    assertEquals(wfid, wfs.get(0).getWorkflowId());

    WorkflowHandle<String, SQLException> handle = dbos.retrieveWorkflow(wfid);
    String hresult = (String) handle.getResult();
    assertEquals("Processed: test-item", hresult);
    assertEquals("wf-123", handle.getWorkflowId());
    assertEquals("SUCCESS", handle.getStatus().getStatus());
  }

  @Test
  public void sameWorkflowId() throws SQLException {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();

    String result = null;
    SimpleServiceImpl.executionCount = 0;

    try (var id = new WorkflowOptions("wf-123").setContext()) {
      result = simpleService.workWithString("test-item");
    }

    assertEquals("Processed: test-item", result);

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).getName(), "workWithString");
    assertEquals("wf-123", wfs.get(0).getWorkflowId());

    assertEquals(1, SimpleServiceImpl.executionCount);

    try (var id = new WorkflowOptions("wf-123").setContext()) {
      result = simpleService.workWithString("test-item");
    }
    assertEquals(1, SimpleServiceImpl.executionCount);
    wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals("wf-123", wfs.get(0).getWorkflowId());

    try (var id = new WorkflowOptions("wf-124").setContext()) {
      result = simpleService.workWithString("test-item");
    }

    assertEquals(2, SimpleServiceImpl.executionCount);
    wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(2, wfs.size());
    assertEquals("wf-124", wfs.get(1).getWorkflowId());
  }

  @Test
  public void childWorkflowWithoutSet() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();

    simpleService.setSimpleService(simpleService);

    String result = null;

    try (var id = new WorkflowOptions("wf-123456").setContext()) {
      result = simpleService.parentWorkflowWithoutSet("123");
    }

    assertEquals("123abc", result);

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());

    assertEquals(2, wfs.size());
    assertEquals("wf-123456", wfs.get(0).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).getStatus());

    assertEquals("wf-123456-0", wfs.get(1).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).getStatus());

    List<StepInfo> steps = dbos.listWorkflowSteps("wf-123456");
    assertEquals(1, steps.size());
    assertEquals("wf-123456-0", steps.get(0).childWorkflowId());
    assertEquals(0, steps.get(0).functionId());
    assertEquals("childWorkflow", steps.get(0).functionName());
  }

  @Test
  public void multipleChildren() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();

    simpleService.setSimpleService(simpleService);

    try (var id = new WorkflowOptions("wf-123456").setContext()) {
      simpleService.workflowWithMultipleChildren("123");
    }

    var handle = dbos.retrieveWorkflow("wf-123456");
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

    List<StepInfo> steps = dbos.listWorkflowSteps("wf-123456");
    assertEquals(3, steps.size());
    assertEquals("child1", steps.get(0).childWorkflowId());
    assertEquals(0, steps.get(0).functionId());
    assertEquals("childWorkflow", steps.get(0).functionName());

    assertEquals("child2", steps.get(1).childWorkflowId());
    assertEquals(1, steps.get(1).functionId());
    assertEquals("childWorkflow2", steps.get(1).functionName());

    assertEquals("child3", steps.get(2).childWorkflowId());
    assertEquals(2, steps.get(2).functionId());
    assertEquals("childWorkflow3", steps.get(2).functionName());
  }

  @Test
  public void nestedChildren() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();

    simpleService.setSimpleService(simpleService);

    try (var id = new WorkflowOptions("wf-123456").setContext()) {
      simpleService.grandParent("123");
    }

    var handle = dbos.retrieveWorkflow("wf-123456");
    assertEquals("p-c-gc-123", handle.getResult());

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());

    assertEquals(3, wfs.size());
    assertEquals("wf-123456", wfs.get(0).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).getStatus());

    assertEquals("child4", wfs.get(1).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).getStatus());

    assertEquals("child5", wfs.get(2).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(2).getStatus());

    List<StepInfo> steps = dbos.listWorkflowSteps("wf-123456");
    assertEquals(1, steps.size());
    assertEquals("child4", steps.get(0).childWorkflowId());
    assertEquals(0, steps.get(0).functionId());
    assertEquals("childWorkflow4", steps.get(0).functionName());

    steps = dbos.listWorkflowSteps("child4");
    assertEquals(1, steps.size());
    assertEquals("child5", steps.get(0).childWorkflowId());
    assertEquals(0, steps.get(0).functionId());
    assertEquals("grandchildWorkflow", steps.get(0).functionName());
  }
}
