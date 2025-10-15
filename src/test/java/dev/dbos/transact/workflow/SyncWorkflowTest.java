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

  @BeforeAll
  static void onetimeSetup() throws Exception {

    SyncWorkflowTest.dbosConfig =
        new DBOSConfig.Builder()
            .appName("systemdbtest")
            .databaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .dbUser("postgres")
            .maximumPoolSize(2)
            .runAdminServer()
            .build();
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
  public void workflowWithOneInput() throws SQLException {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

    DBOS.launch();

    String result = simpleService.workWithString("test-item");
    assertEquals("Processed: test-item", result);

    List<WorkflowStatus> wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).name(), "workWithString");
    assertNotNull(wfs.get(0).workflowId());
    assertEquals("test-item", wfs.get(0).input()[0]);
    assertEquals("Processed: test-item", wfs.get(0).output());
  }

  @Test
  public void workflowWithError() throws SQLException {
    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

    DBOS.launch();

    var e = assertThrows(Exception.class, () -> simpleService.workWithError());
    assertEquals("DBOS Test error", e.getMessage());

    List<WorkflowStatus> wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).name(), "workError");
    assertEquals("java.lang.Exception", wfs.get(0).error().className());
    assertEquals("DBOS Test error", wfs.get(0).error().message());
    assertNotNull(wfs.get(0).workflowId());
  }

  @Test
  public void setWorkflowId() throws SQLException {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

    DBOS.launch();

    String result = null;

    String wfid = "wf-123";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      result = simpleService.workWithString("test-item");
    }

    assertEquals("Processed: test-item", result);

    List<WorkflowStatus> wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).name(), "workWithString");
    assertEquals(wfid, wfs.get(0).workflowId());

    WorkflowHandle<String, SQLException> handle = DBOS.retrieveWorkflow(wfid);
    String hresult = (String) handle.getResult();
    assertEquals("Processed: test-item", hresult);
    assertEquals("wf-123", handle.getWorkflowId());
    assertEquals("SUCCESS", handle.getStatus().status());
  }

  @Test
  public void sameWorkflowId() throws SQLException {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

    DBOS.launch();

    String result = null;
    SimpleServiceImpl.executionCount = 0;

    try (var id = new WorkflowOptions("wf-123").setContext()) {
      result = simpleService.workWithString("test-item");
    }

    assertEquals("Processed: test-item", result);

    List<WorkflowStatus> wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).name(), "workWithString");
    assertEquals("wf-123", wfs.get(0).workflowId());

    assertEquals(1, SimpleServiceImpl.executionCount);

    try (var id = new WorkflowOptions("wf-123").setContext()) {
      result = simpleService.workWithString("test-item");
    }
    assertEquals(1, SimpleServiceImpl.executionCount);
    wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals("wf-123", wfs.get(0).workflowId());

    try (var id = new WorkflowOptions("wf-124").setContext()) {
      result = simpleService.workWithString("test-item");
    }

    assertEquals(2, SimpleServiceImpl.executionCount);
    wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(2, wfs.size());
    assertEquals("wf-124", wfs.get(1).workflowId());
  }

  @Test
  public void childWorkflowWithoutSet() throws Exception {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

    DBOS.launch();

    simpleService.setSimpleService(simpleService);

    String result = null;

    try (var id = new WorkflowOptions("wf-123456").setContext()) {
      result = simpleService.parentWorkflowWithoutSet("123");
    }

    assertEquals("123abc", result);

    List<WorkflowStatus> wfs = DBOS.listWorkflows(new ListWorkflowsInput());

    assertEquals(2, wfs.size());
    assertEquals("wf-123456", wfs.get(0).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).status());

    assertEquals("wf-123456-0", wfs.get(1).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).status());

    List<StepInfo> steps = DBOS.listWorkflowSteps("wf-123456");
    assertEquals(1, steps.size());
    assertEquals("wf-123456-0", steps.get(0).childWorkflowId());
    assertEquals(0, steps.get(0).functionId());
    assertEquals("childWorkflow", steps.get(0).functionName());
  }

  @Test
  public void multipleChildren() throws Exception {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

    DBOS.launch();

    simpleService.setSimpleService(simpleService);

    try (var id = new WorkflowOptions("wf-123456").setContext()) {
      simpleService.workflowWithMultipleChildren("123");
    }

    var handle = DBOS.retrieveWorkflow("wf-123456");
    assertEquals("123abcdefghi", handle.getResult());

    List<WorkflowStatus> wfs = DBOS.listWorkflows(new ListWorkflowsInput());

    assertEquals(4, wfs.size());
    assertEquals("wf-123456", wfs.get(0).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).status());

    assertEquals("child1", wfs.get(1).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).status());

    assertEquals("child2", wfs.get(2).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(2).status());

    assertEquals("child3", wfs.get(3).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(3).status());

    List<StepInfo> steps = DBOS.listWorkflowSteps("wf-123456");
    assertEquals(6, steps.size());
    assertEquals("child1", steps.get(0).childWorkflowId());
    assertEquals(0, steps.get(0).functionId());
    assertEquals("childWorkflow", steps.get(0).functionName());
    assertEquals("DBOS.getResult", steps.get(1).functionName());

    assertEquals("child2", steps.get(2).childWorkflowId());
    assertEquals(2, steps.get(2).functionId());
    assertEquals("childWorkflow2", steps.get(2).functionName());
    assertEquals("DBOS.getResult", steps.get(3).functionName());

    assertEquals("child3", steps.get(4).childWorkflowId());
    assertEquals(4, steps.get(4).functionId());
    assertEquals("childWorkflow3", steps.get(4).functionName());
    assertEquals("DBOS.getResult", steps.get(5).functionName());
  }

  @Test
  public void nestedChildren() throws Exception {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

    DBOS.launch();

    simpleService.setSimpleService(simpleService);

    try (var id = new WorkflowOptions("wf-123456").setContext()) {
      simpleService.grandParent("123");
    }

    var handle = DBOS.retrieveWorkflow("wf-123456");
    assertEquals("p-c-gc-123", handle.getResult());

    List<WorkflowStatus> wfs = DBOS.listWorkflows(new ListWorkflowsInput());

    assertEquals(3, wfs.size());
    assertEquals("wf-123456", wfs.get(0).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).status());

    assertEquals("child4", wfs.get(1).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).status());

    assertEquals("child5", wfs.get(2).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(2).status());

    List<StepInfo> steps = DBOS.listWorkflowSteps("wf-123456");
    assertEquals(2, steps.size());
    assertEquals("child4", steps.get(0).childWorkflowId());
    assertEquals(0, steps.get(0).functionId());
    assertEquals("childWorkflow4", steps.get(0).functionName());
    assertEquals("DBOS.getResult", steps.get(1).functionName());

    steps = DBOS.listWorkflowSteps("child4");
    assertEquals(2, steps.size());
    assertEquals("child5", steps.get(0).childWorkflowId());
    assertEquals(0, steps.get(0).functionId());
    assertEquals("grandchildWorkflow", steps.get(0).functionName());
    assertEquals("DBOS.getResult", steps.get(1).functionName());
  }
}
