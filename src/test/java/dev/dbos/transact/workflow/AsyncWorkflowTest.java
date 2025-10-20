package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.*;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class AsyncWorkflowTest {

  private static DBOSConfig dbosConfig;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    AsyncWorkflowTest.dbosConfig =
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
  void afterEachTest() throws SQLException, Exception {
    DBOS.shutdown();
  }

  @Test
  public void sameWorkflowId() throws Exception {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

    DBOS.launch();

    SimpleServiceImpl.executionCount = 0;

    String wfid = "wf-123";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      simpleService.workWithString("test-item");
    }

    var handle = DBOS.retrieveWorkflow(wfid);
    String result = (String) handle.getResult();
    assertEquals("Processed: test-item", result);
    assertEquals(wfid, handle.getWorkflowId());

    List<WorkflowStatus> wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).name(), "workWithString");
    assertEquals(wfid, wfs.get(0).workflowId());

    try (var id = new WorkflowOptions(wfid).setContext()) {
      simpleService.workWithString("test-item");
    }

    handle = DBOS.retrieveWorkflow(wfid);
    result = (String) handle.getResult();
    assertEquals(1, SimpleServiceImpl.executionCount);
    assertEquals("Processed: test-item", result);
    assertEquals("wf-123", handle.getWorkflowId());

    wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals("wf-123", wfs.get(0).workflowId());

    String wfid2 = "wf-124";
    try (var id = new WorkflowOptions(wfid2).setContext()) {
      simpleService.workWithString("test-item");
    }

    handle = DBOS.retrieveWorkflow(wfid2);
    result = (String) handle.getResult();
    assertEquals("wf-124", handle.getWorkflowId());

    assertEquals(2, SimpleServiceImpl.executionCount);
    wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(2, wfs.size());
    assertEquals("wf-124", wfs.get(1).workflowId());
  }

  @Test
  public void workflowWithError() throws Exception {
    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

    DBOS.launch();

    String wfid = "abc";
    WorkflowHandle<Void, ?> handle =
        DBOS.startWorkflow(
            () -> {
              simpleService.workWithError();
              return null;
            },
            new StartWorkflowOptions(wfid));

    var e = assertThrows(Exception.class, () -> handle.getResult());
    assertEquals("DBOS Test error", e.getMessage());

    List<WorkflowStatus> wfs = DBOS.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).name(), "workError");
    assertNotNull(wfs.get(0).workflowId());
    assertEquals(wfs.get(0).workflowId(), handle.getWorkflowId());
    assertEquals("java.lang.Exception", handle.getStatus().error().className());
    assertEquals("DBOS Test error", handle.getStatus().error().message());
    assertEquals(WorkflowState.ERROR.name(), handle.getStatus().status());
  }

  @Test
  public void childWorkflowWithoutSet() throws Exception {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

    DBOS.launch();

    simpleService.setSimpleService(simpleService);

    WorkflowHandle<String, ?> handle =
        DBOS.startWorkflow(
            () -> simpleService.parentWorkflowWithoutSet("123"),
            new StartWorkflowOptions("wf-123456"));

    System.out.println(handle.getResult());

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

    WorkflowHandle<String, ?> handle =
        DBOS.startWorkflow(
            () -> simpleService.workflowWithMultipleChildren("123"),
            new StartWorkflowOptions("wf-123456"));

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

    WorkflowHandle<String, ?> handle =
        DBOS.startWorkflow(
            () -> simpleService.grandParent("123"), new StartWorkflowOptions("wf-123456"));

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
    assertEquals("child4", steps.get(1).childWorkflowId());

    steps = DBOS.listWorkflowSteps("child4");
    assertEquals(2, steps.size());
    assertEquals("child5", steps.get(0).childWorkflowId());
    assertEquals(0, steps.get(0).functionId());
    assertEquals("grandchildWorkflow", steps.get(0).functionName());
    assertEquals("DBOS.getResult", steps.get(1).functionName());
    assertEquals("child5", steps.get(1).childWorkflowId());
  }

  @Test
  public void startWorkflowClosure() {
    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

    DBOS.launch();

    WorkflowHandle<String, RuntimeException> handle =
        DBOS.startWorkflow(() -> simpleService.workWithString("test-item"));

    String result = handle.getResult();
    assertEquals("Processed: test-item", result);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());
  }

  @Test
  public void resAndStatus() throws Exception {

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

    DBOS.launch();

    simpleService.setSimpleService(simpleService);

    var wfh = DBOS.startWorkflow(() -> simpleService.childWorkflow("Base"));
    var wfhgrs = DBOS.startWorkflow(() -> simpleService.getResultInStep(wfh.getWorkflowId()));
    var wfres = wfhgrs.getResult();
    assertEquals("Base", wfres);
    var wfhstat = DBOS.startWorkflow(() -> simpleService.getStatus(wfh.getWorkflowId()));
    var wfstat = wfhstat.getResult();
    assertEquals(WorkflowState.SUCCESS.toString(), wfstat);
    var wfhstat2 = DBOS.startWorkflow(() -> simpleService.getStatusInStep(wfh.getWorkflowId()));
    var wfstat2 = wfhstat2.getResult();
    assertEquals(WorkflowState.SUCCESS.toString(), wfstat2);

    var steps = DBOS.listWorkflowSteps(wfhgrs.getWorkflowId());
    assertEquals(1, steps.size());
    assertEquals("getResInStep", steps.get(0).functionName());

    steps = DBOS.listWorkflowSteps(wfhstat.getWorkflowId());
    assertEquals(1, steps.size());
    assertEquals("DBOS.getWorkflowStatus", steps.get(0).functionName());

    steps = DBOS.listWorkflowSteps(wfhstat2.getWorkflowId());
    assertEquals(1, steps.size());
    assertEquals("getStatusInStep", steps.get(0).functionName());

    var ise = assertThrows(IllegalStateException.class, () -> simpleService.startWfInStep());
    assertEquals("cannot invoke a workflow from a step", ise.getMessage());
    ise =
        assertThrows(
            IllegalStateException.class, () -> simpleService.startWfInStepById("whatAboutWId?"));
    assertEquals("cannot invoke a workflow from a step", ise.getMessage());
  }
}
