package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.PgContainer;

import java.util.List;

import org.junit.jupiter.api.*;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class AsyncWorkflowTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
  }

  @Test
  public void sameWorkflowId() throws Exception {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerWorkflows(SimpleService.class, impl);
    dbos.launch();

    String wfid = "wf-123";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      simpleService.workWithString("test-item");
    }

    var handle = dbos.retrieveWorkflow(wfid);
    String result = (String) handle.getResult();
    assertEquals("Processed: test-item", result);
    assertEquals(wfid, handle.workflowId());

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).workflowName(), "workWithString");
    assertEquals(wfid, wfs.get(0).workflowId());

    try (var id = new WorkflowOptions(wfid).setContext()) {
      simpleService.workWithString("test-item");
    }

    handle = dbos.retrieveWorkflow(wfid);
    result = (String) handle.getResult();
    assertEquals(1, impl.executionCount);
    assertEquals("Processed: test-item", result);
    assertEquals("wf-123", handle.workflowId());

    wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals("wf-123", wfs.get(0).workflowId());

    String wfid2 = "wf-124";
    try (var id = new WorkflowOptions(wfid2).setContext()) {
      simpleService.workWithString("test-item");
    }

    handle = dbos.retrieveWorkflow(wfid2);
    result = (String) handle.getResult();
    assertEquals("wf-124", handle.workflowId());

    assertEquals(2, impl.executionCount);
    wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(2, wfs.size());
    assertEquals("wf-124", wfs.get(1).workflowId());
  }

  @Test
  public void workflowWithError() throws Exception {
    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerWorkflows(SimpleService.class, impl);
    dbos.launch();

    String wfid = "abc";
    WorkflowHandle<Void, ?> handle =
        dbos.startWorkflow(
            () -> {
              simpleService.workWithError();
              return null;
            },
            new StartWorkflowOptions(wfid));

    var e = assertThrows(Exception.class, () -> handle.getResult());
    assertEquals("DBOS Test error", e.getMessage());

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).workflowName(), "workError");
    assertNotNull(wfs.get(0).workflowId());
    assertEquals(wfs.get(0).workflowId(), handle.workflowId());
    assertEquals("java.lang.Exception", handle.getStatus().error().className());
    assertEquals("DBOS Test error", handle.getStatus().error().message());
    assertEquals(WorkflowState.ERROR, handle.getStatus().status());
  }

  @Test
  public void childWorkflowWithoutSet() throws Exception {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerWorkflows(SimpleService.class, impl);
    impl.setSelf(simpleService);
    dbos.launch();

    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(
            () -> simpleService.parentWorkflowWithoutSet("123"),
            new StartWorkflowOptions("wf-123456"));

    System.out.println(handle.getResult());

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());

    assertEquals(2, wfs.size());
    assertEquals("wf-123456", wfs.get(0).workflowId());
    assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());

    assertEquals("wf-123456-0", wfs.get(1).workflowId());
    assertEquals(WorkflowState.SUCCESS, wfs.get(1).status());

    List<StepInfo> steps = dbos.listWorkflowSteps("wf-123456");
    assertEquals(1, steps.size());
    assertEquals("wf-123456-0", steps.get(0).childWorkflowId());
    assertEquals(0, steps.get(0).functionId());
    assertEquals("childWorkflow", steps.get(0).functionName());
  }

  @Test
  public void multipleChildren() throws Exception {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerWorkflows(SimpleService.class, impl);
    impl.setSelf(simpleService);
    dbos.launch();

    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(
            () -> simpleService.workflowWithMultipleChildren("123"),
            new StartWorkflowOptions("wf-123456"));

    assertEquals("123abcdefghi", handle.getResult());

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());

    assertEquals(4, wfs.size());
    assertEquals("wf-123456", wfs.get(0).workflowId());
    assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());

    assertEquals("child1", wfs.get(1).workflowId());
    assertEquals(WorkflowState.SUCCESS, wfs.get(1).status());

    assertEquals("child2", wfs.get(2).workflowId());
    assertEquals(WorkflowState.SUCCESS, wfs.get(2).status());

    assertEquals("child3", wfs.get(3).workflowId());
    assertEquals(WorkflowState.SUCCESS, wfs.get(3).status());

    List<StepInfo> steps = dbos.listWorkflowSteps("wf-123456");
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

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerWorkflows(SimpleService.class, impl);
    impl.setSelf(simpleService);
    dbos.launch();

    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(
            () -> simpleService.grandParent("123"), new StartWorkflowOptions("wf-123456"));

    assertEquals("p-c-gc-123", handle.getResult());

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());

    assertEquals(3, wfs.size());
    assertEquals("wf-123456", wfs.get(0).workflowId());
    assertEquals(WorkflowState.SUCCESS, wfs.get(0).status());

    assertEquals("child4", wfs.get(1).workflowId());
    assertEquals(WorkflowState.SUCCESS, wfs.get(1).status());

    assertEquals("child5", wfs.get(2).workflowId());
    assertEquals(WorkflowState.SUCCESS, wfs.get(2).status());

    List<StepInfo> steps = dbos.listWorkflowSteps("wf-123456");
    assertEquals(2, steps.size());
    assertEquals("child4", steps.get(0).childWorkflowId());
    assertEquals(0, steps.get(0).functionId());
    assertEquals("childWorkflow4", steps.get(0).functionName());
    assertEquals("DBOS.getResult", steps.get(1).functionName());
    assertEquals("child4", steps.get(1).childWorkflowId());

    steps = dbos.listWorkflowSteps("child4");
    assertEquals(2, steps.size());
    assertEquals("child5", steps.get(0).childWorkflowId());
    assertEquals(0, steps.get(0).functionId());
    assertEquals("grandchildWorkflow", steps.get(0).functionName());
    assertEquals("DBOS.getResult", steps.get(1).functionName());
    assertEquals("child5", steps.get(1).childWorkflowId());
  }

  @Test
  public void startWorkflowClosure() {
    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerWorkflows(SimpleService.class, impl);
    dbos.launch();

    WorkflowHandle<String, RuntimeException> handle =
        dbos.startWorkflow(() -> simpleService.workWithString("test-item"));

    String result = handle.getResult();
    assertEquals("Processed: test-item", result);
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
  }

  @Test
  public void resAndStatus() throws Exception {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerWorkflows(SimpleService.class, impl);
    impl.setSelf(simpleService);
    dbos.launch();

    var wfh = dbos.startWorkflow(() -> simpleService.childWorkflow("Base"));
    var wfhgrs = dbos.startWorkflow(() -> simpleService.getResultInStep(wfh.workflowId()));
    var wfres = wfhgrs.getResult();
    assertEquals("Base", wfres);
    var wfhstat = dbos.startWorkflow(() -> simpleService.getStatus(wfh.workflowId()));
    var wfstat = wfhstat.getResult();
    assertEquals(WorkflowState.SUCCESS.toString(), wfstat);
    var wfhstat2 = dbos.startWorkflow(() -> simpleService.getStatusInStep(wfh.workflowId()));
    var wfstat2 = wfhstat2.getResult();
    assertEquals(WorkflowState.SUCCESS.toString(), wfstat2);

    var steps = dbos.listWorkflowSteps(wfhgrs.workflowId());
    assertEquals(1, steps.size());
    assertEquals("getResInStep", steps.get(0).functionName());

    steps = dbos.listWorkflowSteps(wfhstat.workflowId());
    assertEquals(1, steps.size());
    assertEquals("DBOS.getWorkflowStatus", steps.get(0).functionName());

    steps = dbos.listWorkflowSteps(wfhstat2.workflowId());
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
