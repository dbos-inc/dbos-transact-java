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
import org.junit.jupiter.api.Timeout;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
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
  public void sameWorkflowId() throws Exception {

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();

    SimpleServiceImpl.executionCount = 0;

    String wfid = "wf-123";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      simpleService.workWithString("test-item");
    }

    var handle = dbos.retrieveWorkflow(wfid);
    String result = (String) handle.getResult();
    assertEquals("Processed: test-item", result);
    assertEquals(wfid, handle.getWorkflowId());

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals(wfs.get(0).name(), "workWithString");
    assertEquals(wfid, wfs.get(0).workflowId());

    try (var id = new WorkflowOptions(wfid).setContext()) {
      simpleService.workWithString("test-item");
    }

    handle = dbos.retrieveWorkflow(wfid);
    result = (String) handle.getResult();
    assertEquals(1, SimpleServiceImpl.executionCount);
    assertEquals("Processed: test-item", result);
    assertEquals("wf-123", handle.getWorkflowId());

    wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, wfs.size());
    assertEquals("wf-123", wfs.get(0).workflowId());

    String wfid2 = "wf-124";
    try (var id = new WorkflowOptions(wfid2).setContext()) {
      simpleService.workWithString("test-item");
    }

    handle = dbos.retrieveWorkflow(wfid2);
    result = (String) handle.getResult();
    assertEquals("wf-124", handle.getWorkflowId());

    assertEquals(2, SimpleServiceImpl.executionCount);
    wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(2, wfs.size());
    assertEquals("wf-124", wfs.get(1).workflowId());
  }

  @Test
  public void workflowWithError() throws Exception {
    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

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
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();

    simpleService.setSimpleService(simpleService);

    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(
            () -> simpleService.parentWorkflowWithoutSet("123"),
            new StartWorkflowOptions("wf-123456"));

    System.out.println(handle.getResult());

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());

    assertEquals(2, wfs.size());
    assertEquals("wf-123456", wfs.get(0).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).status());

    assertEquals("wf-123456-0", wfs.get(1).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).status());

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

    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(
            () -> simpleService.workflowWithMultipleChildren("123"),
            new StartWorkflowOptions("wf-123456"));

    assertEquals("123abcdefghi", handle.getResult());

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());

    assertEquals(4, wfs.size());
    assertEquals("wf-123456", wfs.get(0).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).status());

    assertEquals("child1", wfs.get(1).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).status());

    assertEquals("child2", wfs.get(2).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(2).status());

    assertEquals("child3", wfs.get(3).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(3).status());

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

    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(
            () -> simpleService.grandParent("123"), new StartWorkflowOptions("wf-123456"));

    assertEquals("p-c-gc-123", handle.getResult());

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());

    assertEquals(3, wfs.size());
    assertEquals("wf-123456", wfs.get(0).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).status());

    assertEquals("child4", wfs.get(1).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).status());

    assertEquals("child5", wfs.get(2).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(2).status());

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

  @Test
  public void startWorkflowClosure() {
    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .build();

    dbos.launch();

    WorkflowHandle<String, RuntimeException> handle =
        dbos.startWorkflow(() -> simpleService.workWithString("test-item"));

    String result = handle.getResult();
    assertEquals("Processed: test-item", result);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());
  }
}
