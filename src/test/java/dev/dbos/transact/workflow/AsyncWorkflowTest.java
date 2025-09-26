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
    assertEquals(wfs.get(0).getName(), "workWithString");
    assertEquals(wfid, wfs.get(0).getWorkflowId());

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
    assertEquals("wf-123", wfs.get(0).getWorkflowId());

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
    assertEquals(wfs.get(0).getName(), "workError");
    assertNotNull(wfs.get(0).getWorkflowId());
    assertEquals(wfs.get(0).getWorkflowId(), handle.getWorkflowId());
    assertEquals("java.lang.Exception", handle.getStatus().getError().className());
    assertEquals("DBOS Test error", handle.getStatus().getError().message());
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

    simpleService.setSimpleService(simpleService);

    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(
            () -> simpleService.parentWorkflowWithoutSet("123"),
            new StartWorkflowOptions("wf-123456"));

    System.out.println(handle.getResult());

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());

    assertEquals(2, wfs.size());
    assertEquals("wf-123456", wfs.get(0).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).getStatus());

    assertEquals("wf-123456-0", wfs.get(1).getWorkflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).getStatus());

    List<StepInfo> steps = dbos.listWorkflowSteps("wf-123456");
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

    simpleService.setSimpleService(simpleService);

    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(
            () -> simpleService.workflowWithMultipleChildren("123"),
            new StartWorkflowOptions("wf-123456"));

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

    simpleService.setSimpleService(simpleService);

    WorkflowHandle<String, ?> handle =
        dbos.startWorkflow(
            () -> simpleService.grandParent("123"), new StartWorkflowOptions("wf-123456"));

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
    assertEquals("child4", steps.get(0).getChildWorkflowId());
    assertEquals(0, steps.get(0).getFunctionId());
    assertEquals("childWorkflow4", steps.get(0).getFunctionName());

    steps = dbos.listWorkflowSteps("child4");
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

    WorkflowHandle<String, RuntimeException> handle =
        dbos.startWorkflow(() -> simpleService.workWithString("test-item"));

    String result = handle.getResult();
    assertEquals("Processed: test-item", result);
    assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());
  }
}
