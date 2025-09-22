package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.queue.QueuesTest;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueChildWorkflowTest {

  Logger logger = LoggerFactory.getLogger(QueuesTest.class);

  private static DBOSConfig dbosConfig;
  private DBOS dbos;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    QueueChildWorkflowTest.dbosConfig =
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
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);

    dbos = DBOS.initialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dbos.shutdown();
  }

  @Test
  public void multipleChildren() throws Exception {

    Queue childQ = dbos.Queue("childQ").concurrency(5).workerConcurrency(5).build();

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .queue(childQ)
            .build();

    simpleService.setSimpleService(simpleService);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    try (SetWorkflowID id = new SetWorkflowID("wf-123456")) {
      simpleService.WorkflowWithMultipleChildren("123");
    }

    var handle = dbosExecutor.retrieveWorkflow("wf-123456");
    assertEquals("123abcdefghi", (String) handle.getResult());

    List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());

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

    Queue childQ = dbos.Queue("childQ").concurrency(5).workerConcurrency(5).build();

    SimpleService simpleService =
        dbos.<SimpleService>Workflow()
            .interfaceClass(SimpleService.class)
            .implementation(new SimpleServiceImpl())
            .queue(childQ)
            .build();

    simpleService.setSimpleService(simpleService);

    dbos.launch();
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    try (SetWorkflowID id = new SetWorkflowID("wf-123456")) {
      simpleService.grandParent("123");
    }

    var handle = dbosExecutor.retrieveWorkflow("wf-123456");
    assertEquals("p-c-gc-123", handle.getResult());

    List<WorkflowStatus> wfs = systemDatabase.listWorkflows(new ListWorkflowsInput());

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
}
