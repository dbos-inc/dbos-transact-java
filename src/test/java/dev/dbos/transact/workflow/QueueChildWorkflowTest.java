package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class QueueChildWorkflowTest {

  private static DBOSConfig dbosConfig;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    QueueChildWorkflowTest.dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withDbUser("postgres")
            .withMaximumPoolSize(2);
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
  public void multipleChildren() throws Exception {

    Queue childQ = new Queue("childQ").withConcurrency(5).withWorkerConcurrency(5);
    DBOS.registerQueue(childQ);

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

    simpleService.setSimpleService(simpleService);

    DBOS.launch();

    var handle =
        DBOS.startWorkflow(
            () -> simpleService.workflowWithMultipleChildren("123"),
            new StartWorkflowOptions().withQueue(childQ));

    assertEquals("123abcdefghi", (String) handle.getResult());

    List<WorkflowStatus> wfs = DBOS.listWorkflows(new ListWorkflowsInput());

    assertEquals(4, wfs.size());
    assertEquals(handle.getWorkflowId(), wfs.get(0).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(0).status());

    assertEquals("child1", wfs.get(1).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).status());

    assertEquals("child2", wfs.get(2).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(2).status());

    assertEquals("child3", wfs.get(3).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(3).status());

    List<StepInfo> steps = DBOS.listWorkflowSteps(handle.getWorkflowId());
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

    Queue childQ = new Queue("childQ").withConcurrency(5).withWorkerConcurrency(5);
    DBOS.registerQueue(childQ);

    SimpleService simpleService =
        DBOS.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

    simpleService.setSimpleService(simpleService);

    DBOS.launch();

    DBOS.startWorkflow(
        () -> simpleService.grandParent("123"),
        new StartWorkflowOptions("wf-123456").withQueue(childQ));

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
