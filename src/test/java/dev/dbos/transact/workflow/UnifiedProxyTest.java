package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class UnifiedProxyTest {

  private static DBOSConfig dbosConfig;
  private DBOS.Instance dbos;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    UnifiedProxyTest.dbosConfig =
        new DBOSConfig.Builder()
            .appName("systemdbtest")
            .databaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .dbUser("postgres")
            .maximumPoolSize(2)
            .build();
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);

    dbos = DBOS.initialize(dbosConfig);
  }

  @AfterEach
  void afterEachTest() {
    dbos.shutdown();
  }

  @Test
  public void optionsWithCall() throws Exception {

    SimpleService simpleService =
        dbos.registerWorkflows(SimpleService.class, new SimpleServiceImpl());
    Queue q = dbos.Queue("simpleQ").build();

    dbos.launch();

    // synchronous
    String wfid1 = "wf-123";
    WorkflowOptions options = new WorkflowOptions(wfid1);
    String result;
    try (var id = options.setContext()) {
      result = simpleService.workWithString("test-item");
    }
    assertEquals("Processed: test-item", result);

    // asynchronous

    String wfid2 = "wf-124";
    options = new WorkflowOptions(wfid2);
    WorkflowHandle<String, ?> handle = null;
    try (var id = options.setContext()) {
      handle = DBOS.startWorkflow(() -> simpleService.workWithString("test-item-async"));
    }

    result = handle.getResult();
    assertEquals("Processed: test-item-async", result);
    assertEquals(wfid2, handle.getWorkflowId());
    assertEquals("SUCCESS", handle.getStatus().status());

    // Queued
    String wfid3 = "wf-125";
    var startOptions = new StartWorkflowOptions(wfid3).withQueue(q);

    DBOS.startWorkflow(() -> simpleService.workWithString("test-item-q"), startOptions);

    handle = dbos.retrieveWorkflow(wfid3);
    result = (String) handle.getResult();
    assertEquals("Processed: test-item-q", result);
    assertEquals(wfid3, handle.getWorkflowId());
    assertEquals("SUCCESS", handle.getStatus().status());

    var builder = new ListWorkflowsInput.Builder().workflowIDs(Arrays.asList(wfid3));
    ListWorkflowsInput input = builder.build();
    List<WorkflowStatus> wfs = dbos.listWorkflows(input);
    assertEquals("simpleQ", wfs.get(0).queueName());
  }

  @Test
  public void syncParentWithQueuedChildren() throws Exception {

    SimpleService simpleService =
        dbos.registerWorkflows(SimpleService.class, new SimpleServiceImpl());

    dbos.Queue("childQ").build();
    dbos.launch();

    simpleService.setSimpleService(simpleService);

    String wfid1 = "wf-123";
    WorkflowOptions options = new WorkflowOptions(wfid1);
    String result;
    try (var id = options.setContext()) {
      result = simpleService.syncWithQueued();
    }
    assertEquals("QueuedChildren", result);

    for (int i = 0; i < 3; i++) {
      String wid = "child" + i;
      WorkflowHandle<?, ?> h = dbos.retrieveWorkflow(wid);
      assertEquals(wid, h.getResult());
    }

    List<WorkflowStatus> wfs = dbos.listWorkflows(new ListWorkflowsInput());
    assertEquals(wfs.size(), 4);

    assertEquals(wfid1, wfs.get(0).workflowId());
    assertEquals("child0", wfs.get(1).workflowId());
    assertEquals("childQ", wfs.get(1).queueName());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(1).status());

    assertEquals("child1", wfs.get(2).workflowId());
    assertEquals("childQ", wfs.get(2).queueName());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(2).status());

    assertEquals("child2", wfs.get(3).workflowId());
    assertEquals("childQ", wfs.get(3).queueName());
    assertEquals(WorkflowState.SUCCESS.name(), wfs.get(3).status());
  }
}
