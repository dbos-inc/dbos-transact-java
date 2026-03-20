package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.PgContainer;

import java.util.List;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT)
public class UnifiedProxyTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
  }

  @Test
  public void optionsWithCall() throws Exception {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerWorkflows(SimpleService.class, impl);
    Queue q = new Queue("simpleQ");
    dbos.registerQueue(q);

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
      handle = dbos.startWorkflow(() -> simpleService.workWithString("test-item-async"));
    }

    result = handle.getResult();
    assertEquals("Processed: test-item-async", result);
    assertEquals(wfid2, handle.workflowId());
    assertEquals("SUCCESS", handle.getStatus().status());

    // Queued
    String wfid3 = "wf-125";
    var startOptions = new StartWorkflowOptions(wfid3).withQueue(q);

    dbos.startWorkflow(() -> simpleService.workWithString("test-item-q"), startOptions);

    handle = dbos.retrieveWorkflow(wfid3);
    result = (String) handle.getResult();
    assertEquals("Processed: test-item-q", result);
    assertEquals(wfid3, handle.workflowId());
    assertEquals("SUCCESS", handle.getStatus().status());

    ListWorkflowsInput input = new ListWorkflowsInput().withWorkflowId(wfid3);
    List<WorkflowStatus> wfs = dbos.listWorkflows(input);
    assertEquals("simpleQ", wfs.get(0).queueName());
  }

  @Test
  public void syncParentWithQueuedChildren() throws Exception {

    SimpleServiceImpl impl = new SimpleServiceImpl(dbos);
    SimpleService simpleService = dbos.registerWorkflows(SimpleService.class, impl);

    dbos.registerQueue(new Queue("childQ"));
    impl.setSelf(simpleService);
    dbos.launch();

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
