package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT)
public class GarbageCollectionTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;

  private GCTestServiceImpl impl;
  private GCTestService proxy;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);

    impl = new GCTestServiceImpl(dbos);
    proxy = dbos.registerWorkflows(GCTestService.class, impl);

    dbos.launch();
  }

  @Test
  void garbageCollection() throws Exception {
    int numWorkflows = 10;

    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    // Start one blocked workflow and 10 normal workflows
    WorkflowHandle<String, ?> handle = dbos.startWorkflow(() -> proxy.gcBlockedWorkflow());
    for (int i = 0; i < numWorkflows; i++) {
      int result = proxy.testWorkflow(i);
      assertEquals(i, result);
    }

    // Garbage collect all but one completed workflow
    List<WorkflowStatus> statusList = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(11, statusList.size());
    systemDatabase.garbageCollect(null, 1L);
    statusList = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(2, statusList.size());
    assertEquals(handle.workflowId(), statusList.get(0).workflowId());

    // Garbage collect all completed workflows
    systemDatabase.garbageCollect(System.currentTimeMillis(), null);
    statusList = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(1, statusList.size());
    assertEquals(handle.workflowId(), statusList.get(0).workflowId());

    // Finish the blocked workflow, garbage collect everything
    impl.gcLatch.countDown();
    assertEquals(handle.workflowId(), handle.getResult());
    systemDatabase.garbageCollect(System.currentTimeMillis(), null);
    statusList = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(0, statusList.size());

    // Verify GC runs without errors on an empty table
    systemDatabase.garbageCollect(null, 1L);

    // Run workflows, wait, run them again
    for (int i = 0; i < numWorkflows; i++) {
      int result = proxy.testWorkflow(i);
      assertEquals(i, result);
    }

    Thread.sleep(1000L);

    for (int i = 0; i < numWorkflows; i++) {
      int result = proxy.testWorkflow(i);
      assertEquals(i, result);
    }

    // GC the first half, verify only half were GC'ed
    systemDatabase.garbageCollect(System.currentTimeMillis() - 1000, null);
    statusList = systemDatabase.listWorkflows(new ListWorkflowsInput());
    assertEquals(numWorkflows, statusList.size());
  }

  @Test
  void globalTimeout() throws Exception {
    int numWorkflows = 10;

    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    List<WorkflowHandle<String, ?>> handles = new ArrayList<>();
    for (int i = 0; i < numWorkflows; i++) {
      handles.add(dbos.startWorkflow(() -> proxy.timeoutBlockedWorkflow()));
    }

    Thread.sleep(1000L);

    // Wait one second, start one final workflow, then timeout all workflows started
    // more than one second ago
    WorkflowHandle<String, ?> finalHandle =
        dbos.startWorkflow(() -> proxy.timeoutBlockedWorkflow());

    dbosExecutor.globalTimeout(System.currentTimeMillis() - 1000);
    for (var handle : handles) {
      assertEquals(WorkflowState.CANCELLED.toString(), handle.getStatus().status());
    }
    impl.timeoutLatch.countDown();
    assertEquals(finalHandle.workflowId(), finalHandle.getResult());
  }
}
