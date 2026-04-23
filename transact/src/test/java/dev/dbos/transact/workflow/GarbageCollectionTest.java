package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.PgContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class GarbageCollectionTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;

  private GCTestServiceImpl impl;
  private GCTestService proxy;
  private Queue gcQueue;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);

    impl = new GCTestServiceImpl(dbos);
    proxy = dbos.registerProxy(GCTestService.class, impl);

    gcQueue = new Queue("gcqueue");
    dbos.registerQueue(gcQueue);

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
    List<WorkflowStatus> statusList = systemDatabase.listWorkflows(null);
    assertEquals(11, statusList.size());
    systemDatabase.garbageCollect(null, 1L);
    statusList = systemDatabase.listWorkflows(null);
    assertEquals(2, statusList.size());
    assertEquals(handle.workflowId(), statusList.get(0).workflowId());

    // Garbage collect all completed workflows
    systemDatabase.garbageCollect(Instant.now(), null);
    statusList = systemDatabase.listWorkflows(null);
    assertEquals(1, statusList.size());
    assertEquals(handle.workflowId(), statusList.get(0).workflowId());

    // Finish the blocked workflow, garbage collect everything
    impl.gcLatch.countDown();
    assertEquals(handle.workflowId(), handle.getResult());
    systemDatabase.garbageCollect(Instant.now(), null);
    statusList = systemDatabase.listWorkflows(null);
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
    systemDatabase.garbageCollect(Instant.now().minus(Duration.ofMillis(1000)), null);
    statusList = systemDatabase.listWorkflows(null);
    assertEquals(numWorkflows, statusList.size());
  }

  @Test
  void gcPreservesDelayedAndEnqueued() throws Exception {
    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    // Create a DELAYED workflow (long delay, won't run)
    var delayedHandle =
        dbos.startWorkflow(
            () -> proxy.testWorkflow(1),
            new StartWorkflowOptions().withQueue(gcQueue).withDelay(Duration.ofHours(1)));

    // Create an ENQUEUED workflow (stays ENQUEUED since QueueService is paused)
    var enqueuedHandle =
        dbos.startWorkflow(
            () -> proxy.testWorkflow(2), new StartWorkflowOptions().withQueue(gcQueue));

    // Run some completed workflows
    proxy.testWorkflow(3);
    proxy.testWorkflow(4);

    List<WorkflowStatus> statusList = systemDatabase.listWorkflows(null);
    assertEquals(4, statusList.size());

    // GC all completed workflows
    systemDatabase.garbageCollect(Instant.now(), null);

    // DELAYED and ENQUEUED should survive; completed ones should be gone
    statusList = systemDatabase.listWorkflows(null);
    assertEquals(2, statusList.size());
    var survivingIds = statusList.stream().map(WorkflowStatus::workflowId).toList();
    assertTrue(survivingIds.contains(delayedHandle.workflowId()));
    assertTrue(survivingIds.contains(enqueuedHandle.workflowId()));

    qs.unpause();
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

    dbosExecutor.globalTimeout(Instant.now().minus(Duration.ofMillis(1000)));
    for (var handle : handles) {
      assertEquals(WorkflowState.CANCELLED, handle.getStatus().status());
    }
    impl.timeoutLatch.countDown();
    assertEquals(finalHandle.workflowId(), finalHandle.getResult());
  }

  @Test
  void globalTimeoutCancelsDelayed() throws Exception {
    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

    // Create delayed workflows that should be cancelled
    int numDelayed = 5;
    List<WorkflowHandle<Integer, ?>> delayedHandles = new ArrayList<>();
    for (int i = 0; i < numDelayed; i++) {
      delayedHandles.add(
          dbos.startWorkflow(
              () -> proxy.testWorkflow(0),
              new StartWorkflowOptions().withQueue(gcQueue).withDelay(Duration.ofHours(1))));
    }

    Thread.sleep(1000L);

    // Start one final delayed workflow after the sleep
    WorkflowHandle<Integer, ?> finalDelayedHandle =
        dbos.startWorkflow(
            () -> proxy.testWorkflow(0),
            new StartWorkflowOptions().withQueue(gcQueue).withDelay(Duration.ofHours(1)));

    // Timeout all workflows created more than one second ago
    dbosExecutor.globalTimeout(Instant.now().minus(Duration.ofMillis(1000)));

    // All early delayed workflows should be cancelled
    for (var handle : delayedHandles) {
      assertEquals(WorkflowState.CANCELLED, handle.getStatus().status());
    }

    // The final delayed workflow should still be DELAYED (not cancelled)
    assertEquals(WorkflowState.DELAYED, finalDelayedHandle.getStatus().status());

    qs.unpause();
  }
}
