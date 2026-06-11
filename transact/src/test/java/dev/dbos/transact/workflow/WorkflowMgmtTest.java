package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowMgmtTest {

  private static final Logger logger = LoggerFactory.getLogger(WorkflowMgmtTest.class);

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;

  private MgmtService proxy;
  private MgmtServiceImpl impl;
  private Queue myqueue;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig().withAppVersion("v1.0.0");
    dbos = new DBOS(dbosConfig);
    dataSource = pgContainer.dataSource();

    impl = new MgmtServiceImpl(dbos);
    proxy = dbos.registerProxy(MgmtService.class, impl);
    impl.proxy = proxy;

    myqueue = new Queue("myqueue");
    dbos.registerQueue(myqueue);

    dbos.launch();
  }

  @Test
  public void testStepTiming() throws Exception {
    var start = System.currentTimeMillis();
    var handle = dbos.startWorkflow(() -> proxy.stepTimingWorkflow());
    assertDoesNotThrow(() -> handle.getResult());

    var steps = dbos.listWorkflowSteps(handle.workflowId());
    assertEquals(9, steps.size());
    for (var step : steps) {
      assertNotNull(step.startedAtEpochMs());
      assertNotNull(step.completedAtEpochMs());
      assert (step.startedAtEpochMs() >= start);
      assert (step.completedAtEpochMs() >= step.startedAtEpochMs());
      if (step.functionName().equals("stepTimingStep")) {
        assert (step.completedAtEpochMs() - step.startedAtEpochMs() >= 100);
      }
    }
  }

  @Test
  public void asyncCancelResumeTest() throws Exception {
    String workflowId = "asyncCancelResumeTest:%d".formatted(System.currentTimeMillis());
    var options = new StartWorkflowOptions(workflowId);
    WorkflowHandle<Integer, ?> h = dbos.startWorkflow(() -> proxy.simpleWorkflow(23), options);

    impl.mainLatch.await();
    dbos.cancelWorkflow(workflowId);
    impl.workLatch.countDown();

    assertEquals(1, impl.stepsExecuted());
    assertEquals(WorkflowState.CANCELLED, h.getStatus().status());

    WorkflowHandle<Integer, ?> handle = dbos.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());

    // resume again

    handle = dbos.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());
    h = dbos.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS, h.getStatus().status());

    logger.info("Test completed");
  }

  @Test
  public void syncCancelResumeTest() throws Exception {

    ExecutorService e = Executors.newFixedThreadPool(2);
    String workflowId = "syncCancelResumeTest:%d".formatted(System.currentTimeMillis());

    CountDownLatch testLatch = new CountDownLatch(2);

    e.submit(
        () -> {
          WorkflowOptions options = new WorkflowOptions(workflowId);

          assertThrows(
              DBOSAwaitedWorkflowCancelledException.class,
              () -> {
                try (var o = options.setContext()) {
                  proxy.simpleWorkflow(23);
                }
              });
          assertEquals(1, impl.stepsExecuted());
          testLatch.countDown();
        });

    e.submit(
        () -> {
          impl.mainLatch.await();
          dbos.cancelWorkflow(workflowId);
          impl.workLatch.countDown();
          testLatch.countDown();
          // return a value to force using Callable<T> submit overload
          return null;
        });

    testLatch.await();

    var handle = dbos.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());

    // resume again

    handle = dbos.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());
  }

  @Test
  public void queuedCancelResumeTest() throws Exception {
    String workflowId = "wfid1";
    var options = new StartWorkflowOptions(workflowId).withQueue(myqueue);
    var origHandle = dbos.startWorkflow(() -> proxy.simpleWorkflow(23), options);

    impl.mainLatch.await();
    dbos.cancelWorkflow(workflowId);
    impl.workLatch.countDown();

    assertEquals(1, impl.stepsExecuted());
    var h = dbos.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.CANCELLED, h.getStatus().status());

    var handle = dbos.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());

    // resume again

    handle = dbos.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());

    assertEquals(WorkflowState.SUCCESS, origHandle.getStatus().status());
  }

  @Test
  public void cancelAfterFinalStepTest() throws Exception {
    // A workflow cancelled after its final step completes (but before it returns)
    // must not complete as SUCCESS. CANCELLED is terminal.
    String workflowId = "cancelAfterFinalStepTest:%d".formatted(System.currentTimeMillis());
    var options = new StartWorkflowOptions(workflowId);
    WorkflowHandle<Integer, ?> h =
        dbos.startWorkflow(() -> proxy.cancelAfterFinalStepWorkflow(42), options);

    impl.mainLatch.await();
    dbos.cancelWorkflow(workflowId);
    impl.workLatch.countDown();

    // getResult() blocks until done; CANCELLED status throws DBOSAwaitedWorkflowCancelledException
    assertThrows(DBOSAwaitedWorkflowCancelledException.class, h::getResult);
    assertEquals(1, impl.stepsExecuted());
    assertEquals(WorkflowState.CANCELLED, h.getStatus().status());

    // Resume: completes successfully without re-running the already-recorded step
    WorkflowHandle<Integer, ?> handle = dbos.resumeWorkflow(workflowId);
    assertEquals(42, handle.getResult());
    assertEquals(1, impl.stepsExecuted()); // step not re-run
    assertEquals(WorkflowState.SUCCESS, h.getStatus().status());
  }

  @Test
  public void cancelAfterCompletionTest() throws Exception {
    // Cancelling a workflow that has already completed successfully is a no-op.
    // SUCCESS is a terminal state; cancel must not change it.
    WorkflowHandle<String, ?> h = dbos.startWorkflow(() -> proxy.helloWorkflow("world"));
    assertEquals("Hello, world!", h.getResult());
    assertEquals(WorkflowState.SUCCESS, h.getStatus().status());

    dbos.cancelWorkflow(h.workflowId());

    assertEquals(WorkflowState.SUCCESS, h.getStatus().status());
  }

  @Test
  public void testListWorkflowsDontLoadInputOutput() throws Exception {
    var handle = dbos.startWorkflow(() -> proxy.helloWorkflow("Chuck"));
    assertEquals("Hello, Chuck!", handle.getResult());

    var input =
        new ListWorkflowsInput(handle.workflowId()).withLoadInput(false).withLoadOutput(false);
    var workflows = dbos.listWorkflows(input);
    assertEquals(1, workflows.size());
    var workflow = workflows.get(0);
    assertEquals(handle.workflowId(), workflow.workflowId());
    assertNull(workflow.input());
    assertNull(workflow.output());
    assertNull(workflow.error());
    assertNull(workflow.serialization());
  }

  @Test
  public void testListApplicationVersions() throws Exception {
    var sysdb = DBOSTestAccess.getSystemDatabase(dbos);
    sysdb.createApplicationVersion("v1.0.0");
    sysdb.createApplicationVersion("v2.0.0");
    sysdb.createApplicationVersion("v3.0.0");

    var versions = dbos.listApplicationVersions();
    assertEquals(3, versions.size());

    // createApplicationVersion defaults version_timestamp to insertion order, so all three
    // exist; just verify all names are present
    var names = versions.stream().map(v -> v.versionName()).toList();
    assertTrue(names.contains("v1.0.0"));
    assertTrue(names.contains("v2.0.0"));
    assertTrue(names.contains("v3.0.0"));

    // createdAt should be set and positive on all versions
    for (var v : versions) {
      assertNotNull(v.createdAt());
      assertTrue(v.createdAt().toEpochMilli() > 0);
    }
  }

  @Test
  public void testGetLatestApplicationVersion() throws Exception {
    var sysdb = DBOSTestAccess.getSystemDatabase(dbos);
    sysdb.createApplicationVersion("v1.0.0");
    sysdb.createApplicationVersion("v2.0.0");

    // Promote v1.0.0 to be the latest by updating its timestamp to now
    sysdb.updateApplicationVersionTimestamp("v1.0.0", java.time.Instant.now().plusSeconds(60));

    var latest = dbos.getLatestApplicationVersion();
    assertEquals("v1.0.0", latest.versionName());
    assertNotNull(latest.createdAt());
    assertTrue(latest.createdAt().toEpochMilli() > 0);
  }

  @Test
  @org.junit.jupiter.api.parallel.Execution(
      org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD)
  public void testSetLatestApplicationVersion() throws Exception {
    var sysdb = DBOSTestAccess.getSystemDatabase(dbos);
    sysdb.createApplicationVersion("v1.0.0");
    sysdb.createApplicationVersion("v2.0.0");

    // introduce a slight delay to ensure the v1.0.0 timestamp we're about the set is later than the
    // v2.0.0 we just created
    Thread.sleep(100);

    // Record v1.0.0's createdAt before promoting it
    var v1CreatedAt =
        dbos.listApplicationVersions().stream()
            .filter(v -> v.versionName().equals("v1.0.0"))
            .findFirst()
            .orElseThrow()
            .createdAt();

    // v2.0.0 was inserted last so it should be the current latest; promote v1.0.0
    dbos.setLatestApplicationVersion("v1.0.0");

    var latest = dbos.getLatestApplicationVersion();
    assertEquals("v1.0.0", latest.versionName());

    // setLatestApplicationVersion updates the timestamp but must not change createdAt
    assertEquals(v1CreatedAt, latest.createdAt());

    // v1.0.0 should now sort first in the list (highest timestamp)
    var versions = dbos.listApplicationVersions();
    assertEquals("v1.0.0", versions.get(0).versionName());
  }

  @Test
  public void setWorkflowDelayWithDuration() throws Exception {
    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    long before = System.currentTimeMillis();
    var delay = Duration.ofSeconds(60);
    var handle =
        dbos.startWorkflow(
            () -> proxy.helloWorkflow("world"),
            new StartWorkflowOptions().withQueue(myqueue).withDelay(delay));
    var wfId = handle.workflowId();

    // startWorkflow stores delay_until_epoch_ms as an absolute epoch timestamp
    var row = DBUtils.getWorkflowRow(dataSource, wfId);
    assertEquals(WorkflowState.DELAYED.name(), row.status());
    assertNotNull(row.delayUntilEpochMs());
    assertTrue(row.delayUntilEpochMs() >= before + delay.toMillis() - 1_000);
    assertTrue(row.delayUntilEpochMs() <= before + delay.toMillis() + 1_000);

    // setWorkflowDelay updates delay_until_epoch_ms to a new absolute epoch timestamp
    before = System.currentTimeMillis();
    dbos.setWorkflowDelay(wfId, Duration.ofSeconds(30));

    row = DBUtils.getWorkflowRow(dataSource, wfId);
    assertTrue(row.delayUntilEpochMs() >= before + 29_000);
    assertTrue(row.delayUntilEpochMs() <= before + 31_000);

    // Clear the delay so the workflow can run
    dbos.setWorkflowDelay(wfId, Instant.now().minusSeconds(1));
    qs.unpause();
    assertEquals("Hello, world!", handle.getResult());
  }

  @Test
  public void setWorkflowDelayWithInstant() throws Exception {
    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    long before = System.currentTimeMillis();
    var delay = Duration.ofSeconds(60);
    var handle =
        dbos.startWorkflow(
            () -> proxy.helloWorkflow("world"),
            new StartWorkflowOptions().withQueue(myqueue).withDelay(delay));
    var wfId = handle.workflowId();

    // startWorkflow stores delay_until_epoch_ms as an absolute epoch timestamp
    var row = DBUtils.getWorkflowRow(dataSource, wfId);
    assertEquals(WorkflowState.DELAYED.name(), row.status());
    assertNotNull(row.delayUntilEpochMs());
    assertTrue(row.delayUntilEpochMs() >= before + delay.toMillis() - 1_000);
    assertTrue(row.delayUntilEpochMs() <= before + delay.toMillis() + 1_000);

    // setWorkflowDelay updates delay_until_epoch_ms to a new absolute epoch timestamp
    var targetInstant = Instant.now().plusSeconds(120);
    dbos.setWorkflowDelay(wfId, targetInstant);

    row = DBUtils.getWorkflowRow(dataSource, wfId);
    assertEquals(targetInstant.toEpochMilli(), row.delayUntilEpochMs());

    // Clear the delay so the workflow can run
    dbos.setWorkflowDelay(wfId, Instant.now().minusSeconds(1));
    qs.unpause();
    assertEquals("Hello, world!", handle.getResult());
  }

  @Test
  public void bulkCancelTest() throws Exception {
    // Start 3 concurrent blocking workflows, cancel them all at once, verify none complete.
    var wfIds = new ArrayList<String>();
    var handles = new ArrayList<WorkflowHandle<String, ?>>();

    for (int i = 0; i < 3; i++) {
      var wfId = "bulkCancelTest-%d:%d".formatted(i, System.currentTimeMillis());
      wfIds.add(wfId);
      impl.testMainLatches.put(wfId, new CountDownLatch(1));
      impl.testWorkLatches.put(wfId, new CountDownLatch(1));
      handles.add(
          dbos.startWorkflow(() -> proxy.blockingWorkflow(), new StartWorkflowOptions(wfId)));
    }

    for (var wfId : wfIds) {
      impl.testMainLatches.get(wfId).await();
    }
    assertEquals(3, impl.stepsExecuted()); // only step one ran for each

    dbos.cancelWorkflows(wfIds);

    // Release workflows so they proceed to step two and detect cancellation there
    for (var wfId : wfIds) {
      impl.testWorkLatches.get(wfId).countDown();
    }

    for (var handle : handles) {
      assertThrows(DBOSAwaitedWorkflowCancelledException.class, handle::getResult);
    }
    assertEquals(3, impl.stepsExecuted()); // step two did not run for any
  }

  @Test
  public void cancelChildrenEndToEndTest() throws Exception {
    // Build a running parent→child→grandchild tree and verify cancel_children propagation.
    var parentId = "cancelChildren-parent:%d".formatted(System.currentTimeMillis());
    var childId = "cancelChildren-child:%d".formatted(System.currentTimeMillis());
    var grandchildId = "cancelChildren-grandchild:%d".formatted(System.currentTimeMillis());
    var ids = List.of(parentId, childId, grandchildId);

    for (var id : ids) {
      impl.testMainLatches.put(id, new CountDownLatch(1));
      impl.testWorkLatches.put(id, new CountDownLatch(1));
    }

    var parentHandle =
        dbos.startWorkflow(
            () -> proxy.treeParentWorkflow(childId, grandchildId),
            new StartWorkflowOptions(parentId));

    // Wait until the whole tree is running and blocked
    for (var id : ids) {
      impl.testMainLatches.get(id).await();
    }

    var sysdb = DBOSTestAccess.getSystemDatabase(dbos);
    assertEquals(Set.of(childId, grandchildId), sysdb.getWorkflowChildren(parentId));

    // Cancelling without cancel_children only affects the parent
    dbos.cancelWorkflow(parentId, false);
    assertEquals(WorkflowState.CANCELLED, dbos.retrieveWorkflow(parentId).getStatus().status());
    assertNotEquals(WorkflowState.CANCELLED, dbos.retrieveWorkflow(childId).getStatus().status());
    assertNotEquals(
        WorkflowState.CANCELLED, dbos.retrieveWorkflow(grandchildId).getStatus().status());

    // Cancelling with cancel_children cancels the full subtree
    dbos.cancelWorkflow(parentId, true);
    for (var id : ids) {
      assertEquals(WorkflowState.CANCELLED, dbos.retrieveWorkflow(id).getStatus().status());
    }

    // Release all so each workflow thread can proceed to its step and observe cancellation
    for (var id : ids) {
      impl.testWorkLatches.get(id).countDown();
    }

    assertThrows(DBOSAwaitedWorkflowCancelledException.class, parentHandle::getResult);
  }

  @Test
  public void testListWorkflowsLoadInputOutput() throws Exception {
    var handle = dbos.startWorkflow(() -> proxy.helloWorkflow("Chuck"));
    assertEquals("Hello, Chuck!", handle.getResult());

    // loadInput/loadOutput default to true
    var input = new ListWorkflowsInput(handle.workflowId());
    var workflows = dbos.listWorkflows(input);
    assertEquals(1, workflows.size());
    var workflow = workflows.get(0);
    assertEquals(handle.workflowId(), workflow.workflowId());
    assertEquals(1, workflow.input().length);
    assertEquals("Chuck", workflow.input()[0]);
    assertEquals("Hello, Chuck!", workflow.output());
    assertNull(workflow.error());
    assertEquals("java_jackson", workflow.serialization());
  }
}
