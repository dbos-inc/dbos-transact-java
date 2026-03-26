package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.utils.PgContainer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class WorkflowMgmtTest {

  private static final Logger logger = LoggerFactory.getLogger(WorkflowMgmtTest.class);

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;

  private MgmtService proxy;
  private MgmtServiceImpl impl;
  private Queue myqueue;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig().withAppVersion("v1.0.0");
    dbos = new DBOS(dbosConfig);

    impl = new MgmtServiceImpl(dbos);
    proxy = dbos.registerWorkflows(MgmtService.class, impl);

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
    assertEquals(WorkflowState.CANCELLED.name(), h.getStatus().status());

    WorkflowHandle<Integer, ?> handle = dbos.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());

    // resume again

    handle = dbos.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());
    h = dbos.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), h.getStatus().status());

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
    assertEquals(WorkflowState.CANCELLED.name(), h.getStatus().status());

    var handle = dbos.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());

    // resume again

    handle = dbos.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());

    assertEquals(WorkflowState.SUCCESS.name(), origHandle.getStatus().status());
  }

  @Test
  public void testListWorkflowsDontLoadInputOutput() throws Exception {
    var handle = dbos.startWorkflow(() -> proxy.helloWorkflow("Chuck"));
    assertEquals("Hello, Chuck!", handle.getResult());

    var input =
        new ListWorkflowsInput()
            .withWorkflowId(handle.workflowId())
            .withLoadInput(false)
            .withLoadOutput(false);
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

    // v2.0.0 was inserted last so it should be the current latest; promote v1.0.0
    dbos.setLatestApplicationVersion("v1.0.0");

    var latest = dbos.getLatestApplicationVersion();
    assertEquals("v1.0.0", latest.versionName());
  }

  @Test
  public void testListWorkflowsLoadInputOutput() throws Exception {
    var handle = dbos.startWorkflow(() -> proxy.helloWorkflow("Chuck"));
    assertEquals("Hello, Chuck!", handle.getResult());

    // loadInput/loadOutput default to true
    var input = new ListWorkflowsInput().withWorkflowId(handle.workflowId());
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
