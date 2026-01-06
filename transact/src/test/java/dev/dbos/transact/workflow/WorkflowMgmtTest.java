package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

interface MgmtService {
  int simpleWorkflow(int input) throws InterruptedException;

  void stepTimingWorkflow() throws InterruptedException;
}

class MgmtServiceImpl implements MgmtService {

  private int stepsExecuted;
  public CountDownLatch mainLatch = new CountDownLatch(1);
  public CountDownLatch workLatch = new CountDownLatch(1);

  public int stepsExecuted() {
    return this.stepsExecuted;
  }

  @Workflow(name = "myworkflow")
  public int simpleWorkflow(int input) throws InterruptedException {

    DBOS.runStep(() -> ++stepsExecuted, "stepOne");
    mainLatch.countDown();
    workLatch.await();
    DBOS.runStep(() -> ++stepsExecuted, "stepTwo");
    DBOS.runStep(() -> ++stepsExecuted, "stepThree");

    return input;
  }

  @Workflow
  public void stepTimingWorkflow() throws InterruptedException {
    for (var i = 0; i < 5; i++) {
      DBOS.runStep(() -> Thread.sleep(100), "stepTimingStep");
    }

    DBOS.setEvent("key", "value");
    DBOS.listWorkflows(new ListWorkflowsInput());
    DBOS.recv(null, Duration.ofSeconds(1));
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class WorkflowMgmtTest {

  private static final Logger logger = LoggerFactory.getLogger(WorkflowMgmtTest.class);

  private static DBOSConfig dbosConfig;

  private MgmtService proxy;
  private MgmtServiceImpl impl;
  private Queue myqueue;

  @BeforeAll
  static void onetimeSetup() throws Exception {

    WorkflowMgmtTest.dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withMaximumPoolSize(2);
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    DBOS.reinitialize(dbosConfig);

    impl = new MgmtServiceImpl();
    proxy = DBOS.registerWorkflows(MgmtService.class, impl);

    myqueue = new Queue("myqueue");
    DBOS.registerQueue(myqueue);

    DBOS.launch();
  }

  @AfterEach
  void afterEachTest() throws Exception {
    DBOS.shutdown();
  }

  @Test
  public void testStepTiming() throws Exception {
    var start = System.currentTimeMillis();
    var handle = DBOS.startWorkflow(() -> proxy.stepTimingWorkflow());
    assertDoesNotThrow(() -> handle.getResult());

    var steps = DBOS.listWorkflowSteps(handle.workflowId());
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
    WorkflowHandle<Integer, ?> h = DBOS.startWorkflow(() -> proxy.simpleWorkflow(23), options);

    impl.mainLatch.await();
    DBOS.cancelWorkflow(workflowId);
    impl.workLatch.countDown();

    assertEquals(1, impl.stepsExecuted());
    assertEquals(WorkflowState.CANCELLED.name(), h.getStatus().status());

    WorkflowHandle<Integer, ?> handle = DBOS.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());

    // resume again

    handle = DBOS.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());
    h = DBOS.retrieveWorkflow(workflowId);
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
          DBOS.cancelWorkflow(workflowId);
          impl.workLatch.countDown();
          testLatch.countDown();
          // return a value to force using Callable<T> submit overload
          return null;
        });

    testLatch.await();

    var handle = DBOS.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());

    // resume again

    handle = DBOS.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());
  }

  @Test
  public void queuedCancelResumeTest() throws Exception {
    String workflowId = "wfid1";
    var options = new StartWorkflowOptions(workflowId).withQueue(myqueue);
    var origHandle = DBOS.startWorkflow(() -> proxy.simpleWorkflow(23), options);

    impl.mainLatch.await();
    DBOS.cancelWorkflow(workflowId);
    impl.workLatch.countDown();

    assertEquals(1, impl.stepsExecuted());
    var h = DBOS.retrieveWorkflow(workflowId);
    assertEquals(WorkflowState.CANCELLED.name(), h.getStatus().status());

    var handle = DBOS.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());

    // resume again

    handle = DBOS.resumeWorkflow(workflowId);

    assertEquals(23, handle.getResult());
    assertEquals(3, impl.stepsExecuted());

    assertEquals(WorkflowState.SUCCESS.name(), origHandle.getStatus().status());
  }
}
