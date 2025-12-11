package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface GCTestService {

  int testWorkflow(int x);

  String gcBlockedWorkflow() throws InterruptedException;

  String timeoutBlockedWorkflow();
}

class GCTestServiceImpl implements GCTestService {

  public CountDownLatch gcLatch = new CountDownLatch(1);
  public CountDownLatch timeoutLatch = new CountDownLatch(1);

  @Workflow
  @Override
  public int testWorkflow(int x) {
    DBOS.runStep(() -> x, "testStep");
    return x;
  }

  @Workflow
  @Override
  public String gcBlockedWorkflow() throws InterruptedException {
    gcLatch.await();
    return DBOS.workflowId();
  }

  @Workflow
  @Override
  public String timeoutBlockedWorkflow() {
    while (timeoutLatch.getCount() > 0) {
      DBOS.sleep(Duration.ofMillis(100));
    }
    return DBOS.workflowId();
  }
}

@org.junit.jupiter.api.Timeout(value = 2, unit = TimeUnit.MINUTES)
public class GarbageCollectionTest {

  private static DBOSConfig dbosConfig;
  private GCTestServiceImpl impl;
  private GCTestService proxy;

  @BeforeAll
  static void onetimeSetup() throws Exception {
    dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withMaximumPoolSize(2);
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    DBOS.reinitialize(dbosConfig);

    impl = new GCTestServiceImpl();
    proxy = DBOS.registerWorkflows(GCTestService.class, impl);

    DBOS.launch();
  }

  @AfterEach
  void afterEachTest() throws Exception {
    DBOS.shutdown();
  }

  @Test
  void garbageCollection() throws Exception {
    int numWorkflows = 10;

    var systemDatabase = DBOSTestAccess.getSystemDatabase();

    // Start one blocked workflow and 10 normal workflows
    WorkflowHandle<String, ?> handle = DBOS.startWorkflow(() -> proxy.gcBlockedWorkflow());
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

    var dbosExecutor = DBOSTestAccess.getDbosExecutor();

    List<WorkflowHandle<String, ?>> handles = new ArrayList<>();
    for (int i = 0; i < numWorkflows; i++) {
      handles.add(DBOS.startWorkflow(() -> proxy.timeoutBlockedWorkflow()));
    }

    Thread.sleep(1000L);

    // Wait one second, start one final workflow, then timeout all workflows started
    // more than one second ago
    WorkflowHandle<String, ?> finalHandle =
        DBOS.startWorkflow(() -> proxy.timeoutBlockedWorkflow());

    dbosExecutor.globalTimeout(System.currentTimeMillis() - 1000);
    for (var handle : handles) {
      assertEquals(WorkflowState.CANCELLED.toString(), handle.getStatus().status());
    }
    impl.timeoutLatch.countDown();
    assertEquals(finalHandle.workflowId(), finalHandle.getResult());
  }
}
