package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;

import java.sql.*;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
class RecoveryServiceTest {

  private static DBOSConfig dbosConfig;
  private static DataSource dataSource;
  private DBOS.Instance dbos;
  private Queue testQueue;
  private SystemDatabase systemDatabase;
  private DBOSExecutor dbosExecutor;
  private ExecutingServiceImpl executingServiceImpl;
  private ExecutingService executingService;
  private static final Logger logger = LoggerFactory.getLogger(RecoveryServiceTest.class);

  @BeforeAll
  public static void onetimeBefore() {

    RecoveryServiceTest.dbosConfig =
        new DBOSConfig.Builder()
            .appName("systemdbtest")
            .databaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .dbUser("postgres")
            .maximumPoolSize(2)
            .build();
  }

  @BeforeEach
  void setUp() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    RecoveryServiceTest.dataSource = SystemDatabase.createDataSource(dbosConfig);

    dbos = DBOS.reinitialize(dbosConfig);
    executingService =
        DBOS.registerWorkflows(
            ExecutingService.class, executingServiceImpl = new ExecutingServiceImpl());
    executingService.setExecutingService(executingService);

    testQueue = dbos.Queue("q1").build();

    DBOS.launch();
    systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
    dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    DBOS.shutdown();
  }

  @Test
  void recoverWorkflows() throws Exception {

    String wfid = "wf-123";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      executingService.workflowMethod("test-item");
    }
    wfid = "wf-124";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      executingService.workflowMethod("test-item");
    }
    wfid = "wf-125";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      executingService.workflowMethod("test-item");
    }
    wfid = "wf-126";
    WorkflowHandle<String, ?> handle6 = null;
    try (var id = new WorkflowOptions(wfid).setContext()) {
      handle6 = DBOS.startWorkflow(() -> executingService.workflowMethod("test-item"));
    }
    handle6.getResult();

    wfid = "wf-127";
    var options = new StartWorkflowOptions(wfid).withQueue(testQueue);
    var handle7 = DBOS.startWorkflow(() -> executingService.workflowMethod("test-item"), options);
    assertEquals("q1", handle7.getStatus().queueName());
    handle7.getResult();

    setWorkflowStateToPending(dataSource);

    List<GetPendingWorkflowsOutput> pending =
        systemDatabase.getPendingWorkflows(dbosExecutor.executorId(), dbosExecutor.appVersion());

    assertEquals(5, pending.size());

    for (GetPendingWorkflowsOutput output : pending) {
      WorkflowHandle<?, ?> handle = dbosExecutor.recoverWorkflow(output);
      handle.getResult();
      assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());
    }
  }

  @Test
  void recoverPendingWorkflows() throws Exception {
    executingService.workflowMethod("test-item");
    executingService.workflowMethod("test-item");
    executingService.workflowMethod("test-item");
    WorkflowHandle<String, ?> handle6 = null;
    try (var id = new WorkflowOptions("wf-126").setContext()) {
      handle6 = DBOS.startWorkflow(() -> executingService.workflowMethod("test-item"));
    }
    handle6.getResult();

    var options = new StartWorkflowOptions("wf-127").withQueue(testQueue);
    var handle7 = DBOS.startWorkflow(() -> executingService.workflowMethod("test-item"), options);
    assertEquals("q1", handle7.getStatus().queueName());
    assertEquals("wf-126", handle6.getWorkflowId());
    assertEquals("wf-127", handle7.getWorkflowId());

    handle7.getResult();

    setWorkflowStateToPending(dataSource);

    List<WorkflowHandle<?, ?>> pending = dbosExecutor.recoverPendingWorkflows(null);
    assertEquals(5, pending.size());

    for (var handle : pending) {
      handle.getResult();
      assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().status());
    }
  }

  @Test
  public void recoveryThreadTest() throws Exception {

    String wfid = "wf-123";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      executingService.workflowMethod("test-item");
    }
    wfid = "wf-124";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      executingService.workflowMethod("test-item");
    }

    setWorkflowStateToPending(dataSource);

    var s = systemDatabase.getWorkflowStatus("wf-123");
    assertTrue(s.isPresent());
    assertEquals(WorkflowState.PENDING.name(), s.get().status());

    DBOS.shutdown();

    dbos = DBOS.reinitialize(dbosConfig);
    // dbos = DBOS.getInstance();

    // need to register again
    // towatch: we are registering after launch. could lead to a race condition
    // toimprove : allow registration before launch
    executingService = DBOS.registerWorkflows(ExecutingService.class, new ExecutingServiceImpl());

    DBOS.launch();

    var h = DBOS.retrieveWorkflow("wf-123");
    h.getResult();
    assertEquals(WorkflowState.SUCCESS.name(), h.getStatus().status());

    h = DBOS.retrieveWorkflow("wf-124");
    h.getResult();
    assertEquals(WorkflowState.SUCCESS.name(), h.getStatus().status());
  }

  @Test
  public void testRecoverNoOutputSteps() throws Exception {
    // Run a workflow that will run a step that throws, and run a no-result step
    //   in the catch handler.
    // Check that this returns null (void) and that the right calls were made.
    String wfid = "wftr-1x3";
    try (var id = new WorkflowOptions(wfid).setContext()) {
      executingService.workflowWithNoResultSteps();
    }
    assertEquals(executingServiceImpl.callsToThrowStep, 1);
    assertEquals(executingServiceImpl.callsToNoReturnStep, 1);
    var h = DBOS.retrieveWorkflow(wfid);
    assertNull(h.getStatus().error());
    assertNull(h.getResult());

    // Recover workflow
    // This should use checkpointed step values
    DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
    h = dbosExecutor.executeWorkflowById(wfid);
    assertNull(h.getStatus().error());
    assertNull(h.getResult());
    assertEquals(executingServiceImpl.callsToThrowStep, 1);
    assertEquals(executingServiceImpl.callsToNoReturnStep, 1);

    // Recover workflow net of last step
    // This should use 1 checkpointed step value
    DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
    DBUtils.deleteStepOutput(dataSource, wfid, 1);
    h = dbosExecutor.executeWorkflowById(wfid);
    assertNull(h.getStatus().error());
    assertNull(h.getResult());
    assertEquals(executingServiceImpl.callsToThrowStep, 1);
    assertEquals(executingServiceImpl.callsToNoReturnStep, 2);
  }

  private void setWorkflowStateToPending(DataSource ds) throws SQLException {

    String sql = "UPDATE dbos.workflow_status SET status = ?, updated_at = ? ;";

    try (Connection connection = ds.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(sql)) {

      pstmt.setString(1, WorkflowState.PENDING.name());
      pstmt.setLong(2, Instant.now().toEpochMilli());

      // Execute the update and get the number of rows affected
      int rowsAffected = pstmt.executeUpdate();

      logger.info("Number of workflows made pending " + rowsAffected);
    }
  }
}
