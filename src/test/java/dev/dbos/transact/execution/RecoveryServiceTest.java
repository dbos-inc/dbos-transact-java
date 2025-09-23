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
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;

import java.sql.*;
import java.time.Instant;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RecoveryServiceTest {

  private static DBOSConfig dbosConfig;
  private static DataSource dataSource;
  private DBOS dbos;
  private Queue testQueue;
  private SystemDatabase systemDatabase;
  private DBOSExecutor dbosExecutor;
  private ExecutingServiceImpl executingServiceImpl;
  private ExecutingService executingService;
  Logger logger = LoggerFactory.getLogger(RecoveryServiceTest.class);

  @BeforeAll
  public static void onetimeBefore() throws SQLException {

    RecoveryServiceTest.dbosConfig =
        new DBOSConfig.Builder()
            .name("systemdbtest")
            .dbHost("localhost")
            .dbPort(5432)
            .dbUser("postgres")
            .sysDbName("dbos_java_sys")
            .maximumPoolSize(2)
            .build();
  }

  @BeforeEach
  void setUp() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    RecoveryServiceTest.dataSource = SystemDatabase.createDataSource(dbosConfig);

    dbos = DBOS.initialize(dbosConfig);
    executingService =
        dbos.<ExecutingService>Workflow()
            .interfaceClass(ExecutingService.class)
            .implementation(executingServiceImpl = new ExecutingServiceImpl())
            .build();
    executingService.setExecutingService(executingService);

    testQueue = dbos.Queue("q1").build();

    dbos.launch();
    systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
    dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dbos.shutdown();
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
      handle6 = dbos.startWorkflow(() -> executingService.workflowMethod("test-item"));
    }
    handle6.getResult();

    wfid = "wf-127";
    var options = new StartWorkflowOptions(wfid).withQueue(testQueue);
    var handle7 = dbos.startWorkflow(() -> executingService.workflowMethod("test-item"), options);
    assertEquals("q1", handle7.getStatus().getQueueName());
    handle7.getResult();

    setWorkflowStateToPending(dataSource);

    List<GetPendingWorkflowsOutput> pending =
        systemDatabase.getPendingWorkflows(
            dbosExecutor.getExecutorId(), dbosExecutor.getAppVersion());

    assertEquals(5, pending.size());

    for (GetPendingWorkflowsOutput output : pending) {
      WorkflowHandle<?, ?> handle = dbosExecutor.recoverWorkflow(output);
      handle.getResult();
      assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());
    }
  }

  @Test
  void recoverPendingWorkflows() throws Exception {

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
      handle6 = dbos.startWorkflow(() -> executingService.workflowMethod("test-item"));
    }
    handle6.getResult();

    wfid = "wf-127";
    var options = new StartWorkflowOptions(wfid).withQueue(testQueue);
    var handle7 = dbos.startWorkflow(() -> executingService.workflowMethod("test-item"), options);
    assertEquals("q1", handle7.getStatus().getQueueName());
    handle7.getResult();

    setWorkflowStateToPending(dataSource);

    List<WorkflowHandle<?, ?>> pending = dbosExecutor.recoverPendingWorkflows(null);
    assertEquals(5, pending.size());

    for (var handle : pending) {
      handle.getResult();
      assertEquals(WorkflowState.SUCCESS.name(), handle.getStatus().getStatus());
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

    WorkflowStatus s = systemDatabase.getWorkflowStatus("wf-123");
    assertEquals(WorkflowState.PENDING.name(), s.getStatus());

    dbos.shutdown();

    dbos = DBOS.initialize(dbosConfig);
    // dbos = DBOS.getInstance();

    // need to register again
    // towatch: we are registering after launch. could lead to a race condition
    // toimprove : allow registration before launch
    executingService =
        dbos.<ExecutingService>Workflow()
            .interfaceClass(ExecutingService.class)
            .implementation(new ExecutingServiceImpl())
            .build();

    dbos.launch();

    var h = dbos.retrieveWorkflow("wf-123");
    h.getResult();
    assertEquals(WorkflowState.SUCCESS.name(), h.getStatus().getStatus());

    h = dbos.retrieveWorkflow("wf-124");
    h.getResult();
    assertEquals(WorkflowState.SUCCESS.name(), h.getStatus().getStatus());
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
    var h = dbos.retrieveWorkflow(wfid);
    assertNull(h.getStatus().getError());
    assertNull(h.getResult());

    // Recover workflow
    // This should use checkpointed step values
    DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
    h = dbosExecutor.executeWorkflowById(wfid);
    assertNull(h.getStatus().getError());
    assertNull(h.getResult());
    assertEquals(executingServiceImpl.callsToThrowStep, 1);
    assertEquals(executingServiceImpl.callsToNoReturnStep, 1);

    // Recover workflow net of last step
    // This should use 1 checkpointed step value
    DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
    DBUtils.deleteStepOutput(dataSource, wfid, 1);
    h = dbosExecutor.executeWorkflowById(wfid);
    assertNull(h.getStatus().getError());
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
