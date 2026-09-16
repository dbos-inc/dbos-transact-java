package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.*;
import java.time.Instant;
import java.util.List;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class RecoveryServiceTest {

  private static final Logger logger = LoggerFactory.getLogger(RecoveryServiceTest.class);

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose HikariDataSource dataSource;

  private String testQueue;

  @BeforeEach
  void setUp() {
    // Pin executor ID and app version explicitly so both DBOS instances in recoveryThreadTest
    // use the same values. This ensures getPendingWorkflows finds the right pending workflows
    // when the second instance recovers them.
    dbosConfig =
        pgContainer
            .dbosConfig()
            .withExecutorId("recovery-test-executor")
            .withAppVersion("recovery-test-version");
    dataSource = pgContainer.dataSource();
    testQueue = "q1";
  }

  private ExecutingService register(DBOS dbos) {
    var impl = new ExecutingServiceImpl(dbos);
    var service = dbos.registerProxy(ExecutingService.class, impl);
    impl.setSelf(service);

    return service;
  }

  @Test
  void recoverWorkflows() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      var executingService = register(dbos);
      dbos.launch();
      dbos.registerQueue(testQueue, QueueOptions.empty());

      var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);
      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

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
      assertEquals("q1", handle7.getStatus().queueName());
      handle7.getResult();

      setWorkflowStateToPending(dataSource);

      var pending =
          systemDatabase.listWorkflows(
              new ListWorkflowsInput()
                  .withStatus(WorkflowState.PENDING)
                  .withExecutorIds(List.of(dbosExecutor.executorId()))
                  .withApplicationVersion(dbosExecutor.appVersion()));

      assertEquals(5, pending.size());

      var recovered = dbosExecutor.recoverPendingWorkflows(List.of(dbosExecutor.executorId()));
      assertEquals(5, recovered.size());

      // Recovery re-enqueued them; the queue is what runs them.
      for (var workflowId : recovered) {
        var handle = dbos.retrieveWorkflow(workflowId);
        handle.getResult();
        assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
      }
    }
  }

  @Test
  void recoverPendingWorkflows() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      var executingService = register(dbos);
      dbos.launch();
      dbos.registerQueue(testQueue, QueueOptions.empty());

      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

      executingService.workflowMethod("test-item");
      executingService.workflowMethod("test-item");
      executingService.workflowMethod("test-item");
      WorkflowHandle<String, ?> handle6 = null;
      try (var id = new WorkflowOptions("wf-126").setContext()) {
        handle6 = dbos.startWorkflow(() -> executingService.workflowMethod("test-item"));
      }
      handle6.getResult();

      var options = new StartWorkflowOptions("wf-127").withQueue(testQueue);
      var handle7 = dbos.startWorkflow(() -> executingService.workflowMethod("test-item"), options);
      assertEquals("q1", handle7.getStatus().queueName());
      assertEquals("wf-126", handle6.workflowId());
      assertEquals("wf-127", handle7.workflowId());

      handle7.getResult();

      setWorkflowStateToPending(dataSource);

      List<String> pending =
          dbosExecutor.recoverPendingWorkflows(List.of(dbosExecutor.executorId()));
      assertEquals(5, pending.size());

      for (var workflowId : pending) {
        var handle = dbos.retrieveWorkflow(workflowId);
        handle.getResult();
        assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
      }
    }
  }

  @Test
  public void recoveryThreadTest() throws Exception {
    String wfid1 = "wf-123";
    String wfid2 = "wf-124";

    try (var dbos = new DBOS(dbosConfig)) {
      var service = register(dbos);
      dbos.launch();

      try (var id = new WorkflowOptions(wfid1).setContext()) {
        service.workflowMethod("test-item");
      }
      try (var id = new WorkflowOptions(wfid2).setContext()) {
        service.workflowMethod("test-item");
      }
    }

    setWorkflowStateToPending(dataSource);

    // Re-launch and check recovery
    try (var dbos = new DBOS(dbosConfig)) {

      var wfRow = DBUtils.getWorkflowRow(dataSource, wfid1);
      assertNotNull(wfRow);
      assertEquals(WorkflowState.PENDING.name(), wfRow.status());

      register(dbos);
      dbos.launch();

      var h = dbos.retrieveWorkflow(wfid1);
      h.getResult();
      assertEquals(WorkflowState.SUCCESS, h.getStatus().status());

      h = dbos.retrieveWorkflow(wfid2);
      h.getResult();
      assertEquals(WorkflowState.SUCCESS, h.getStatus().status());
    }
  }

  @Test
  public void testRecoverNoOutputSteps() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      var executingService = register(dbos);
      dbos.launch();

      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

      // Run a workflow that will run a step that throws, and run a no-result step
      //   in the catch handler.
      // Check that this returns null (void) and that the right calls were made.
      String wfid = "wftr-1x3";
      try (var id = new WorkflowOptions(wfid).setContext()) {
        executingService.workflowWithNoResultSteps();
      }
      var h = dbos.retrieveWorkflow(wfid);
      assertNull(h.getStatus().error());
      assertNull(h.getResult());

      // Recover workflow
      // This should use checkpointed step values
      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
      h = dbosExecutor.executeWorkflowById(wfid);
      assertNull(h.getStatus().error());
      assertNull(h.getResult());

      // Recover workflow net of last step
      // This should use 1 checkpointed step value
      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
      DBUtils.deleteStepOutput(dataSource, wfid, 1);
      h = dbosExecutor.executeWorkflowById(wfid);
      assertNull(h.getStatus().error());
      assertNull(h.getResult());
    }
  }

  @Test
  void recoveryReenqueuesOntoTheInternalQueue() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      var executingService = register(dbos);
      dbos.launch();

      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);
      var queueService = DBOSTestAccess.getQueueService(dbos);

      var wfid = "recovery-reenqueues";
      try (var id = new WorkflowOptions(wfid).setContext()) {
        executingService.workflowMethod("test-item");
      }
      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());

      // Hold the dispatch so the re-enqueued row can be inspected before anything claims it.
      queueService.pause();
      try {
        var recovered = dbosExecutor.recoverPendingWorkflows(List.of(dbosExecutor.executorId()));
        assertEquals(List.of(wfid), recovered);

        // A workflow that never had a queue of its own goes onto the internal one, and gives up
        // the start time the previous run stamped: it has not started on its new runner yet.
        var row = DBUtils.getWorkflowRow(dataSource, wfid);
        assertEquals(WorkflowState.ENQUEUED.name(), row.status());
        assertEquals(Constants.DBOS_INTERNAL_QUEUE, row.queueName());
        assertNull(row.startedAtEpochMs());
      } finally {
        queueService.unpause();
      }

      var handle = dbos.retrieveWorkflow(wfid);
      handle.getResult();
      assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
    }
  }

  @Test
  void recoveryLeavesOtherExecutorsWorkflowsAlone() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      var executingService = register(dbos);
      dbos.launch();

      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

      var wfid = "recovery-names-executors";
      try (var id = new WorkflowOptions(wfid).setContext()) {
        executingService.workflowMethod("test-item");
      }
      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());

      // Naming an executor that owns nothing moves nothing. This is what makes a repeated
      // recovery request cost nothing: once a live executor has claimed a row, a later sweep for
      // the executor it was recovered from no longer matches it.
      assertEquals(List.of(), dbosExecutor.recoverPendingWorkflows(List.of("some-other-executor")));
      assertEquals(WorkflowState.PENDING.name(), DBUtils.getWorkflowRow(dataSource, wfid).status());
    }
  }

  @Test
  void aDeadLetteredWorkflowDoesNotStrandTheRestOfItsBatch() throws Exception {
    // The queue's claim counts a dispatch, and a workflow that has exhausted its attempts is
    // dead-lettered when that dispatch reaches it, which then throws. That must not abandon the
    // workflows dispatched alongside it (#461).
    try (var dbos = new DBOS(dbosConfig)) {
      var executingService = register(dbos);
      dbos.launch();

      var dbosExecutor = DBOSTestAccess.getDbosExecutor(dbos);

      var ids = List.of("wf-dlq-before", "wf-dlq-doomed", "wf-dlq-after");
      for (var id : ids) {
        try (var ctx = new WorkflowOptions(id).setContext()) {
          executingService.workflowMethod("test-item");
        }
      }

      setWorkflowStateToPending(dataSource);
      // Past DEFAULT_MAX_RECOVERY_ATTEMPTS, so the dispatch that claims it dead-letters it.
      setRecoveryAttempts(dataSource, "wf-dlq-doomed", 1000);

      // Recovery itself has nothing to say about any of this: it re-enqueues all three.
      var recovered = dbosExecutor.recoverPendingWorkflows(List.of(dbosExecutor.executorId()));
      assertEquals(3, recovered.size());

      for (var id : List.of("wf-dlq-before", "wf-dlq-after")) {
        var handle = dbos.retrieveWorkflow(id);
        handle.getResult();
        assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());
      }

      String doomedStatus = null;
      for (var i = 0; i < 300; i++) {
        doomedStatus = DBUtils.getWorkflowRow(dataSource, "wf-dlq-doomed").status();
        if (WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED.name().equals(doomedStatus)) {
          break;
        }
        Thread.sleep(100);
      }
      assertEquals(
          WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED.name(),
          doomedStatus,
          "the doomed workflow must have been dead-lettered");
    }
  }

  private static void setRecoveryAttempts(DataSource ds, String workflowId, int attempts)
      throws SQLException {
    try (var conn = ds.getConnection();
        var stmt =
            conn.prepareStatement(
                "UPDATE dbos.workflow_status SET recovery_attempts = ? WHERE workflow_uuid = ?")) {
      stmt.setInt(1, attempts);
      stmt.setString(2, workflowId);
      assertEquals(1, stmt.executeUpdate());
    }
  }

  private void setWorkflowStateToPending(DataSource ds) throws SQLException {

    String sql = "UPDATE dbos.workflow_status SET status = ?, updated_at = ? ;";

    try (Connection connection = ds.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(sql)) {

      pstmt.setString(1, WorkflowState.PENDING.name());
      pstmt.setLong(2, Instant.now().toEpochMilli());

      // Execute the update and get the number of rows affected
      int rowsAffected = pstmt.executeUpdate();

      logger.info("Number of workflows made pending {}", rowsAffected);
    }
  }
}
