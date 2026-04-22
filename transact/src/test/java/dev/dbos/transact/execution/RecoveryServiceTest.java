package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Queue;
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

  private Queue testQueue;

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
    testQueue = new Queue("q1");
  }

  private ExecutingService register(DBOS dbos) {
    var impl = new ExecutingServiceImpl(dbos);
    var service = dbos.registerProxy(ExecutingService.class, impl);
    impl.setSelf(service);
    dbos.registerQueue(testQueue);
    return service;
  }

  @Test
  void recoverWorkflows() throws Exception {
    try (var dbos = new DBOS(dbosConfig)) {
      var executingService = register(dbos);
      dbos.launch();

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
          systemDatabase.getPendingWorkflows(
              List.of(dbosExecutor.executorId()), dbosExecutor.appVersion());

      assertEquals(5, pending.size());

      for (var output : pending) {
        WorkflowHandle<?, ?> handle =
            dbosExecutor.recoverWorkflow(output.workflowId(), output.queueName());
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

      List<WorkflowHandle<?, ?>> pending =
          dbosExecutor.recoverPendingWorkflows(List.of(dbosExecutor.executorId()));
      assertEquals(5, pending.size());

      for (var handle : pending) {
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
      h = dbosExecutor.executeWorkflowById(wfid, true, false);
      assertNull(h.getStatus().error());
      assertNull(h.getResult());

      // Recover workflow net of last step
      // This should use 1 checkpointed step value
      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.PENDING.name());
      DBUtils.deleteStepOutput(dataSource, wfid, 1);
      h = dbosExecutor.executeWorkflowById(wfid, true, false);
      assertNull(h.getStatus().error());
      assertNull(h.getResult());
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
