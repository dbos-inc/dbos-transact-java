package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.dao.StepsDAO;
import dev.dbos.transact.database.dao.WorkflowDAO;
import dev.dbos.transact.exceptions.DBOSStepNondeterminismException;
import dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.WorkflowStatusInternalBuilder;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A step checkpoint is written once per (workflow, step), and only by the execution that owns the
 * workflow. A second write of the same row is the same call retried after its acknowledgement was
 * lost, and succeeds. A different row, written past the owner check, is the execution's own
 * nondeterminism. A write from an execution that no longer owns the workflow is a conflict, and
 * records nothing.
 */
public class RecordStepResultTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose SystemDatabase sysdb;
  @AutoClose HikariDataSource dataSource;
  DbContext ctx;
  String schema;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    MigrationManager.runMigrations(dbosConfig);
    sysdb = SystemDatabase.create(dbosConfig, null, dbosConfig.appName());
    dataSource = pgContainer.dataSource();
    schema = SystemDatabase.sanitizeSchema(dbosConfig.databaseSchema());
    ctx =
        new DbContext(
            dataSource,
            schema,
            null,
            () -> false,
            "test-executor",
            dbosConfig.appName(),
            new PollingLimiter(0));
  }

  // Starts the workflow directly and returns the token its execution owns it with.
  private String createWorkflow(String workflowId) {
    return sysdb
        .initWorkflowStatus(
            new WorkflowStatusInternalBuilder()
                .workflowId(workflowId)
                .workflowName("TestWorkflow")
                .appVersion("1.0.0")
                .priority(0)
                .build(),
            null,
            null)
        .ownerXid();
  }

  private static StepResult output(String workflowId, String value) {
    return new StepResult(workflowId, 0, "step", value, null, null, null);
  }

  private static StepResult child(String workflowId, String childId) {
    return new StepResult(workflowId, 0, "ChildWorkflow", null, null, null, null)
        .withChildWorkflowId(childId);
  }

  private record Row(String output, String childWorkflowId) {}

  private Row readStep(String workflowId) throws SQLException {
    var sql =
        "SELECT output, child_workflow_id FROM \"%s\".operation_outputs".formatted(schema)
            + " WHERE workflow_uuid = ? AND function_id = 0";
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "step row not found for " + workflowId);
        return new Row(rs.getString("output"), rs.getString("child_workflow_id"));
      }
    }
  }

  private boolean stepRecorded(String workflowId) throws SQLException {
    var sql = "SELECT 1 FROM \"%s\".operation_outputs WHERE workflow_uuid = ?".formatted(schema);
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        return rs.next();
      }
    }
  }

  @Test
  public void recordingTheSameResultAgainSucceeds() throws Exception {
    var workflowId = "record-step-same-%d".formatted(System.nanoTime());
    var owner = createWorkflow(workflowId);

    StepsDAO.recordStepResult(ctx, output(workflowId, "a"), 100L, 200L, owner);
    assertDoesNotThrow(
        () -> StepsDAO.recordStepResult(ctx, output(workflowId, "a"), 100L, 200L, owner),
        "a retry whose first attempt committed must find its own row");

    assertEquals("a", readStep(workflowId).output());
  }

  @Test
  public void aDifferentResultFromTheOwnerIsNondeterminismAndKeepsTheFirst() throws Exception {
    var workflowId = "record-step-different-%d".formatted(System.nanoTime());
    var owner = createWorkflow(workflowId);

    StepsDAO.recordStepResult(ctx, output(workflowId, "first"), 100L, 200L, owner);
    var e =
        assertThrows(
            DBOSStepNondeterminismException.class,
            () -> StepsDAO.recordStepResult(ctx, output(workflowId, "second"), 150L, 250L, owner));
    assertEquals(workflowId, e.workflowId());
    assertEquals(0, e.stepId());

    assertEquals("first", readStep(workflowId).output(), "the first checkpoint must stand");
  }

  @Test
  public void theSameResultAtAnotherTimeIsNondeterminism() throws Exception {
    var workflowId = "record-step-other-time-%d".formatted(System.nanoTime());
    var owner = createWorkflow(workflowId);

    StepsDAO.recordStepResult(ctx, output(workflowId, "a"), 100L, 200L, owner);
    assertThrows(
        DBOSStepNondeterminismException.class,
        () -> StepsDAO.recordStepResult(ctx, output(workflowId, "a"), 150L, 250L, owner),
        "recording the step a second time is not a retry of the first");
  }

  @Test
  public void recordingTheSameChildAgainSucceeds() throws Exception {
    var workflowId = "record-child-same-%d".formatted(System.nanoTime());
    var owner = createWorkflow(workflowId);

    StepsDAO.recordStepResult(ctx, child(workflowId, "child-a"), 100L, 200L, owner);
    assertDoesNotThrow(
        () -> StepsDAO.recordStepResult(ctx, child(workflowId, "child-a"), 100L, 200L, owner));

    assertEquals("child-a", readStep(workflowId).childWorkflowId());
  }

  @Test
  public void aDifferentChildAtTheSameStepIsNondeterminism() throws Exception {
    var workflowId = "record-child-different-%d".formatted(System.nanoTime());
    var owner = createWorkflow(workflowId);

    StepsDAO.recordStepResult(ctx, child(workflowId, "child-a"), 100L, 200L, owner);
    assertThrows(
        DBOSStepNondeterminismException.class,
        () -> StepsDAO.recordStepResult(ctx, child(workflowId, "child-b"), 100L, 200L, owner));

    assertEquals("child-a", readStep(workflowId).childWorkflowId());
  }

  @Test
  public void aStaleOwnerIsAConflictAndRecordsNothing() throws Exception {
    var workflowId = "record-step-stale-%d".formatted(System.nanoTime());
    var owner = createWorkflow(workflowId);
    DBTestAccess.takeOwnership(sysdb, workflowId);

    var e =
        assertThrows(
            DBOSWorkflowExecutionConflictException.class,
            () -> StepsDAO.recordStepResult(ctx, output(workflowId, "a"), 100L, 200L, owner));
    assertEquals(workflowId, e.workflowId());
    assertFalse(stepRecorded(workflowId), "a stale execution must record nothing");
  }

  @Test
  public void aReleasedWorkflowIsAConflict() throws Exception {
    var workflowId = "record-step-released-%d".formatted(System.nanoTime());
    var owner = createWorkflow(workflowId);
    sysdb.cancelWorkflows(List.of(workflowId), false, null);

    assertThrows(
        DBOSWorkflowExecutionConflictException.class,
        () -> StepsDAO.recordStepResult(ctx, output(workflowId, "a"), 100L, 200L, owner));
    assertFalse(stepRecorded(workflowId));
  }

  @Test
  public void aDeletedWorkflowIsAConflict() throws Exception {
    var workflowId = "record-step-deleted-%d".formatted(System.nanoTime());
    var owner = createWorkflow(workflowId);
    sysdb.deleteWorkflows(List.of(workflowId), false, null);

    assertThrows(
        DBOSWorkflowExecutionConflictException.class,
        () -> StepsDAO.recordStepResult(ctx, output(workflowId, "a"), 100L, 200L, owner));
  }

  // The owner check holds the workflow's row until the write it guards commits, so a hand-off
  // cannot land in between: a cancel issued meanwhile waits, then takes the workflow.
  @Test
  public void theOwnerCheckHoldsOffAHandOffUntilTheWriteCommits() throws Exception {
    var workflowId = "record-step-lock-%d".formatted(System.nanoTime());
    var owner = createWorkflow(workflowId);

    var checked = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var writer =
        CompletableFuture.runAsync(
            () -> {
              try (var conn = dataSource.getConnection()) {
                SqlTransaction.run(
                    conn,
                    c -> {
                      StepsDAO.recordStepResult(ctx, c, output(workflowId, "a"), 100L, 200L, owner);
                      checked.countDown();
                      try {
                        release.await();
                      } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                      }
                    });
              } catch (SQLException e) {
                throw new RuntimeException(e);
              }
            });
    assertTrue(checked.await(10, TimeUnit.SECONDS));

    var cancel =
        CompletableFuture.runAsync(() -> sysdb.cancelWorkflows(List.of(workflowId), false, null));
    assertThrows(
        TimeoutException.class,
        () -> cancel.get(1, TimeUnit.SECONDS),
        "the cancel must wait for the owner's write to commit");

    release.countDown();
    writer.get(10, TimeUnit.SECONDS);
    cancel.get(10, TimeUnit.SECONDS);

    assertEquals("a", readStep(workflowId).output(), "the owner's step committed first");
    assertEquals(null, WorkflowDAO.getWorkflowOwner(ctx, workflowId), "the cancel released it");
  }
}
