package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.dao.StepsDAO;
import dev.dbos.transact.database.dao.WorkflowDAO;
import dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.WorkflowStatusInternalBuilder;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.SQLException;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A step checkpoint is written once per (workflow, step). A second write of the same row is the
 * same call retried after its acknowledgement was lost, and succeeds; a different row means another
 * execution of the workflow got there first, and is a conflict.
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

  private void createWorkflow(String workflowId) {
    sysdb.initWorkflowStatus(
        new WorkflowStatusInternalBuilder()
            .workflowId(workflowId)
            .workflowName("TestWorkflow")
            .appVersion("1.0.0")
            .priority(0)
            .build(),
        null);
  }

  private static StepResult output(String workflowId, String value) {
    return new StepResult(workflowId, 0, "step", value, null, null, null);
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

  @Test
  public void recordingTheSameResultAgainIsNotAConflict() throws Exception {
    var workflowId = "record-step-same-%d".formatted(System.nanoTime());
    createWorkflow(workflowId);

    StepsDAO.recordStepResult(ctx, output(workflowId, "a"), 100L, 200L);
    assertDoesNotThrow(
        () -> StepsDAO.recordStepResult(ctx, output(workflowId, "a"), 100L, 200L),
        "a retry whose first attempt committed must find its own row");

    assertEquals("a", readStep(workflowId).output());
  }

  @Test
  public void aDifferentResultIsAConflictAndKeepsTheFirst() throws Exception {
    var workflowId = "record-step-different-%d".formatted(System.nanoTime());
    createWorkflow(workflowId);

    StepsDAO.recordStepResult(ctx, output(workflowId, "first"), 100L, 200L);
    assertThrows(
        DBOSWorkflowExecutionConflictException.class,
        () -> StepsDAO.recordStepResult(ctx, output(workflowId, "second"), 150L, 250L));

    assertEquals("first", readStep(workflowId).output(), "the first checkpoint must stand");
  }

  @Test
  public void theSameResultAtAnotherTimeIsAConflict() throws Exception {
    var workflowId = "record-step-other-time-%d".formatted(System.nanoTime());
    createWorkflow(workflowId);

    StepsDAO.recordStepResult(ctx, output(workflowId, "a"), 100L, 200L);
    assertThrows(
        DBOSWorkflowExecutionConflictException.class,
        () -> StepsDAO.recordStepResult(ctx, output(workflowId, "a"), 150L, 250L),
        "another execution computing the same output is still another execution");
  }

  @Test
  public void recordingTheSameChildAgainIsNotAConflict() throws Exception {
    var workflowId = "record-child-same-%d".formatted(System.nanoTime());
    createWorkflow(workflowId);

    WorkflowDAO.recordChildWorkflow(ctx, workflowId, "child-a", 0, "ChildWorkflow", 100L);
    assertDoesNotThrow(
        () ->
            WorkflowDAO.recordChildWorkflow(ctx, workflowId, "child-a", 0, "ChildWorkflow", 200L));

    assertEquals("child-a", readStep(workflowId).childWorkflowId());
  }

  @Test
  public void aDifferentChildAtTheSameStepIsAConflict() throws Exception {
    var workflowId = "record-child-different-%d".formatted(System.nanoTime());
    createWorkflow(workflowId);

    WorkflowDAO.recordChildWorkflow(ctx, workflowId, "child-a", 0, "ChildWorkflow", 100L);
    assertThrows(
        DBOSWorkflowExecutionConflictException.class,
        () ->
            WorkflowDAO.recordChildWorkflow(ctx, workflowId, "child-b", 0, "ChildWorkflow", 100L));

    assertEquals("child-a", readStep(workflowId).childWorkflowId());
  }
}
