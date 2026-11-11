package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.dao.WorkflowDAO;
import dev.dbos.transact.exceptions.DBOSWorkflowIdInUseException;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.WorkflowStatusInternalBuilder;
import dev.dbos.transact.workflow.WorkflowIdReusePolicy;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

/**
 * Who owns a workflow's execution after the insert that creates its row: a direct start owns it
 * with its creator token, a queued workflow is owned by nobody until a claim, and a start that
 * finds a row already there owns nothing and changes nothing on it.
 */
public class WorkflowInsertOwnershipTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose SystemDatabase sysdb;
  @AutoClose HikariDataSource dataSource;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    MigrationManager.runMigrations(dbosConfig);
    sysdb = SystemDatabase.create(dbosConfig, null, dbosConfig.appName());
    dataSource = pgContainer.dataSource();
  }

  private WorkflowInitResult insert(
      WorkflowStatusInternal status,
      String creatorXid,
      @Nullable WorkflowIdReusePolicy workflowIdReusePolicy)
      throws SQLException {
    return WorkflowDAO.initWorkflowStatus(
        sysdb.context(), status, null, null, creatorXid, workflowIdReusePolicy, null);
  }

  @Test
  public void aDirectStartOwnsItsWorkflowWithItsCreatorToken() throws Exception {
    var workflowId = "insert-direct-%d".formatted(System.nanoTime());
    var token = UUID.randomUUID().toString();

    var result = insert(WorkflowStatusInternalBuilder.create(workflowId).build(), token, null);

    assertTrue(result.shouldExecuteOnThisExecutor());
    assertEquals(token, result.ownerXid());
    assertEquals(token, WorkflowDAO.getWorkflowOwner(sysdb.context(), workflowId));
  }

  @Test
  public void anEnqueuedWorkflowIsOwnedByNobody() throws Exception {
    var workflowId = "insert-enqueued-%d".formatted(System.nanoTime());
    var status = WorkflowStatusInternalBuilder.create(workflowId).queueName("q").build();

    var result = insert(status, UUID.randomUUID().toString(), null);

    assertEquals(WorkflowState.ENQUEUED, result.status());
    assertNull(result.ownerXid());
    assertNull(WorkflowDAO.getWorkflowOwner(sysdb.context(), workflowId));
  }

  @Test
  public void aSecondStartOwnsNothingAndChangesNothing() throws Exception {
    var workflowId = "insert-second-%d".formatted(System.nanoTime());
    var token = UUID.randomUUID().toString();
    insert(
        WorkflowStatusInternalBuilder.create(workflowId).executorId("exec-a").build(), token, null);

    var second =
        insert(
            WorkflowStatusInternalBuilder.create(workflowId).executorId("exec-b").build(),
            UUID.randomUUID().toString(),
            null);

    assertFalse(second.shouldExecuteOnThisExecutor());
    assertNull(second.ownerXid());
    assertEquals(token, WorkflowDAO.getWorkflowOwner(sysdb.context(), workflowId));
    assertEquals("exec-a", DBUtils.getWorkflowRow(dataSource, workflowId).executorId());
  }

  // A start whose commit landed but whose acknowledgement was lost retries with the same token and
  // finds its own row. If the row has moved on in between -- cancelled by its deadline -- the start
  // still created it, but there is nothing left for it to own.
  @Test
  public void aRetriedStartFindingItsOwnRowCancelledOwnsNothing() throws Exception {
    var workflowId = "insert-retried-%d".formatted(System.nanoTime());
    var token = UUID.randomUUID().toString();
    var status = WorkflowStatusInternalBuilder.create(workflowId).build();
    insert(status, token, null);
    sysdb.cancelWorkflows(List.of(workflowId), false, null);

    var retried = insert(status, token, null);

    assertTrue(retried.shouldExecuteOnThisExecutor(), "the retry recognizes its own row");
    assertEquals(WorkflowState.CANCELLED, retried.status());
    assertNull(retried.ownerXid(), "a row no longer PENDING is owned by nobody");
  }

  // WorkflowIdReusePolicy.REJECT refuses an ID another caller's insert created, whatever its
  // status, and the refused insert writes nothing: no input, no updated_at.
  @Test
  public void aRejectingInsertFindingAnotherCallersRowIsRefusedAndWritesNothing() throws Exception {
    var workflowId = "insert-reject-%d".formatted(System.nanoTime());
    insert(
        WorkflowStatusInternalBuilder.create(workflowId).workflowName("wf").inputs("first").build(),
        UUID.randomUUID().toString(),
        null);
    var before = DBUtils.getWorkflowRow(dataSource, workflowId);

    var rejecting =
        WorkflowStatusInternalBuilder.create(workflowId)
            .workflowName("wf")
            .inputs("second")
            .build();
    var refused =
        assertThrows(
            DBOSWorkflowIdInUseException.class,
            () -> insert(rejecting, UUID.randomUUID().toString(), WorkflowIdReusePolicy.REJECT));

    assertEquals(workflowId, refused.workflowId());
    assertEquals(WorkflowState.PENDING, refused.workflowStatus());
    assertEquals("wf", refused.workflowName());
    var after = DBUtils.getWorkflowRow(dataSource, workflowId);
    assertEquals("first", after.inputs());
    assertEquals(before.updatedAt(), after.updatedAt());
  }

  // A retry whose first attempt committed but lost its acknowledgement finds its own token, so
  // REJECT does not refuse the row that attempt created.
  @Test
  public void aRetriedRejectingInsertFindingItsOwnRowIsNotRefused() throws Exception {
    var workflowId = "insert-reject-retry-%d".formatted(System.nanoTime());
    var token = UUID.randomUUID().toString();
    var status = WorkflowStatusInternalBuilder.create(workflowId).build();
    insert(status, token, WorkflowIdReusePolicy.REJECT);

    var retried = insert(status, token, WorkflowIdReusePolicy.REJECT);

    assertTrue(retried.shouldExecuteOnThisExecutor(), "the retry recognizes its own row");
    assertEquals(token, retried.ownerXid());
  }

  // The owner check locks the workflow's row, so a step record can deadlock with a write that locks
  // several rows. A deadlock rolls the record back, and it is replayed rather than failing the
  // workflow.
  @Test
  @ResourceLock(DebugTriggers.DEBUG_TRIGGER_STEP_COMMIT)
  public void aStepRecordIsReplayedAfterADeadlock() throws Exception {
    var workflowId = "insert-deadlock-%d".formatted(System.nanoTime());
    var token =
        insert(WorkflowStatusInternalBuilder.create(workflowId).build(), "t", null).ownerXid();
    assertNotNull(token);

    DebugTriggers.setDebugTrigger(
        DebugTriggers.DEBUG_TRIGGER_STEP_COMMIT,
        new DebugTriggers.DebugAction()
            .setSqlExceptionToThrow(new SQLException("deadlock detected", "40P01")));
    try {
      var step = new StepResult(workflowId, 0, "step", "out", null, null, null);
      assertDoesNotThrow(() -> sysdb.recordStepResult(step, 100L, token));
    } finally {
      DebugTriggers.clearDebugTrigger(DebugTriggers.DEBUG_TRIGGER_STEP_COMMIT);
    }

    assertEquals(1, DBUtils.getStepRows(dataSource, workflowId).size());
  }
}
