package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What an internal step records when its work fails, and that a caller who no longer owns its
 * workflow commits nothing, whether it calls from the workflow body or from inside a step.
 */
public class TransactionalStepTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @AutoClose DBOS dbos;

  private SystemDatabase sysdb;
  private BlockingServiceImpl impl;
  private BlockingService proxy;

  @BeforeEach
  void beforeEach() {
    dbos = new DBOS(pgContainer.dbosConfig());
    impl = new BlockingServiceImpl();
    proxy = dbos.registerProxy(BlockingService.class, impl);
    dbos.launch();
    sysdb = DBOSTestAccess.getSystemDatabase(dbos);
  }

  @AfterEach
  void afterEach() {
    impl.release.countDown();
  }

  private String startBlocked(String prefix) throws Exception {
    var id = "%s-%d".formatted(prefix, System.nanoTime());
    dbos.startWorkflow(proxy::blocked, new StartWorkflowOptions(id));
    assertTrue(impl.started.tryAcquire(10, TimeUnit.SECONDS));
    return id;
  }

  private String ownerOf(String workflowId) throws SQLException {
    var sql =
        "SELECT owner_xid FROM \"%s\".workflow_status WHERE workflow_uuid = ?"
            .formatted(sysdb.context().schema());
    try (var conn = sysdb.context().getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        return rs.getString(1);
      }
    }
  }

  private WorkflowState statusOf(String workflowId) {
    return dbos.getWorkflowStatus(workflowId).orElseThrow().status();
  }

  @Test
  public void aConflictThatOutlastsItsRetriesIsRecordedAndReplayed() throws Exception {
    var callerId = startBlocked("caller");
    var step = new StepCaller.FromWorkflow(callerId, 1000, ownerOf(callerId));
    var attempts = new AtomicInteger();

    assertThrows(
        RuntimeException.class,
        () ->
            sysdb.runTransactionalStep(
                step,
                "conflicted",
                conn -> {
                  attempts.incrementAndGet();
                  throw new SQLException("conflict", "40001");
                }));
    assertEquals(10, attempts.get(), "retried until the retries ran out");

    var recorded = dbos.listWorkflowSteps(callerId);
    assertEquals(1, recorded.size());
    assertNotNull(recorded.get(0).error(), "the failure is the step's answer");

    // A replay that would now succeed is refused the same way, so it takes the same branch.
    assertThrows(
        RuntimeException.class,
        () ->
            sysdb.runTransactionalStep(
                step,
                "conflicted",
                conn -> {
                  attempts.incrementAndGet();
                  return "succeeded";
                }));
    assertEquals(10, attempts.get(), "the replay did not run the work");
  }

  @Test
  public void aCallerThatLostItsWorkflowCommitsNothing() throws Exception {
    var callerId = startBlocked("caller");
    var targetId = startBlocked("target");
    var staleOwner = ownerOf(callerId);
    DBTestAccess.takeOwnership(sysdb, callerId);

    assertThrows(
        DBOSWorkflowExecutionConflictException.class,
        () ->
            sysdb.cancelWorkflows(
                List.of(targetId), false, new StepCaller.FromStep(callerId, staleOwner)));
    assertEquals(WorkflowState.PENDING, statusOf(targetId), "the step's cancel rolled back");

    assertThrows(
        DBOSWorkflowExecutionConflictException.class,
        () ->
            sysdb.cancelWorkflows(
                List.of(targetId), false, new StepCaller.FromWorkflow(callerId, 1000, staleOwner)));
    assertEquals(WorkflowState.PENDING, statusOf(targetId), "the body's cancel rolled back");
    assertTrue(dbos.listWorkflowSteps(callerId).isEmpty(), "nothing recorded");
  }
}

interface BlockingService {
  String blocked() throws InterruptedException;
}

class BlockingServiceImpl implements BlockingService {
  final Semaphore started = new Semaphore(0);
  final CountDownLatch release = new CountDownLatch(1);

  @Override
  @Workflow
  public String blocked() throws InterruptedException {
    started.release();
    release.await();
    return "released";
  }
}
