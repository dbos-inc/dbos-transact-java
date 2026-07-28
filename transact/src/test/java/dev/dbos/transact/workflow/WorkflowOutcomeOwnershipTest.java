package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.exceptions.DBOSMaxRecoveryAttemptsExceededException;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A run may record its outcome only while its workflow_status row is still PENDING: that row is
 * what says "this run is what the workflow is doing". Every other status means the run lost
 * ownership (a concurrent resume re-enqueued it, a recovery raced it, it was cancelled or
 * dead-lettered) and the recorded outcome, not the one the run computed, is the workflow's outcome.
 */
public class WorkflowOutcomeOwnershipTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;

  private OutcomeOwnershipService proxy;
  private OutcomeOwnershipServiceImpl impl;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig().withAppVersion("v1.0.0");
    dbos = new DBOS(dbosConfig);
    dataSource = pgContainer.dataSource();

    impl = new OutcomeOwnershipServiceImpl();
    proxy = dbos.registerProxy(OutcomeOwnershipService.class, impl);

    dbos.launch();
  }

  // Starts a run and returns once it is blocked inside the workflow function, with its row
  // PENDING.
  private WorkflowHandle<String, ?> startBlockedRun(String workflowId) throws InterruptedException {
    impl.startedLatches.put(workflowId, new CountDownLatch(1));
    impl.releaseLatches.put(workflowId, new CountDownLatch(1));
    var handle =
        dbos.startWorkflow(() -> proxy.blockedWorkflow(), new StartWorkflowOptions(workflowId));
    impl.startedLatches.get(workflowId).await();
    return handle;
  }

  private void releaseRun(String workflowId) {
    impl.releaseLatches.get(workflowId).countDown();
  }

  // Takes the row away from the blocked run, standing in for the concurrent
  // resume/recovery/cancel that would do it in production.
  private void rewriteRow(String workflowId, WorkflowState status, String output, String error)
      throws SQLException {
    var sql =
        "UPDATE dbos.workflow_status SET status = ?, output = ?, error = ?, updated_at = ?"
            + " WHERE workflow_uuid = ?";
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, status.name());
      stmt.setString(2, output);
      stmt.setString(3, error);
      stmt.setLong(4, Instant.now().toEpochMilli());
      stmt.setString(5, workflowId);
      assertEquals(1, stmt.executeUpdate());
    }
  }

  private void setRecoveryAttempts(String workflowId, int attempts) throws SQLException {
    var sql = "UPDATE dbos.workflow_status SET recovery_attempts = ? WHERE workflow_uuid = ?";
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setInt(1, attempts);
      stmt.setString(2, workflowId);
      assertEquals(1, stmt.executeUpdate());
    }
  }

  private void deleteRow(String workflowId) throws SQLException {
    var sql = "DELETE FROM dbos.workflow_status WHERE workflow_uuid = ?";
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      assertEquals(1, stmt.executeUpdate());
    }
  }

  private record Row(String status, String output) {}

  private Row readRow(String workflowId) throws SQLException {
    var sql = "SELECT status, output FROM dbos.workflow_status WHERE workflow_uuid = ?";
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        assertTrue(rs.next(), "workflow row not found: " + workflowId);
        return new Row(rs.getString("status"), rs.getString("output"));
      }
    }
  }

  // Mirrors the default workflow serializer, so rewritten outputs/errors deserialize the same
  // way a recorded outcome would.
  private static String serializeValue(Object value) {
    return SerializationUtil.serializeValue(value, null, null).serializedValue();
  }

  private static String serializeError(Throwable error) {
    return SerializationUtil.serializeError(error, null, null).serializedValue();
  }

  @Test
  public void recordedSuccessSupersedesTheRunResult() throws Exception {
    var workflowId = "outcome-ownership-success-%d".formatted(System.currentTimeMillis());
    var handle = startBlockedRun(workflowId);
    var recorded = serializeValue("recorded-elsewhere");
    rewriteRow(workflowId, WorkflowState.SUCCESS, recorded, null);
    releaseRun(workflowId);

    assertEquals(
        "recorded-elsewhere",
        handle.getResult(),
        "the run must report the recorded output, not its own");

    var row = readRow(workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), row.status());
    assertEquals(recorded, row.output(), "the recorded output must not be overwritten");
  }

  @Test
  public void recordedErrorSupersedesTheRunResult() throws Exception {
    var workflowId = "outcome-ownership-error-%d".formatted(System.currentTimeMillis());
    var handle = startBlockedRun(workflowId);
    rewriteRow(
        workflowId,
        WorkflowState.ERROR,
        null,
        serializeError(new IllegalStateException("recorded failure")));
    releaseRun(workflowId);

    var e =
        assertThrows(
            IllegalStateException.class, handle::getResult, "the recorded error must be adopted");
    assertEquals("recorded failure", e.getMessage());
    assertEquals(WorkflowState.ERROR.name(), readRow(workflowId).status());
  }

  @Test
  public void nonTerminalRowParksTheRunUntilAnOutcomeIsRecorded() throws Exception {
    var workflowId = "outcome-ownership-parked-%d".formatted(System.currentTimeMillis());
    var handle = startBlockedRun(workflowId);
    // ENQUEUED with no queue name: nothing dequeues it, so the run stays parked until this test
    // records the outcome itself.
    rewriteRow(workflowId, WorkflowState.ENQUEUED, null, null);
    releaseRun(workflowId);

    var done =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return handle.getResult();
              } catch (Exception e) {
                throw new CompletionException(e);
              }
            });

    assertThrows(
        TimeoutException.class,
        () -> done.get(3, TimeUnit.SECONDS),
        "the run must wait for the owning execution");

    rewriteRow(workflowId, WorkflowState.SUCCESS, serializeValue("recorded-by-owner"), null);

    assertEquals(
        "recorded-by-owner",
        done.get(30, TimeUnit.SECONDS),
        "the parked run must adopt the recorded outcome");
  }

  @Test
  public void deadLetteredRowFailsTheRun() throws Exception {
    var workflowId = "outcome-ownership-dlq-%d".formatted(System.currentTimeMillis());
    var handle = startBlockedRun(workflowId);
    rewriteRow(workflowId, WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED, null, null);
    // A workflow is dead-lettered by the attempt that pushes recovery_attempts past
    // maxRetries+1, so a dead-lettered row carries maxRetries+2 attempts.
    final int maxRetries = 3;
    setRecoveryAttempts(workflowId, maxRetries + 2);
    releaseRun(workflowId);

    var e =
        assertThrows(
            DBOSMaxRecoveryAttemptsExceededException.class,
            handle::getResult,
            "a dead-lettered workflow must not report a completion");
    assertEquals(maxRetries, e.maxRetries(), "the error must report the exhausted retry budget");

    var row = readRow(workflowId);
    assertEquals(WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED.name(), row.status());
    assertNull(row.output(), "the refused outcome must not record an output");
  }

  @Test
  public void deletedRowFailsTheRunWithNonExistentWorkflow() throws Exception {
    var workflowId = "outcome-ownership-deleted-%d".formatted(System.currentTimeMillis());
    var handle = startBlockedRun(workflowId);
    deleteRow(workflowId);
    releaseRun(workflowId);

    assertThrows(
        DBOSNonExistentWorkflowException.class,
        handle::getResult,
        "a run whose row vanished must not report a completion");
  }
}

interface OutcomeOwnershipService {
  String blockedWorkflow() throws InterruptedException;
}

class OutcomeOwnershipServiceImpl implements OutcomeOwnershipService {

  // Per-workflow latches, keyed by workflow ID: each run blocks until the test has rewritten its
  // row, then returns a result the test can tell apart from anything recorded out-of-band.
  final ConcurrentHashMap<String, CountDownLatch> startedLatches = new ConcurrentHashMap<>();
  final ConcurrentHashMap<String, CountDownLatch> releaseLatches = new ConcurrentHashMap<>();

  @Override
  @Workflow
  public String blockedWorkflow() throws InterruptedException {
    var wfId = DBOSContextHolder.get().getWorkflowId();
    startedLatches.get(wfId).countDown();
    releaseLatches.get(wfId).await();
    return "own-result";
  }
}
