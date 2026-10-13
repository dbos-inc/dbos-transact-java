package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.database.DBTestAccess;
import dev.dbos.transact.exceptions.DBOSStepNondeterminismException;
import dev.dbos.transact.execution.ThrowingSupplier;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Set;
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
 * Two executions of one workflow race on a step: the workflow changes hands while one is inside the
 * step, and the new owner checkpoints it first. The old execution must not carry on with its own
 * step result. It stops, runs no later step, records nothing as the workflow's outcome, and reports
 * the outcome the new owner records.
 */
public class StepCheckpointConflictTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;

  private StepConflictService proxy;
  private StepConflictServiceImpl impl;

  @BeforeEach
  void beforeEach() {
    dbos = new DBOS(pgContainer.dbosConfig());
    dataSource = pgContainer.dataSource();

    impl = new StepConflictServiceImpl(dbos);
    proxy = dbos.registerProxy(StepConflictService.class, impl);

    dbos.launch();
  }

  // Takes the workflow over, as a resume or recovery and the claim after it would, and writes step
  // 0's checkpoint as the new owner.
  private void recordStepElsewhere(String workflowId, String output) throws SQLException {
    DBTestAccess.takeOwnership(DBOSTestAccess.getSystemDatabase(dbos), workflowId);
    insertStep(workflowId, output);
  }

  // Writes step 0's checkpoint without taking the workflow over, as only an executor that does not
  // fence its writes could.
  private void insertStep(String workflowId, String output) throws SQLException {
    var serialized = SerializationUtil.serializeValue(output, null, null);
    var sql =
        "INSERT INTO dbos.operation_outputs (workflow_uuid, function_id, function_name, output,"
            + " started_at_epoch_ms, completed_at_epoch_ms, serialization)"
            + " VALUES (?, 0, 'raceStep', ?, ?, ?, ?)";
    var now = Instant.now().toEpochMilli();
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setString(2, serialized.serializedValue());
      stmt.setLong(3, now);
      stmt.setLong(4, now);
      stmt.setString(5, serialized.serialization());
      assertEquals(1, stmt.executeUpdate());
    }
  }

  // Records the workflow's outcome as the new owner would, releasing it.
  private void recordSuccessElsewhere(String workflowId, String output) throws SQLException {
    var sql =
        "UPDATE dbos.workflow_status SET status = 'SUCCESS', output = ?, updated_at = ?,"
            + " owner_xid = NULL WHERE workflow_uuid = ?";
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, SerializationUtil.serializeValue(output, null, null).serializedValue());
      stmt.setLong(2, Instant.now().toEpochMilli());
      stmt.setString(3, workflowId);
      assertEquals(1, stmt.executeUpdate());
    }
  }

  private String readStatus(String workflowId) throws SQLException {
    var sql = "SELECT status FROM dbos.workflow_status WHERE workflow_uuid = ?";
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        return rs.getString("status");
      }
    }
  }

  private int countSteps(String workflowId) throws SQLException {
    var sql = "SELECT COUNT(*) FROM dbos.operation_outputs WHERE workflow_uuid = ?";
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        return rs.getInt(1);
      }
    }
  }

  private void assertLoserStopsAndAdopts(
      String workflowId, WorkflowHandle<String, ? extends Exception> handle) throws Exception {
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
        () -> done.get(2, TimeUnit.SECONDS),
        "the losing run must stop and wait, not finish with its own step result");

    assertEquals("PENDING", readStatus(workflowId), "the conflict must not become the outcome");
    assertFalse(impl.ranAfterStep.contains(workflowId), "the loser must not run later steps");
    assertEquals(1, countSteps(workflowId), "the loser must not checkpoint later steps");

    recordSuccessElsewhere(workflowId, "winner-result");
    assertEquals(
        "winner-result",
        done.get(30, TimeUnit.SECONDS),
        "the loser must report the outcome the winner recorded");
  }

  private <E extends Exception> WorkflowHandle<String, E> startBlockedInStep(
      String workflowId, ThrowingSupplier<String, E> workflow) throws InterruptedException {
    impl.startedLatches.put(workflowId, new CountDownLatch(1));
    impl.releaseLatches.put(workflowId, new CountDownLatch(1));
    var handle = dbos.startWorkflow(workflow, new StartWorkflowOptions(workflowId));
    impl.startedLatches.get(workflowId).await();
    return handle;
  }

  @Test
  public void aStepCheckpointedByANewOwnerStopsTheOldExecution() throws Exception {
    var workflowId = "step-conflict-%d".formatted(System.nanoTime());
    var handle = startBlockedInStep(workflowId, proxy::raceWorkflow);

    recordStepElsewhere(workflowId, "winner-step");
    impl.releaseLatches.get(workflowId).countDown();

    assertLoserStopsAndAdopts(workflowId, handle);
  }

  @Test
  public void aConflictTheWorkflowRethrowsWrappedStillStopsTheLoser() throws Exception {
    var workflowId = "step-conflict-wrapped-%d".formatted(System.nanoTime());
    var handle = startBlockedInStep(workflowId, proxy::wrappingRaceWorkflow);

    recordStepElsewhere(workflowId, "winner-step");
    impl.releaseLatches.get(workflowId).countDown();

    assertLoserStopsAndAdopts(workflowId, handle);
  }

  @Test
  public void aConflictTheWorkflowRethrowsInACheckedExceptionStillStopsTheLoser() throws Exception {
    var workflowId = "step-conflict-checked-%d".formatted(System.nanoTime());
    var handle = startBlockedInStep(workflowId, proxy::checkedWrappingRaceWorkflow);

    recordStepElsewhere(workflowId, "winner-step");
    impl.releaseLatches.get(workflowId).countDown();

    assertLoserStopsAndAdopts(workflowId, handle);
  }

  // A different checkpoint at the step, written while this execution still owns the workflow, can
  // only be its own doing: nondeterminism, recorded as the workflow's error rather than waited out.
  @Test
  public void aDifferentCheckpointUnderTheSameOwnerIsNondeterminism() throws Exception {
    var workflowId = "step-conflict-nondeterminism-%d".formatted(System.nanoTime());
    var handle = startBlockedInStep(workflowId, proxy::raceWorkflow);

    insertStep(workflowId, "other-step");
    impl.releaseLatches.get(workflowId).countDown();

    var e = assertThrows(DBOSStepNondeterminismException.class, handle::getResult);
    assertEquals(workflowId, e.workflowId());
    assertEquals("ERROR", readStatus(workflowId));
    assertFalse(impl.ranAfterStep.contains(workflowId), "no later step may run");
  }

  // A recv that claimed no message records a NULL output and returns null. Its replay must
  // return null too, not receive again and try to record a second, different result.
  @Test
  public void aRecvRecordedWithNoMessageReplaysAsNull() throws Exception {
    var workflowId = "step-conflict-recv-%d".formatted(System.nanoTime());
    var sql =
        "INSERT INTO dbos.operation_outputs (workflow_uuid, function_id, function_name,"
            + " started_at_epoch_ms, completed_at_epoch_ms) VALUES (?, 0, 'DBOS.recv', ?, ?)";
    var now = Instant.now().toEpochMilli();
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setLong(2, now);
      stmt.setLong(3, now);
      assertEquals(1, stmt.executeUpdate());
    }

    var handle = dbos.startWorkflow(proxy::recvWorkflow, new StartWorkflowOptions(workflowId));

    assertEquals("no-message", handle.getResult());
    assertEquals("SUCCESS", readStatus(workflowId));
    assertEquals(1, countSteps(workflowId), "the replay must not run recv's timeout again");
  }
}

interface StepConflictService {
  String raceWorkflow() throws InterruptedException;

  String wrappingRaceWorkflow() throws InterruptedException;

  String checkedWrappingRaceWorkflow() throws Exception;

  String recvWorkflow();
}

class StepConflictServiceImpl implements StepConflictService {

  private final DBOS dbos;

  // Per-workflow latches, keyed by workflow ID: the race step blocks until the test has written
  // its checkpoint as another execution.
  final ConcurrentHashMap<String, CountDownLatch> startedLatches = new ConcurrentHashMap<>();
  final ConcurrentHashMap<String, CountDownLatch> releaseLatches = new ConcurrentHashMap<>();
  final Set<String> ranAfterStep = ConcurrentHashMap.newKeySet();

  StepConflictServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  private String raceStep(String workflowId) throws InterruptedException {
    return dbos.runStep(
        () -> {
          startedLatches.get(workflowId).countDown();
          releaseLatches.get(workflowId).await();
          return "own-step";
        },
        "raceStep");
  }

  private void afterStep(String workflowId) {
    dbos.runStep(() -> ranAfterStep.add(workflowId), "afterStep");
  }

  @Override
  @Workflow
  public String raceWorkflow() throws InterruptedException {
    var workflowId = DBOSContextHolder.get().getWorkflowId();
    var result = raceStep(workflowId);
    afterStep(workflowId);
    return result;
  }

  // Catches everything the step throws and rethrows it wrapped, as application code may.
  @Override
  @Workflow
  public String wrappingRaceWorkflow() throws InterruptedException {
    var workflowId = DBOSContextHolder.get().getWorkflowId();
    String result;
    try {
      result = raceStep(workflowId);
    } catch (RuntimeException e) {
      throw new IllegalStateException("step failed", e);
    }
    afterStep(workflowId);
    return result;
  }

  // As wrappingRaceWorkflow, but the wrapper is a checked exception.
  @Override
  @Workflow
  public String checkedWrappingRaceWorkflow() throws Exception {
    var workflowId = DBOSContextHolder.get().getWorkflowId();
    String result;
    try {
      result = raceStep(workflowId);
    } catch (RuntimeException e) {
      throw new Exception("step failed", e);
    }
    afterStep(workflowId);
    return result;
  }

  @Override
  @Workflow
  public String recvWorkflow() {
    return dbos.<String>recv("topic", Duration.ofSeconds(1)).orElse("no-message");
  }
}
