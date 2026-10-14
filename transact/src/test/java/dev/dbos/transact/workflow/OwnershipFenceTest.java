package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.database.DBTestAccess;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.txstep.JdbcStepFactory;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Only the execution that currently owns a workflow writes its checkpoints. One that has lost the
 * workflow -- to a cancel, resume, recovery or another claim -- is refused at its next write, lands
 * nothing, and waits for the outcome the owner records.
 */
public class OwnershipFenceTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;

  private FenceService proxy;
  private FenceServiceImpl impl;

  @BeforeEach
  void beforeEach() {
    dbos = new DBOS(pgContainer.dbosConfig().withEnablePatching());
    dataSource = pgContainer.dataSource();

    impl = new FenceServiceImpl(dbos, new JdbcStepFactory(dbos, dataSource));
    proxy = dbos.registerProxy(FenceService.class, impl);
    impl.self = proxy;

    dbos.launch();
  }

  private String takeOwnership(String workflowId) {
    return DBTestAccess.takeOwnership(DBOSTestAccess.getSystemDatabase(dbos), workflowId);
  }

  // Records the workflow's outcome as its current owner would, releasing it.
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
    return DBUtils.getWorkflowRow(dataSource, workflowId).status();
  }

  private int count(String sql, String workflowId) throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        return rs.getInt(1);
      }
    }
  }

  // Everything any of the writes below could leave behind.
  private void assertNothingLanded(String workflowId) throws SQLException {
    assertEquals(
        0,
        count("SELECT COUNT(*) FROM dbos.operation_outputs WHERE workflow_uuid = ?", workflowId),
        "no step");
    assertEquals(
        0,
        count("SELECT COUNT(*) FROM dbos.workflow_events WHERE workflow_uuid = ?", workflowId),
        "no event");
    assertEquals(
        0,
        count("SELECT COUNT(*) FROM dbos.streams WHERE workflow_uuid = ?", workflowId),
        "no stream entry");
    assertEquals(
        0,
        count("SELECT COUNT(*) FROM dbos.notifications WHERE destination_uuid = ?", workflowId),
        "no message");
    assertEquals(
        0,
        count("SELECT COUNT(*) FROM dbos.workflow_status WHERE parent_workflow_id = ?", workflowId),
        "no child");
    assertEquals(
        0,
        count("SELECT COUNT(*) FROM dbos.tx_step_outputs WHERE workflow_id = ?", workflowId),
        "no transactional step");
    assertEquals("PENDING", readStatus(workflowId), "no outcome");
  }

  private static <T> CompletableFuture<T> resultOf(WorkflowHandle<T, ?> handle) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            return handle.getResult();
          } catch (Exception e) {
            throw new CompletionException(e);
          }
        });
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "step",
        "sleep",
        "setEvent",
        "writeStream",
        "closeStream",
        "send",
        "recv",
        "getEvent",
        "patch",
        "child",
        "setEventFromStep",
        "writeStreamFromStep",
        "sendFromStep",
        "txStep",
        "txStepFailure",
        "outcome"
      })
  public void aStaleExecutionsWriteIsRefusedAndItAdoptsTheOwnersOutcome(String write)
      throws Exception {
    var workflowId = "fence-%s-%d".formatted(write, System.nanoTime());
    impl.gates.put(workflowId, new CountDownLatch(1));
    impl.atGate.put(workflowId, new CountDownLatch(1));
    var handle =
        dbos.startWorkflow(() -> proxy.writeAfterGate(write), new StartWorkflowOptions(workflowId));
    assertTrue(impl.atGate.get(workflowId).await(10, TimeUnit.SECONDS));

    takeOwnership(workflowId);
    impl.gates.get(workflowId).countDown();

    var done = resultOf(handle);
    assertThrows(
        TimeoutException.class,
        () -> done.get(2, TimeUnit.SECONDS),
        "the stale execution must stop and wait, not finish on its own");
    assertNothingLanded(workflowId);

    recordSuccessElsewhere(workflowId, "owner-result");
    assertEquals("owner-result", done.get(30, TimeUnit.SECONDS));
  }

  // A resume or a recovery while the original execution still runs hands the workflow to a new
  // execution. Exactly one of them writes its checkpoints and outcome: the original is refused at
  // its next write and reports the new one's outcome.
  @ParameterizedTest
  @ValueSource(strings = {"resume", "recovery"})
  public void aHandOffWhileRunningLeavesOneExecutionWriting(String handOff) throws Exception {
    var workflowId = "fence-handoff-%s-%d".formatted(handOff, System.nanoTime());
    impl.gates.put(workflowId, new CountDownLatch(1));
    impl.atGate.put(workflowId, new CountDownLatch(1));
    var original = dbos.startWorkflow(proxy::handedOff, new StartWorkflowOptions(workflowId));
    assertTrue(impl.atGate.get(workflowId).await(10, TimeUnit.SECONDS));

    if (handOff.equals("resume")) {
      dbos.resumeWorkflow(workflowId);
    } else {
      var executor = DBOSTestAccess.getDbosExecutor(dbos);
      assertEquals(
          List.of(workflowId), executor.recoverPendingWorkflows(List.of(executor.executorId())));
    }

    // The new execution runs the whole workflow while the original is held in its step.
    var deadline = Instant.now().plusSeconds(30);
    while (!readStatus(workflowId).equals("SUCCESS") && Instant.now().isBefore(deadline)) {
      Thread.sleep(50);
    }
    assertEquals("SUCCESS", readStatus(workflowId));
    assertEquals(1, impl.afterStepRuns.get(), "only the new execution got past the step");

    impl.gates.get(workflowId).countDown();
    assertEquals("handed-off-2", original.getResult(), "the original reports the new outcome");
    assertEquals(1, impl.afterStepRuns.get(), "the original wrote nothing more");
    assertEquals(
        2,
        count("SELECT COUNT(*) FROM dbos.operation_outputs WHERE workflow_uuid = ?", workflowId),
        "one execution's steps");
  }

  // A dispatch whose claim was overtaken -- the workflow re-enqueued and claimed again -- writes
  // nothing, not even the failures it finds before running the workflow.
  @Test
  public void aStaleDispatchRecordsNothing() throws Exception {
    var workflowId = "fence-dispatch-%d".formatted(System.nanoTime());
    dbos.startWorkflow(proxy::quick, new StartWorkflowOptions(workflowId)).getResult();
    // Back to PENDING, without the outcome the first run recorded.
    DBUtils.setWorkflowState(dataSource, workflowId, "PENDING");
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement("DELETE FROM dbos.workflow_output WHERE workflow_uuid = ?")) {
      stmt.setString(1, workflowId);
      stmt.executeUpdate();
    }
    takeOwnership(workflowId);

    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    var done = resultOf(executor.<String, Exception>executeWorkflowById(workflowId, "stale-claim"));
    assertThrows(TimeoutException.class, () -> done.get(2, TimeUnit.SECONDS));
    assertEquals("PENDING", readStatus(workflowId), "the stale dispatch recorded no outcome");

    recordSuccessElsewhere(workflowId, "owner-result");
    assertEquals("owner-result", done.get(30, TimeUnit.SECONDS));

    // The failure path: a row it cannot read is not marked ERROR by a stale dispatch.
    DBUtils.setWorkflowState(dataSource, workflowId, "PENDING");
    takeOwnership(workflowId);
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "UPDATE dbos.workflow_status SET serialization = 'no-such-format'"
                    + " WHERE workflow_uuid = ?")) {
      stmt.setString(1, workflowId);
      assertEquals(1, stmt.executeUpdate());
    }
    assertThrows(
        IllegalStateException.class, () -> executor.executeWorkflowById(workflowId, "stale-claim"));
    assertEquals("PENDING", readStatus(workflowId));
  }

  // A run's own deadline cancels it only while it owns the workflow: once a new execution has
  // taken over, the old run's timer no longer reaches it.
  @Test
  public void aStaleExecutionsDeadlineDoesNotCancelTheNewOwner() throws Exception {
    var workflowId = "fence-timer-%d".formatted(System.nanoTime());
    impl.gates.put(workflowId, new CountDownLatch(1));
    impl.atGate.put(workflowId, new CountDownLatch(1));
    var options = new StartWorkflowOptions(workflowId).withTimeout(Duration.ofSeconds(2));
    dbos.startWorkflow(() -> proxy.writeAfterGate("outcome"), options);
    assertTrue(impl.atGate.get(workflowId).await(10, TimeUnit.SECONDS));

    // Taken over, deadline and all, as a resume clears it.
    takeOwnership(workflowId);
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "UPDATE dbos.workflow_status SET workflow_deadline_epoch_ms = NULL"
                    + " WHERE workflow_uuid = ?")) {
      stmt.setString(1, workflowId);
      assertEquals(1, stmt.executeUpdate());
    }

    Thread.sleep(3000);
    assertEquals("PENDING", readStatus(workflowId), "the stale timer must not cancel it");
    impl.gates.get(workflowId).countDown();
  }

  @Test
  public void aWorkflowCancellingItselfEndsCancelled() throws Exception {
    var workflowId = "fence-self-cancel-%d".formatted(System.nanoTime());
    var handle = dbos.startWorkflow(proxy::cancelSelf, new StartWorkflowOptions(workflowId));

    assertThrows(DBOSAwaitedWorkflowCancelledException.class, handle::getResult);
    assertEquals("CANCELLED", readStatus(workflowId));
    assertEquals(0, impl.afterStepRuns.get(), "nothing runs after the cancel");
  }

  @Test
  public void aWorkflowDeletingItselfEndsMissing() throws Exception {
    var workflowId = "fence-self-delete-%d".formatted(System.nanoTime());
    var handle = dbos.startWorkflow(proxy::deleteSelf, new StartWorkflowOptions(workflowId));

    assertThrows(DBOSNonExistentWorkflowException.class, handle::getResult);
    assertEquals(0, impl.afterStepRuns.get(), "nothing runs after the delete");
  }

  // Recorded as it behaves today: nothing refuses a workflow resuming itself. The resume hands the
  // workflow back to the queue before its own step is recorded, so the run is refused there and
  // waits, and the dispatch the resume caused replays to the same resume and does it again. Only
  // removing the workflow ends it.
  @Test
  public void aWorkflowResumingItselfIsDispatchedAgainAndAgain() throws Exception {
    var workflowId = "fence-self-resume-%d".formatted(System.nanoTime());
    var handle = dbos.startWorkflow(proxy::resumeSelf, new StartWorkflowOptions(workflowId));

    var deadline = Instant.now().plusSeconds(30);
    while (impl.resumeSelfRuns.get() < 3 && Instant.now().isBefore(deadline)) {
      Thread.sleep(50);
    }
    assertTrue(impl.resumeSelfRuns.get() >= 3, "each dispatch resumes the workflow again");

    dbos.deleteWorkflow(workflowId);
    assertThrows(DBOSNonExistentWorkflowException.class, handle::getResult);
  }
}

interface FenceService {
  String writeAfterGate(String write) throws Exception;

  String handedOff() throws InterruptedException;

  String child();

  String quick();

  String cancelSelf();

  String deleteSelf();

  String resumeSelf();
}

class FenceServiceImpl implements FenceService {

  private final DBOS dbos;
  private final JdbcStepFactory stepFactory;
  FenceService self;

  // Per-workflow latches, keyed by workflow ID: the workflow signals it reached the gate, then
  // waits there until the test has taken it over.
  final Map<String, CountDownLatch> atGate = new ConcurrentHashMap<>();
  final Map<String, CountDownLatch> gates = new ConcurrentHashMap<>();
  final AtomicInteger afterStepRuns = new AtomicInteger();
  final AtomicInteger handedOffRuns = new AtomicInteger();
  final AtomicInteger resumeSelfRuns = new AtomicInteger();

  FenceServiceImpl(DBOS dbos, JdbcStepFactory stepFactory) {
    this.dbos = dbos;
    this.stepFactory = stepFactory;
  }

  private static String workflowId() {
    return DBOSContextHolder.get().getWorkflowId();
  }

  private void waitAtGate(String workflowId) throws InterruptedException {
    atGate.get(workflowId).countDown();
    gates.get(workflowId).await();
  }

  @Override
  @Workflow
  public String writeAfterGate(String write) throws Exception {
    var workflowId = workflowId();
    waitAtGate(workflowId);
    switch (write) {
      case "step" -> dbos.runStep(() -> "step-result", "step");
      case "sleep" -> dbos.sleep(Duration.ofMillis(10));
      case "setEvent" -> dbos.setEvent("key", "value");
      case "writeStream" -> dbos.writeStream("key", "value");
      case "closeStream" -> dbos.closeStream("key");
      case "send" -> dbos.send(workflowId, "message", "topic");
      case "recv" -> dbos.recv("topic", Duration.ofMillis(100));
      case "getEvent" -> dbos.getEvent(workflowId, "key", Duration.ofMillis(100));
      case "patch" -> dbos.patch("fence");
      case "child" ->
          dbos.startWorkflow(self::child, new StartWorkflowOptions(workflowId + "-child"));
      case "setEventFromStep" -> dbos.runStep(() -> dbos.setEvent("key", "value"), "inStep");
      case "writeStreamFromStep" -> dbos.runStep(() -> dbos.writeStream("key", "value"), "inStep");
      case "sendFromStep" ->
          dbos.runStep(() -> dbos.send(workflowId, "message", "topic"), "inStep");
      case "txStep" ->
          stepFactory.txStep(
              conn -> {
                conn.createStatement().execute("SELECT 1");
                return "tx-result";
              },
              "txStep");
      // A stale execution's failure is not the owner's to replay: it is not recorded.
      case "txStepFailure" ->
          stepFactory.txStep(
              (JdbcStepFactory.TransactionalRunnable<RuntimeException>)
                  conn -> {
                    throw new IllegalStateException("app failure");
                  },
              "txStep");
      case "outcome" -> {}
      default -> throw new IllegalArgumentException(write);
    }
    return "own-result";
  }

  @Override
  @Workflow
  public String handedOff() throws InterruptedException {
    var workflowId = workflowId();
    int run = handedOffRuns.incrementAndGet();
    dbos.runStep(
        () -> {
          // The first execution is held here until the test releases it.
          if (run == 1) {
            waitAtGate(workflowId);
          }
          return run;
        },
        "heldStep");
    dbos.runStep(() -> afterStepRuns.incrementAndGet(), "afterStep");
    return "handed-off-" + run;
  }

  @Override
  @Workflow
  public String child() {
    return "child";
  }

  @Override
  @Workflow
  public String quick() {
    return "quick";
  }

  @Override
  @Workflow
  public String cancelSelf() {
    dbos.cancelWorkflow(workflowId());
    dbos.runStep(() -> afterStepRuns.incrementAndGet(), "afterStep");
    return "not cancelled";
  }

  @Override
  @Workflow
  public String deleteSelf() {
    dbos.deleteWorkflow(workflowId());
    dbos.runStep(() -> afterStepRuns.incrementAndGet(), "afterStep");
    return "not deleted";
  }

  @Override
  @Workflow
  public String resumeSelf() {
    resumeSelfRuns.incrementAndGet();
    dbos.resumeWorkflow(workflowId());
    return "resumed";
  }
}
