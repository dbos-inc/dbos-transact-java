package dev.dbos.transact.txstep;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.database.DBTestAccess;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.exceptions.DBOSSystemDatabaseException;
import dev.dbos.transact.internal.StepCheckpointStore;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Workflow;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

interface CompletionService {
  String twoSteps() throws SQLException;

  String failsAfterAStep() throws SQLException;

  String waitsAfterAStep() throws Exception;

  String alsoUsesABrokenStore() throws SQLException;

  String cancelledAfterAStep() throws Exception;

  String stepResultNotRecorded() throws SQLException;

  String interruptedThenFails() throws SQLException;
}

class CompletionServiceImpl implements CompletionService {
  private final DBOS dbos;
  private final JdbcStepFactory factory;
  final CountDownLatch atGate = new CountDownLatch(1);
  final CountDownLatch gate = new CountDownLatch(1);
  StepCheckpointStore brokenStore;
  StepCheckpointStore recordingStore;
  final AtomicInteger unrecordedRuns = new AtomicInteger();

  CompletionServiceImpl(DBOS dbos, JdbcStepFactory factory) {
    this.dbos = dbos;
    this.factory = factory;
  }

  @Override
  @Workflow
  public String twoSteps() throws SQLException {
    factory.txStep((Connection conn) -> {}, "one");
    factory.txStep((Connection conn) -> {}, "two");
    return "done";
  }

  @Override
  @Workflow
  public String failsAfterAStep() throws SQLException {
    factory.txStep((Connection conn) -> {}, "one");
    throw new IllegalStateException("workflow failed");
  }

  @Override
  @Workflow
  public String waitsAfterAStep() throws Exception {
    dbos.integration().useStepCheckpointStore(recordingStore);
    factory.txStep((Connection conn) -> {}, "one");
    atGate.countDown();
    gate.await();
    return "done";
  }

  @Override
  @Workflow
  public String cancelledAfterAStep() throws Exception {
    dbos.integration().useStepCheckpointStore(recordingStore);
    factory.txStep((Connection conn) -> {}, "one");
    atGate.countDown();
    gate.await();
    // The step after the cancel observes it.
    factory.txStep((Connection conn) -> {}, "two");
    return "done";
  }

  @Override
  @Workflow
  public String stepResultNotRecorded() throws SQLException {
    factory.txStep((Connection conn) -> unrecordedRuns.incrementAndGet(), "unrecorded");
    return "done";
  }

  @Override
  @Workflow
  public String interruptedThenFails() throws SQLException {
    dbos.integration().useStepCheckpointStore(recordingStore);
    factory.txStep((Connection conn) -> {}, "one");
    Thread.currentThread().interrupt();
    throw new IllegalStateException("interrupted");
  }

  @Override
  @Workflow
  public String alsoUsesABrokenStore() throws SQLException {
    dbos.integration().useStepCheckpointStore(brokenStore);
    factory.txStep((Connection conn) -> {}, "one");
    return "done";
  }
}

/** A workflow deletes its transactional step checkpoints when its execution finishes. */
public class CompletedStepCheckpointsTest {
  private static final String SCHEMA = "completion";

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  CompletionServiceImpl impl;
  CompletionService proxy;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dataSource = pgContainer.dataSource();
  }

  private void launch(boolean keepCompletedStepCheckpoints) {
    dbos = new DBOS(dbosConfig);
    if (keepCompletedStepCheckpoints) {
      DBOSTestAccess.keepCompletedStepCheckpoints(dbos);
    }
    var factory = new JdbcStepFactory(dbos, dataSource, SCHEMA);
    impl = new CompletionServiceImpl(dbos, factory);
    proxy = dbos.registerProxy(CompletionService.class, impl);
    dbos.launch();
  }

  /** A store that only counts the completion cleanups it is asked for. */
  private static StepCheckpointStore countingStore(AtomicInteger cleanups) {
    return new StepCheckpointStore() {
      @Override
      public void deleteCheckpoints(String workflowId, int fromStepId) {}

      @Override
      public void deleteCheckpoints(Collection<String> workflowIds) {}

      @Override
      public void deleteCompletedCheckpoints(String workflowId) {
        cleanups.incrementAndGet();
      }
    };
  }

  private int checkpoints(String workflowId) throws SQLException {
    return DBUtils.getTxStepRows(dataSource, workflowId, SCHEMA).size();
  }

  @Test
  public void keepingCheckpointsIsRefusedAfterLaunch() {
    launch(false);
    var refused =
        assertThrows(
            IllegalStateException.class, () -> DBOSTestAccess.keepCompletedStepCheckpoints(dbos));
    assertTrue(refused.getMessage().contains("after DBOS is launched"), refused.getMessage());
  }

  @Test
  public void aWorkflowThatReturnsDeletesItsCheckpoints() throws Exception {
    launch(false);
    try (var _o = new WorkflowOptions("wf-returns").setContext()) {
      assertEquals("done", proxy.twoSteps());
    }
    assertEquals(0, checkpoints("wf-returns"));
  }

  @Test
  public void aWorkflowThatThrowsDeletesItsCheckpoints() throws Exception {
    launch(false);
    try (var _o = new WorkflowOptions("wf-throws").setContext()) {
      assertThrows(IllegalStateException.class, proxy::failsAfterAStep);
    }
    assertEquals(0, checkpoints("wf-throws"));
    assertEquals("ERROR", DBUtils.getWorkflowRow(dataSource, "wf-throws").status());
  }

  @Test
  @Timeout(60)
  public void aRecoveredWorkflowDeletesTheRowsAnEarlierExecutionWrote() throws Exception {
    // The first execution finishes without deleting, standing in for one that crashed after its
    // steps but before its completion. Its steps are recorded in the system database too.
    launch(true);
    try (var _o = new WorkflowOptions("wf-recovered").setContext()) {
      proxy.twoSteps();
    }
    assertEquals(2, checkpoints("wf-recovered"));
    dbos.close();
    DBUtils.setWorkflowState(dataSource, "wf-recovered", "PENDING");

    // The recovery replays both steps from the system database without running them, and still
    // deletes their rows when it finishes.
    launch(false);
    assertEquals("done", dbos.retrieveWorkflow("wf-recovered").getResult());
    assertEquals(0, checkpoints("wf-recovered"));
  }

  @Test
  public void anExecutionThatLostTheWorkflowDeletesNothing() throws Exception {
    launch(false);
    // Used before the factory, so the cleanup reaches it first: this shows the cleanup ran, and the
    // row left behind shows the factory's delete then rolled back.
    var cleanups = new AtomicInteger();
    impl.recordingStore = countingStore(cleanups);
    var workflowId = "wf-lost";
    var handle = dbos.startWorkflow(proxy::waitsAfterAStep, new StartWorkflowOptions(workflowId));
    assertTrue(impl.atGate.await(10, TimeUnit.SECONDS));

    DBTestAccess.takeOwnership(DBOSTestAccess.getSystemDatabase(dbos), workflowId);
    impl.gate.countDown();

    // The stale execution's delete rolls back, and it waits for the new owner's outcome.
    var done =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return handle.getResult();
              } catch (Exception e) {
                throw new CompletionException(e);
              }
            });
    assertThrows(TimeoutException.class, () -> done.get(2, TimeUnit.SECONDS));
    assertEquals(1, checkpoints(workflowId));

    recordSuccessElsewhere(workflowId, "owner-result");
    assertEquals("owner-result", done.get(30, TimeUnit.SECONDS));
    assertEquals(1, cleanups.get());
    assertEquals(1, checkpoints(workflowId));
  }

  @Test
  public void aFailedDeleteDoesNotChangeTheOutcomeOrSkipTheOtherFactories() throws Exception {
    launch(false);
    var brokenCalls = new AtomicInteger();
    impl.brokenStore =
        new StepCheckpointStore() {
          @Override
          public void deleteCheckpoints(String workflowId, int fromStepId) {}

          @Override
          public void deleteCheckpoints(Collection<String> workflowIds) {}

          @Override
          public void deleteCompletedCheckpoints(String workflowId) throws SQLException {
            brokenCalls.incrementAndGet();
            throw new SQLException("datasource down");
          }
        };

    try (var _o = new WorkflowOptions("wf-broken-store").setContext()) {
      assertEquals("done", proxy.alsoUsesABrokenStore());
    }
    assertEquals(1, brokenCalls.get());
    assertEquals(0, checkpoints("wf-broken-store"));
    assertEquals("SUCCESS", DBUtils.getWorkflowRow(dataSource, "wf-broken-store").status());
  }

  @Test
  public void aWorkflowThatSeesItsOwnCancellationSkipsTheCleanup() throws Exception {
    launch(false);
    var cleanups = new AtomicInteger();
    impl.recordingStore = countingStore(cleanups);
    var workflowId = "wf-cancelled";
    var handle =
        dbos.startWorkflow(proxy::cancelledAfterAStep, new StartWorkflowOptions(workflowId));
    assertTrue(impl.atGate.await(10, TimeUnit.SECONDS));

    dbos.cancelWorkflow(workflowId);
    impl.gate.countDown();

    assertThrows(DBOSAwaitedWorkflowCancelledException.class, handle::getResult);
    assertEquals(0, cleanups.get());
    assertEquals(1, checkpoints(workflowId));
  }

  @Test
  @Timeout(60)
  public void aStepWhoseResultWasNotRecordedKeepsTheCheckpoints() throws Exception {
    launch(false);
    // The step's transaction commits, and then its result cannot be recorded in the system
    // database: the factory's checkpoint is the only record that it ran.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute(
          "ALTER TABLE dbos.operation_outputs ADD CONSTRAINT refuse_unrecorded"
              + " CHECK (function_name <> 'unrecorded')");
    }

    var workflowId = "wf-unrecorded";
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      var refused = assertThrows(DBOSSystemDatabaseException.class, proxy::stepResultNotRecorded);
      assertTrue(refused.getMessage().contains("unrecorded"), refused.getMessage());
    }
    assertEquals(1, impl.unrecordedRuns.get());
    assertEquals("ERROR", DBUtils.getWorkflowRow(dataSource, workflowId).status());
    assertEquals(1, checkpoints(workflowId));

    // Had the execution then crashed before recording that error, recovery would find the step in
    // the factory's checkpoint alone, and must replay it from there instead of running it again.
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("ALTER TABLE dbos.operation_outputs DROP CONSTRAINT refuse_unrecorded");
    }
    dbos.close();
    DBUtils.setWorkflowState(dataSource, workflowId, "PENDING");
    launch(false);
    assertEquals("done", dbos.retrieveWorkflow(workflowId).getResult());
    assertEquals(0, impl.unrecordedRuns.get());
    assertEquals(0, checkpoints(workflowId));
  }

  @Test
  public void anInterruptedWorkflowThatThrowsSkipsTheCleanup() throws Exception {
    launch(false);
    var cleanups = new AtomicInteger();
    impl.recordingStore = countingStore(cleanups);
    var workflowId = "wf-interrupted";
    var handle =
        dbos.startWorkflow(proxy::interruptedThenFails, new StartWorkflowOptions(workflowId));

    assertThrows(Exception.class, handle::getResult);
    assertEquals(0, cleanups.get());
    assertEquals(1, checkpoints(workflowId));
  }

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
}
