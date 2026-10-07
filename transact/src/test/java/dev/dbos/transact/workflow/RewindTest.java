package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.internal.StepCheckpointStore;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.txstep.JdbcStepFactory;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.DBUtils.EventHistoryRow;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.TxStepOutputRow;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class RewindTest {

  private static final String SCHEMA = Constants.DB_SCHEMA;

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  // A pool without autocommit: a checkpoint delete that relied on autocommit would be rolled back
  // when its connection went back to this pool.
  @AutoClose HikariDataSource noAutoCommitDataSource;

  private RewindTestServiceImpl impl;
  private RewindTestService proxy;
  private JdbcStepFactory first;
  private JdbcStepFactory second;

  // When set, the extra checkpoint store registered in beforeEach fails its delete.
  private final AtomicBoolean failCheckpointDelete = new AtomicBoolean(false);

  @BeforeEach
  void beforeEach() throws SQLException {
    dbosConfig = pgContainer.dbosConfig();
    dataSource = pgContainer.dataSource();
    dbos = new DBOS(dbosConfig);

    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement()) {
      stmt.execute("DROP TABLE IF EXISTS rewind_rows");
      stmt.execute("CREATE TABLE rewind_rows (v TEXT NOT NULL)");
    }
    var noAutoCommit = new HikariConfig();
    noAutoCommit.setJdbcUrl(pgContainer.jdbcUrl());
    noAutoCommit.setUsername(pgContainer.username());
    noAutoCommit.setPassword(pgContainer.password());
    noAutoCommit.setAutoCommit(false);
    noAutoCommitDataSource = new HikariDataSource(noAutoCommit);

    // Two factories with separate checkpoint tables, standing in for two databases.
    first = new JdbcStepFactory(dbos, dataSource, "rewind_first");
    second = new JdbcStepFactory(dbos, noAutoCommitDataSource, "rewind_second");
    dbos.integration()
        .registerStepCheckpointStore(
            new StepCheckpointStore() {
              @Override
              public void deleteCheckpoints(String workflowId, int fromStepId) throws SQLException {
                if (failCheckpointDelete.get()) {
                  throw new SQLException("datasource down");
                }
              }

              @Override
              public void deleteCheckpoints(Collection<String> workflowIds) {}
            });

    impl = new RewindTestServiceImpl(dbos);
    proxy = dbos.registerProxy(RewindTestService.class, impl);
    impl.setProxy(proxy);
    impl.setStepFactories(first, second);

    dbos.launch();
  }

  // Runs a workflow to completion under a fresh ID and returns that ID.
  private String start(Supplier<?> workflow) {
    var workflowId = UUID.randomUUID().toString();
    try (var o = new WorkflowOptions(workflowId).setContext()) {
      workflow.get();
    }
    return workflowId;
  }

  private int stepIdOf(String workflowId, String functionName, int occurrence) {
    var matches =
        dbos.listWorkflowSteps(workflowId).stream()
            .filter(
                s ->
                    s.functionName().equals(functionName)
                        || s.functionName().endsWith("." + functionName))
            .map(StepInfo::functionId)
            .toList();
    assertTrue(
        matches.size() > occurrence,
        "%s has %d %s steps".formatted(workflowId, matches.size(), functionName));
    return matches.get(occurrence);
  }

  /**
   * A single-slot queue whose only worker is held until the returned handle is closed. A rewind
   * re-enqueues, so without this every assertion about what a rewind leaves behind races the queue
   * picking the workflow back up.
   */
  private AutoCloseable pausedQueue(String name) throws Exception {
    dbos.registerQueue(name, new QueueOptions().withConcurrency(1));
    var started = new java.util.concurrent.CountDownLatch(1);
    var released = new java.util.concurrent.CountDownLatch(1);
    impl.blockerStarted = started;
    impl.blockerReleased = released;
    var handle =
        dbos.startWorkflow(() -> proxy.blocker(), new StartWorkflowOptions().withQueue(name));
    assertTrue(started.await(10, TimeUnit.SECONDS), name + " blocker never started");
    return () -> {
      released.countDown();
      handle.getResult();
    };
  }

  private record MailboxRow(Object message, boolean consumed, Integer consumedBy) {}

  private List<MailboxRow> mailbox(String workflowId) throws SQLException {
    var sql =
        """
        SELECT message, serialization, consumed, consumed_by_function_id
        FROM "%s".notifications WHERE destination_uuid = ? ORDER BY created_at_epoch_ms
        """
            .formatted(SCHEMA);
    var rows = new ArrayList<MailboxRow>();
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        while (rs.next()) {
          rows.add(
              new MailboxRow(
                  SerializationUtil.deserializeValue(
                      rs.getString("message"), rs.getString("serialization"), null),
                  rs.getBoolean("consumed"),
                  rs.getObject("consumed_by_function_id", Integer.class)));
        }
      }
    }
    return rows;
  }

  private List<Integer> eventHistoryIds(String workflowId) throws SQLException {
    return DBUtils.getWorkflowEventHistory(dataSource, workflowId).stream()
        .map(EventHistoryRow::stepId)
        .sorted()
        .toList();
  }

  private List<Object> readStream(String workflowId, String key) {
    var values = new ArrayList<Object>();
    dbos.readStream(workflowId, key).forEachRemaining(values::add);
    return values;
  }

  private List<Integer> txCheckpoints(String schema, String workflowId) throws SQLException {
    return DBUtils.getTxStepRows(dataSource, workflowId, schema).stream()
        .map(TxStepOutputRow::stepId)
        .sorted()
        .toList();
  }

  private List<String> tableRows() throws SQLException {
    var rows = new ArrayList<String>();
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT v FROM rewind_rows ORDER BY v")) {
      while (rs.next()) {
        rows.add(rs.getString(1));
      }
    }
    return rows;
  }

  @Test
  public void rewindReplaysTheStepsBeforeTheCut() throws Exception {
    var workflowId = start(() -> proxy.fiveSteps("five"));
    assertEquals(1, impl.stepRunsOf("three"));

    WorkflowHandle<String, RuntimeException> handle = dbos.rewindWorkflow(workflowId, 2);
    assertEquals(workflowId, handle.workflowId());
    assertEquals("run2", handle.getResult());
    assertEquals(WorkflowState.SUCCESS, handle.getStatus().status());

    // Steps 0 and 1 replayed from their checkpoints; everything from step 2 ran again.
    assertEquals(1, impl.stepRunsOf("one"));
    assertEquals(1, impl.stepRunsOf("two"));
    assertEquals(2, impl.stepRunsOf("three"));
    assertEquals(2, impl.stepRunsOf("four"));
    assertEquals(2, impl.stepRunsOf("five"));
    assertEquals(5, dbos.listWorkflowSteps(workflowId).size());

    // Step 0 is the whole history.
    assertEquals("run3", dbos.rewindWorkflow(workflowId, 0).getResult());
    assertEquals(2, impl.stepRunsOf("one"));
    assertEquals(3, impl.stepRunsOf("five"));
  }

  @Test
  public void rewindDeletesNotifications() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    WorkflowHandle<String, RuntimeException> handle =
        dbos.startWorkflow(
            () -> proxy.receiver("partial-delete"), new StartWorkflowOptions(workflowId));
    dbos.send(workflowId, "a", "cmd");
    dbos.send(workflowId, "b", "cmd");
    assertEquals("ab:1", handle.getResult());

    // Each recv stamped the row it took with its own step, which is what lets the rewind delete
    // exactly the messages the discarded steps consumed.
    var firstRecv = stepIdOf(workflowId, "DBOS.recv", 0);
    var secondRecv = stepIdOf(workflowId, "DBOS.recv", 1);
    // A message that arrives once the workflow is done sits unconsumed.
    dbos.send(workflowId, "stray", "cmd");
    assertEquals(
        List.of(
            new MailboxRow("a", true, firstRecv),
            new MailboxRow("b", true, secondRecv),
            new MailboxRow("stray", false, null)),
        mailbox(workflowId));

    try (var q = pausedQueue("rewind_delete_gate")) {
      dbos.rewindWorkflow(
          workflowId, secondRecv, new RewindOptions().withQueue("rewind_delete_gate"));
      // The message the discarded step took is gone, and so is the one still waiting. The first
      // recv's message stays consumed: its step survived the cut.
      assertEquals(List.of(new MailboxRow("a", true, firstRecv)), mailbox(workflowId));
      // A message that arrives after the cut is what the replayed recv gets.
      dbos.send(workflowId, "c", "cmd");
    }

    assertEquals("ac:2", dbos.retrieveWorkflow(workflowId).getResult());
    assertEquals(
        List.of(new MailboxRow("a", true, firstRecv), new MailboxRow("c", true, secondRecv)),
        mailbox(workflowId));
  }

  @Test
  public void rewindUnpublishesEvents() throws Exception {
    var workflowId = start(() -> proxy.publisher("events"));
    assertEquals(
        Map.of("below", "kept", "both", "new", "above", "doomed"), dbos.getAllEvents(workflowId));

    // Cut at the third setEvent, so "below" and the first "both" survive.
    var cut = stepIdOf(workflowId, "DBOS.setEvent", 2);

    try (var q = pausedQueue("rewind_events_gate")) {
      dbos.rewindWorkflow(workflowId, cut, new RewindOptions().withQueue("rewind_events_gate"));
      // "below" was never touched past the cut; "both" reverts to its last value from below the
      // cut; "above" was only ever published past the cut, so it is gone.
      assertEquals(Map.of("below", "kept", "both", "old"), dbos.getAllEvents(workflowId));
      assertEquals(List.of(0, 1), eventHistoryIds(workflowId));

      // And that is what a peer reading by key sees.
      assertEquals(
          "old", dbos.getEvent(workflowId, "both", java.time.Duration.ofSeconds(1)).orElseThrow());
      assertTrue(
          dbos.getEvent(workflowId, "above", java.time.Duration.ofMillis(100)).isEmpty(),
          "an event published only past the cut stays unpublished");
    }

    assertEquals("second", dbos.retrieveWorkflow(workflowId).getResult());
    assertEquals(Map.of("below", "kept", "both", "republished"), dbos.getAllEvents(workflowId));
  }

  @Test
  public void rewindKeepsStreamEntries() throws Exception {
    var workflowId = start(() -> proxy.streamWriter("stream-keep"));
    assertEquals(List.of("a1", "b1"), readStream(workflowId, "log"));

    assertEquals("run2", dbos.rewindWorkflow(workflowId, 0).getResult());

    // Offsets are addresses peers read by, so the discarded run's entries keep theirs and the
    // replay appends. Deleting them would hand offset 0 a new value.
    assertEquals(List.of("a1", "b1", "a2", "b2"), readStream(workflowId, "log"));
    assertEquals(
        List.of(0, 1, 2, 3),
        DBUtils.getStreamEntries(dataSource, workflowId).stream()
            .map(DBUtils.StreamRow::offset)
            .toList());
  }

  @Test
  public void rewindReopensAClosedStream() throws Exception {
    var workflowId = start(() -> proxy.streamCloser("stream-close"));
    assertEquals(List.of("v1"), readStream(workflowId, "out"));

    // The close marker ends every reader that reaches it, so one left over from the discarded run
    // would hide the replay's entry.
    assertEquals("run2", dbos.rewindWorkflow(workflowId, 0).getResult());
    assertEquals(List.of("v1", "v2"), readStream(workflowId, "out"));

    // Cut past the close, the marker is not the discarded run's to undo: its step survives, so
    // nothing replays it and it has to stay.
    assertEquals("run3", dbos.rewindWorkflow(workflowId, 2).getResult());
    var rows = DBUtils.getStreamEntries(dataSource, workflowId);
    assertEquals(3, rows.size());
    assertTrue(rows.get(2).value().contains("__DBOS_STREAM_CLOSED__"), rows.toString());
    assertEquals(List.of("v1", "v2"), readStream(workflowId, "out"));
  }

  @Test
  public void rewindTheChildThenTheParentToRepairAFailure() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    WorkflowHandle<Integer, Exception> handle =
        dbos.startWorkflow(() -> proxy.parent("repair"), new StartWorkflowOptions(workflowId));
    var thrown = assertThrows(IllegalStateException.class, handle::getResult);
    assertEquals("child is bogus", thrown.getMessage());

    var childId =
        dbos.listWorkflowSteps(workflowId).stream()
            .map(StepInfo::childWorkflowId)
            .filter(id -> id != null)
            .findFirst()
            .orElseThrow();
    assertEquals(WorkflowState.ERROR, dbos.retrieveWorkflow(childId).getStatus().status());
    assertEquals(WorkflowState.ERROR, dbos.retrieveWorkflow(workflowId).getStatus().status());

    // Repair the child on its own first.
    assertEquals(42, dbos.<Integer, RuntimeException>rewindWorkflow(childId, 0).getResult());

    // Then rewind the parent to the getResult that failed. The step that started the child
    // survives, and the replay picks up the repaired result.
    var getResultStep = stepIdOf(workflowId, "DBOS.getResult", 0);
    assertEquals(
        42, dbos.<Integer, RuntimeException>rewindWorkflow(workflowId, getResultStep).getResult());
    assertEquals(2, impl.childRuns.get());
  }

  @Test
  public void aRewoundParentAdoptsItsExistingChild() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    WorkflowHandle<Integer, Exception> handle =
        dbos.startWorkflow(
            () -> proxy.adoptingParent("adopt"), new StartWorkflowOptions(workflowId));
    assertEquals(43, handle.getResult());
    assertEquals(1, impl.doublerRuns.get());
    var childId =
        dbos.listWorkflowSteps(workflowId).stream()
            .map(StepInfo::childWorkflowId)
            .filter(id -> id != null)
            .findFirst()
            .orElseThrow();

    // The cut is before the step that started the child. The replay starts it again under the
    // same ID, finds it already finished, and takes its result without running it.
    assertEquals(44, dbos.<Integer, Exception>rewindWorkflow(workflowId, 0).getResult());
    assertEquals(1, impl.doublerRuns.get());
    assertEquals(42, dbos.retrieveWorkflow(childId).getResult());
  }

  @Test
  public void rewindOntoAQueueWithAPartitionKey() throws Exception {
    dbos.registerQueue("rewind_partitioned", new QueueOptions().withPartitionConcurrency(1));

    var workflowId = UUID.randomUUID().toString();
    WorkflowHandle<Integer, RuntimeException> handle =
        dbos.startWorkflow(
            () -> proxy.counter("partition"),
            new StartWorkflowOptions(workflowId)
                .withQueue("rewind_partitioned")
                .withQueuePartitionKey("original"));
    assertEquals(1, handle.getResult());

    dbos.rewindWorkflow(
        workflowId,
        0,
        new RewindOptions().withQueue("rewind_partitioned").withQueuePartitionKey("repaired"));
    assertEquals(2, dbos.retrieveWorkflow(workflowId).getResult());
    var status = dbos.retrieveWorkflow(workflowId).getStatus();
    assertEquals("rewind_partitioned", status.queueName());
    assertEquals("repaired", status.queuePartitionKey());

    // Omitting the key clears it; omitting the queue falls back to the internal queue.
    dbos.rewindWorkflow(workflowId, 0);
    assertEquals(3, dbos.retrieveWorkflow(workflowId).getResult());
    status = dbos.retrieveWorkflow(workflowId).getStatus();
    assertEquals(Constants.DBOS_INTERNAL_QUEUE, status.queueName());
    assertNull(status.queuePartitionKey());

    // A partition key needs a partitioned queue, and nothing is written when it is refused.
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.rewindWorkflow(workflowId, 0, new RewindOptions().withQueuePartitionKey("pk")));
    assertEquals(WorkflowState.SUCCESS, dbos.retrieveWorkflow(workflowId).getStatus().status());
  }

  @Test
  public void rewindOntoADifferentApplicationVersion() throws Exception {
    var workflowId = start(() -> proxy.counter("version"));
    var runningVersion = DBUtils.getWorkflowRow(dataSource, workflowId).applicationVersion();

    // Dequeueing matches on application version, so a workflow restamped with a version nothing
    // is running stays enqueued instead of replaying.
    dbos.rewindWorkflow(
        workflowId, 0, new RewindOptions().withApplicationVersion("not-this-deployment"));
    var row = DBUtils.getWorkflowRow(dataSource, workflowId);
    assertEquals("not-this-deployment", row.applicationVersion());
    Thread.sleep(2500); // several queue polls, any of which would pick it up
    assertEquals(
        WorkflowState.ENQUEUED.name(), DBUtils.getWorkflowRow(dataSource, workflowId).status());
    assertEquals(1, impl.runsOf("version"));

    // The workflow is ENQUEUED now, and only a terminal workflow can be rewound.
    var refused =
        assertThrows(
            IllegalStateException.class,
            () ->
                dbos.rewindWorkflow(
                    workflowId, 0, new RewindOptions().withApplicationVersion(runningVersion)));
    assertTrue(refused.getMessage().contains("only a workflow in a terminal state"));
    dbos.cancelWorkflow(workflowId);

    // Restamped with the version this executor runs, it replays.
    dbos.rewindWorkflow(workflowId, 0, new RewindOptions().withApplicationVersion(runningVersion));
    assertEquals(2, dbos.retrieveWorkflow(workflowId).getResult());

    // Omitted, the workflow keeps the version it already had.
    assertEquals(3, dbos.rewindWorkflow(workflowId, 0).getResult());
    assertEquals(
        runningVersion, DBUtils.getWorkflowRow(dataSource, workflowId).applicationVersion());
  }

  @Test
  public void databaseStateBetweenRewindAndReplay() throws Exception {
    var workflowId = start(() -> proxy.publisher("dbstate"));
    var before = DBUtils.getWorkflowRow(dataSource, workflowId);
    assertEquals(WorkflowState.SUCCESS.name(), before.status());
    assertNotNull(before.completedAt());

    // An outcome written by an SDK that predates the workflow_output table sits in the legacy
    // columns, and reads fall back to it. Move this one there.
    try (var conn = dataSource.getConnection()) {
      try (var stmt =
          conn.prepareStatement(
              """
              UPDATE "%1$s".workflow_status
              SET output = (SELECT output FROM "%1$s".workflow_output WHERE workflow_uuid = ?)
              WHERE workflow_uuid = ?
              """
                  .formatted(SCHEMA))) {
        stmt.setString(1, workflowId);
        stmt.setString(2, workflowId);
        assertEquals(1, stmt.executeUpdate());
      }
      try (var stmt =
          conn.prepareStatement(
              "DELETE FROM \"%s\".workflow_output WHERE workflow_uuid = ?".formatted(SCHEMA))) {
        stmt.setString(1, workflowId);
        assertEquals(1, stmt.executeUpdate());
      }
    }
    assertEquals("first", dbos.retrieveWorkflow(workflowId).getStatus().output());

    try (var q = pausedQueue("rewind_gate")) {
      dbos.rewindWorkflow(
          workflowId, 0, new RewindOptions().withQueue("rewind_gate").withQueuePartitionKey(null));

      var after = DBUtils.getWorkflowRow(dataSource, workflowId);
      assertEquals(WorkflowState.ENQUEUED.name(), after.status());
      assertEquals("rewind_gate", after.queueName());
      assertEquals(0L, after.recoveryAttempts());
      assertNull(after.startedAtEpochMs());
      assertNull(after.completedAt());
      assertNull(after.deadlineEpochMs());
      assertNull(after.deduplicationId());
      // The workflow's identity is untouched.
      assertEquals(before.workflowName(), after.workflowName());
      assertEquals(before.createdAt(), after.createdAt());
      assertEquals(before.inputs(), after.inputs());

      // Both outcome shapes are gone: no workflow_output row, and no legacy columns to fall
      // back to.
      assertNull(after.output());
      assertNull(after.error());
      assertNull(dbos.retrieveWorkflow(workflowId).getStatus().output());

      assertEquals(List.of(), DBUtils.getStepRows(dataSource, workflowId));
      assertEquals(List.of(), eventHistoryIds(workflowId));
      assertEquals(Map.of(), dbos.getAllEvents(workflowId));
    }

    assertEquals("second", dbos.retrieveWorkflow(workflowId).getResult());
  }

  @Test
  public void rewindRefusals() throws Exception {
    assertThrows(
        DBOSNonExistentWorkflowException.class,
        () -> dbos.rewindWorkflow(UUID.randomUUID().toString(), 0));

    var workflowId = start(() -> proxy.counter("validation"));
    var refused =
        assertThrows(IllegalArgumentException.class, () -> dbos.rewindWorkflow(workflowId, -1));
    assertTrue(refused.getMessage().contains("must be >= 0"), refused.getMessage());
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.rewindWorkflow(workflowId, 0, new RewindOptions().withQueue("no-such-queue")));

    // Nothing was written.
    assertEquals(WorkflowState.SUCCESS, dbos.retrieveWorkflow(workflowId).getStatus().status());
    assertEquals(1, impl.runsOf("validation"));
  }

  @Test
  public void theSystemDatabaseRefusesAnActiveWorkflow() throws Exception {
    // The client never consults the step factories, so this reaches the system database's own
    // status check rather than the pre-check the in-process rewind makes when factories exist.
    var started = new java.util.concurrent.CountDownLatch(1);
    var released = new java.util.concurrent.CountDownLatch(1);
    impl.blockerStarted = started;
    impl.blockerReleased = released;
    WorkflowHandle<String, InterruptedException> handle = dbos.startWorkflow(() -> proxy.blocker());
    assertTrue(started.await(10, TimeUnit.SECONDS));
    try (var client = pgContainer.dbosClient()) {
      var refused =
          assertThrows(
              IllegalStateException.class, () -> client.rewindWorkflow(handle.workflowId(), 0));
      assertTrue(refused.getMessage().contains("only a workflow in a terminal state"));
      assertTrue(refused.getMessage().contains("PENDING"), refused.getMessage());
    } finally {
      released.countDown();
    }
    assertEquals("held", handle.getResult());
  }

  @Test
  public void aStatusThisSdkDoesNotKnowCountsAsTerminal() throws Exception {
    // A status a newer SDK wrote is not one of the active ones, so the rewind goes ahead.
    var workflowId = start(() -> proxy.counter("future-status"));
    DBUtils.setWorkflowState(dataSource, workflowId, "SOME_FUTURE_STATUS");
    assertEquals(2, dbos.<Integer, RuntimeException>rewindWorkflow(workflowId, 0).getResult());
  }

  @Test
  public void clientRewind() throws Exception {
    var workflowId = start(() -> proxy.counter("client"));
    try (var client = pgContainer.dbosClient()) {
      assertEquals(2, client.<Integer, RuntimeException>rewindWorkflow(workflowId, 0).getResult());
      assertEquals(
          3,
          client
              .<Integer, RuntimeException>rewindWorkflow(
                  workflowId, 0, new RewindOptions().withQueue(Constants.DBOS_INTERNAL_QUEUE))
              .getResult());
      assertThrows(
          DBOSNonExistentWorkflowException.class,
          () -> client.rewindWorkflow(UUID.randomUUID().toString(), 0));
    }
  }

  @Test
  public void rewindFromInsideAWorkflowIsCheckpointed() throws Exception {
    var targetId = start(() -> proxy.counter("repaired"));
    assertEquals(1, impl.runsOf("repaired"));

    var repairerId = start(() -> proxy.repairer(targetId));
    assertEquals(2, dbos.retrieveWorkflow(repairerId).getResult());
    assertEquals(2, impl.runsOf("repaired"));
    stepIdOf(repairerId, "DBOS.rewindWorkflow", 0);

    // Crash and recover the repairer with its checkpoints intact. A second rewind would either
    // run the target again or be refused, because the first left it ENQUEUED.
    DBUtils.setWorkflowState(dataSource, repairerId, WorkflowState.PENDING.name());
    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    var recovered = executor.recoverPendingWorkflows(List.of(executor.executorId()));
    assertTrue(recovered.contains(repairerId), "repairer was not recovered");
    assertEquals(2, dbos.retrieveWorkflow(repairerId).getResult());

    // The repairer's body ran again, but the rewind did not.
    assertEquals(2, impl.runsOf("repairer"));
    assertEquals(2, impl.runsOf("repaired"));
  }

  @Test
  public void rewindFromInsideAStepRunsUnderTheStepsCheckpoint() throws Exception {
    var targetId = start(() -> proxy.counter("rewound-in-step"));
    var callerId = start(() -> proxy.stepRewinder(targetId));
    assertEquals(2, dbos.retrieveWorkflow(targetId).getResult());

    // Inside a step the rewind runs directly, with no checkpoint of its own: the caller's only
    // step is the one that made the call.
    var steps = dbos.listWorkflowSteps(callerId);
    assertEquals(1, steps.size(), steps.toString());
    assertTrue(steps.get(0).functionName().endsWith("rewindInStep"), steps.toString());

    // That step's checkpoint is what keeps a recovered caller from rewinding the target again.
    DBUtils.setWorkflowState(dataSource, callerId, WorkflowState.PENDING.name());
    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    var recovered = executor.recoverPendingWorkflows(List.of(executor.executorId()));
    assertTrue(recovered.contains(callerId), "caller was not recovered");
    assertEquals(targetId, dbos.retrieveWorkflow(callerId).getResult());
    assertEquals(2, impl.runsOf("stepRewinder"));
    assertEquals(2, impl.runsOf("rewound-in-step"));
  }

  @Test
  public void rewindDropsStepFactoryCheckpointsPastTheCut() throws Exception {
    var workflowId = start(() -> sneaky(() -> proxy.txWriter("tx")));
    assertEquals(List.of(0, 2), txCheckpoints("rewind_first", workflowId));
    assertEquals(List.of(1, 3), txCheckpoints("rewind_second", workflowId));

    // A step the system database would reject never reaches the checkpoints.
    assertThrows(IllegalArgumentException.class, () -> dbos.rewindWorkflow(workflowId, -1));
    assertEquals(List.of(0, 2), txCheckpoints("rewind_first", workflowId));

    // Cut at the third step: each factory keeps one checkpoint and loses one.
    try (var q = pausedQueue("rewind_tx_gate")) {
      dbos.rewindWorkflow(workflowId, 2, new RewindOptions().withQueue("rewind_tx_gate"));
      assertEquals(List.of(0), txCheckpoints("rewind_first", workflowId));
      assertEquals(List.of(1), txCheckpoints("rewind_second", workflowId));
    }

    assertEquals(2, dbos.retrieveWorkflow(workflowId).getResult());
    assertEquals(List.of(0, 2), txCheckpoints("rewind_first", workflowId));
    assertEquals(List.of(1, 3), txCheckpoints("rewind_second", workflowId));
    // The transactions before the cut replayed; the ones past it ran again.
    assertEquals(List.of("a", "b", "c", "c", "d", "d"), tableRows());
  }

  @Test
  public void aFailedCheckpointDeleteLeavesTheWorkflowUntouched() throws Exception {
    var workflowId = start(() -> sneaky(() -> proxy.txWriter("delete-failure")));

    failCheckpointDelete.set(true);
    var thrown = assertThrows(RuntimeException.class, () -> dbos.rewindWorkflow(workflowId, 0));
    assertEquals(
        "Failed to delete the transactional step checkpoints of workflow " + workflowId,
        thrown.getMessage());
    // The system database was not rewound, so the workflow keeps its terminal status and is not
    // re-enqueued.
    assertEquals(WorkflowState.SUCCESS, dbos.retrieveWorkflow(workflowId).getStatus().status());
    assertEquals(1, impl.runsOf("delete-failure"));

    // A delete is idempotent, so retrying the rewind finishes the job.
    failCheckpointDelete.set(false);
    assertEquals(2, dbos.rewindWorkflow(workflowId, 0).getResult());
    assertEquals(List.of(0, 2), txCheckpoints("rewind_first", workflowId));
    assertEquals(List.of(1, 3), txCheckpoints("rewind_second", workflowId));
    assertEquals(List.of("a", "a", "b", "b", "c", "c", "d", "d"), tableRows());
  }

  @Test
  public void rewindRefusesAnActiveWorkflowBeforeTouchingCheckpoints() throws Exception {
    WorkflowHandle<Void, Exception> handle = dbos.startWorkflow(() -> proxy.txBlocker());
    assertTrue(impl.txCheckpointed.await(10, TimeUnit.SECONDS));
    assertEquals(List.of(0), txCheckpoints("rewind_first", handle.workflowId()));

    var refused =
        assertThrows(
            IllegalStateException.class, () -> dbos.rewindWorkflow(handle.workflowId(), 0));
    assertTrue(refused.getMessage().contains("only a workflow in a terminal state"));
    assertEquals(List.of(0), txCheckpoints("rewind_first", handle.workflowId()));

    impl.txReleased.countDown();
    handle.getResult();
  }

  @FunctionalInterface
  private interface SqlSupplier<T> {
    T get() throws SQLException;
  }

  private static <T> T sneaky(SqlSupplier<T> supplier) {
    try {
      return supplier.get();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
