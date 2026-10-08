package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The workflow-timeout sweep cancels an application's workflows past their deadline, whatever state
 * they are in and whichever executor they belong to.
 *
 * <p>The statement is driven through a client's system database, so no executor's sweep runs under
 * the assertions; one test launches DBOS to check that the sweep runs on its own.
 */
public class WorkflowTimeoutSweepTest {

  static final String APP = "app-a";

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose HikariDataSource dataSource;
  @AutoClose DBOSClient client;
  SystemDatabase sysdb;

  @BeforeEach
  void beforeEach() {
    // Launching migrates the schema; the client needs nothing else from it.
    try (DBOS dbos = new DBOS(pgContainer.dbosConfig())) {
      dbos.launch();
    }
    dataSource = pgContainer.dataSource();
    client = pgContainer.dbosClient(APP);
    sysdb = DBOSTestAccess.getSystemDatabase(client);
  }

  /** Inserts a workflow row with a deadline the given distance from the database's now. */
  private String plant(WorkflowState status, long deadlineFromNowMs, String appName)
      throws SQLException {
    return plant(List.of(status), deadlineFromNowMs, appName).get(0);
  }

  private List<String> plant(List<WorkflowState> statuses, long deadlineFromNowMs, String appName)
      throws SQLException {
    var sql =
        """
          INSERT INTO "dbos".workflow_status
              (workflow_uuid, status, name, class_name, queue_name, executor_id,
               delay_until_epoch_ms, workflow_timeout_ms, workflow_deadline_epoch_ms,
               application_name, recovery_attempts, priority)
          VALUES (?, ?, 'timedOut', 'com.example.TimedOut', ?, 'dead-executor', ?, 1000,
                  %s + ?, ?, 0, 0)
        """
            .formatted(SystemDatabase.NOW_EPOCH_MS);
    var ids = new ArrayList<String>();
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      for (var status : statuses) {
        var id = UUID.randomUUID().toString();
        ids.add(id);
        stmt.setString(1, id);
        stmt.setString(2, status.name());
        // A queue nothing listens to, so only the sweep can move these rows.
        stmt.setString(3, status == WorkflowState.PENDING ? null : "unlistened-queue");
        stmt.setObject(4, status == WorkflowState.DELAYED ? Long.MAX_VALUE : null, Types.BIGINT);
        stmt.setLong(5, deadlineFromNowMs);
        stmt.setString(6, appName);
        stmt.addBatch();
      }
      stmt.executeBatch();
    }
    return ids;
  }

  private WorkflowState status(String id) throws SQLException {
    return WorkflowState.valueOf(DBUtils.getWorkflowRow(dataSource, id).status());
  }

  /**
   * Sweeps in batches of {@code limit} until every one of {@code expected} is cancelled, and
   * returns each pass's cancellations. On CockroachDB, SKIP LOCKED also skips a row that a
   * transaction which just committed wrote or locked, until its locks are cleaned up, so a pass
   * right after the rows were planted or swept can come back short; this waits out such a pass.
   */
  private List<List<String>> sweepUntilCancelled(Set<String> expected, int limit) throws Exception {
    var passes = new ArrayList<List<String>>();
    var cancelled = new HashSet<String>();
    long giveUp = System.nanoTime() + Duration.ofSeconds(10).toNanos();
    while (!cancelled.containsAll(expected)) {
      assertTrue(System.nanoTime() < giveUp, "the sweep did not cancel the expired rows");
      var pass = sysdb.cancelTimedOutWorkflows(limit);
      if (pass.isEmpty()) {
        Thread.sleep(100);
      } else {
        passes.add(pass);
        cancelled.addAll(pass);
      }
    }
    return passes;
  }

  private static Set<String> union(List<List<String>> passes) {
    var all = new HashSet<String>();
    passes.forEach(all::addAll);
    return all;
  }

  @Test
  void cancelsEveryActiveStatePastItsDeadline() throws Exception {
    var enqueued = plant(WorkflowState.ENQUEUED, -1_000, APP);
    var delayed = plant(WorkflowState.DELAYED, -1_000, APP);
    // Left PENDING by an executor that died: nothing is running it.
    var orphaned = plant(WorkflowState.PENDING, -1_000, APP);
    var expected = Set.of(enqueued, delayed, orphaned);

    assertEquals(expected, union(sweepUntilCancelled(expected, 1_000)));
    for (var id : expected) {
      var row = DBUtils.getWorkflowRow(dataSource, id);
      assertEquals(WorkflowState.CANCELLED.name(), row.status());
      assertNull(row.queueName());
      assertNull(row.deduplicationId());
      assertNotNull(row.completedAt(), "completed_at was not stamped");
    }
  }

  @Test
  void leavesUnexpiredFinishedAndOtherApplicationsRowsAlone() throws Exception {
    var notYet = plant(WorkflowState.ENQUEUED, 60_000, APP);
    var finished = plant(WorkflowState.SUCCESS, -1_000, APP);
    var peers = plant(WorkflowState.ENQUEUED, -1_000, "app-b");
    // Planted last, so once the sweep reaches it, it has seen the others too.
    var expired = plant(WorkflowState.ENQUEUED, -1_000, APP);

    assertEquals(Set.of(expired), union(sweepUntilCancelled(Set.of(expired), 1_000)));
    assertEquals(List.of(), sysdb.cancelTimedOutWorkflows(1_000));

    assertEquals(WorkflowState.ENQUEUED, status(notYet));
    assertEquals(WorkflowState.SUCCESS, status(finished));
    assertEquals(WorkflowState.ENQUEUED, status(peers));
  }

  @Test
  void cancelsOldestDeadlineFirstAndClearsAFullBacklogInBatches() throws Exception {
    var older = plant(WorkflowState.ENQUEUED, -10_000, APP);
    var backlog = plant(Collections.nCopies(4, WorkflowState.ENQUEUED), -1_000, APP);

    assertEquals(List.of(List.of(older)), sweepUntilCancelled(Set.of(older), 1));

    var passes = sweepUntilCancelled(Set.copyOf(backlog), 3);
    assertEquals(Set.copyOf(backlog), union(passes));
    for (var pass : passes) {
      assertTrue(pass.size() <= 3, "a pass cancelled more than its limit: " + pass);
    }
    assertEquals(List.of(), sysdb.cancelTimedOutWorkflows(3));
  }

  @Test
  void skipsARowAnotherTransactionHoldsAndCancelsItNextPass() throws Exception {
    var held = plant(WorkflowState.ENQUEUED, -1_000, APP);

    // A dequeue in flight: its transaction holds the row. (Its status is not read here: on
    // CockroachDB, a plain read waits for that lock.)
    try (var conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try (var stmt =
          conn.prepareStatement(
              "SELECT 1 FROM \"dbos\".workflow_status WHERE workflow_uuid = ? FOR UPDATE")) {
        stmt.setString(1, held);
        stmt.executeQuery().close();
      }

      assertEquals(List.of(), sysdb.cancelTimedOutWorkflows(1_000));
      conn.rollback();
    }

    assertEquals(Set.of(held), union(sweepUntilCancelled(Set.of(held), 1_000)));
  }

  @Test
  void aLaunchedExecutorSweepsWorkflowsItIsNotRunning() throws Exception {
    var enqueued = plant(WorkflowState.ENQUEUED, -1_000, APP);
    var orphaned = plant(WorkflowState.PENDING, -1_000, APP);

    try (DBOS dbos = new DBOS(pgContainer.dbosConfig(APP))) {
      dbos.launch();
      long giveUp = System.nanoTime() + Duration.ofSeconds(10).toNanos();
      while (status(enqueued) != WorkflowState.CANCELLED
          || status(orphaned) != WorkflowState.CANCELLED) {
        assertTrue(System.nanoTime() < giveUp, "the sweep did not cancel the expired rows");
        Thread.sleep(100);
      }
    }
  }
}
