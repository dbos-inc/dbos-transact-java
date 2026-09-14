package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.dao.WorkflowDAO;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.PgContainer;

import java.sql.Connection;
import java.time.Instant;
import java.util.List;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The two-sweep retention round: a batched status sweep that never materializes workflow ids, and a
 * payload sweep that reclaims what it orphaned by retention_timestamp.
 */
class RetentionSweepTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose HikariDataSource dataSource;

  DbContext ctx;

  @BeforeEach
  void setup() throws Exception {
    MigrationManager.runMigrations(pgContainer.dbosConfig());
    dataSource = pgContainer.dataSource();
    try (var conn = dataSource.getConnection()) {
      PgContainer.resetDbosTables(conn);
    }
    ctx =
        new DbContext(
            dataSource, Constants.DB_SCHEMA, null, () -> false, null, null, new PollingLimiter(0));
  }

  /** A status row plus the three child rows a completed workflow leaves behind. */
  private void seedWorkflow(String id, String status, long createdAt) throws Exception {
    seedWorkflow(id, status, createdAt, createdAt);
  }

  /**
   * As above, but with the completion stamped apart from the creation. A terminal row carries a
   * completed_at, which is what the status sweep collects on; an in-flight one holds NULL.
   */
  private void seedWorkflow(String id, String status, long createdAt, long completedAt)
      throws Exception {
    var terminal = List.of("SUCCESS", "ERROR", "CANCELLED").contains(status);
    try (var conn = dataSource.getConnection()) {
      exec(
          conn,
          """
          INSERT INTO dbos.workflow_status(workflow_uuid, name, class_name, config_name, status, created_at, completed_at)
          VALUES (?, 'wf', 'C', '', ?, ?, ?)
          """,
          id,
          status,
          createdAt,
          terminal ? completedAt : null);
      exec(
          conn,
          "INSERT INTO dbos.workflow_input(workflow_uuid, inputs, retention_timestamp)"
              + " VALUES (?, '[]', ?)",
          id,
          completedAt);
      exec(
          conn,
          "INSERT INTO dbos.workflow_output(workflow_uuid, output, retention_timestamp)"
              + " VALUES (?, 'null', ?)",
          id,
          completedAt);
      exec(
          conn,
          "INSERT INTO dbos.operation_outputs(workflow_uuid, function_id, function_name,"
              + " retention_timestamp) VALUES (?, 1, 'step', ?)",
          id,
          completedAt);
    }
  }

  private void exec(Connection conn, String sql, Object... args) throws Exception {
    try (var stmt = conn.prepareStatement(sql)) {
      for (int i = 0; i < args.length; i++) {
        if (args[i] == null) {
          stmt.setNull(i + 1, java.sql.Types.BIGINT);
        } else if (args[i] instanceof Long l) {
          stmt.setLong(i + 1, l);
        } else {
          stmt.setString(i + 1, (String) args[i]);
        }
      }
      stmt.executeUpdate();
    }
  }

  private int count(String table) throws Exception {
    try (var conn = dataSource.getConnection();
        var stmt = conn.createStatement();
        var rs = stmt.executeQuery("SELECT COUNT(*) FROM dbos." + table)) {
      rs.next();
      return rs.getInt(1);
    }
  }

  @Test
  void batchedSweepCollectsEverythingAcrossBatches() throws Exception {
    var base = System.currentTimeMillis() - 100_000;
    for (int i = 0; i < 7; i++) {
      seedWorkflow("wf-" + i, "SUCCESS", base + i);
    }
    assertEquals(7, count("workflow_status"));

    // batchSize 2 over 7 rows: three bounded batches plus the unbounded remainder.
    WorkflowDAO.garbageCollect(ctx, Instant.now(), null, 2);

    assertEquals(0, count("workflow_status"), "every eligible status row should be collected");
    assertEquals(0, count("workflow_input"), "orphaned inputs should be swept");
    assertEquals(0, count("workflow_output"), "orphaned outputs should be swept");
    assertEquals(0, count("operation_outputs"), "orphaned steps should be swept");
  }

  @Test
  void batchSizeOfOneCollectsEverything() throws Exception {
    var base = System.currentTimeMillis() - 100_000;
    for (int i = 0; i < 5; i++) {
      seedWorkflow("wf-" + i, "SUCCESS", base + i);
    }

    WorkflowDAO.garbageCollect(ctx, Instant.now(), null, 1);

    assertEquals(0, count("workflow_status"));
    assertEquals(0, count("workflow_input"));
  }

  @Test
  void tiedTimestampsDoNotStallTheSweep() throws Exception {
    // Every row shares a completed_at, so no watermark can separate them: the batch bound has to
    // take the whole tie rather than loop forever on it.
    var same = System.currentTimeMillis() - 100_000;
    for (int i = 0; i < 6; i++) {
      seedWorkflow("wf-" + i, "SUCCESS", same);
    }

    WorkflowDAO.garbageCollect(ctx, Instant.now(), null, 2);

    assertEquals(0, count("workflow_status"));
    assertEquals(0, count("operation_outputs"));
  }

  @Test
  void payloadSweepSparesLiveWorkflows() throws Exception {
    var old = System.currentTimeMillis() - 100_000;
    // PENDING, so it holds no completed_at and the status sweep must skip it -- and the payload
    // sweep must then see it as still present and leave its payload rows alone.
    seedWorkflow("live", "PENDING", old);
    seedWorkflow("done", "SUCCESS", old);

    WorkflowDAO.garbageCollect(ctx, Instant.now(), null, 2);

    assertEquals(1, count("workflow_status"), "the PENDING workflow must survive");
    assertEquals(1, count("workflow_input"), "a live workflow keeps its inputs");
    assertEquals(1, count("workflow_output"), "a live workflow keeps its output");
    assertEquals(1, count("operation_outputs"), "a live workflow keeps its steps");
  }

  @Test
  void payloadSweepReclaimsPreexistingOrphans() throws Exception {
    // Payload rows whose status row is already gone. workflow_input and workflow_output never had
    // a foreign key, so they can strand at any schema version. operation_outputs cannot yet: its
    // foreign key still cascades until migration 112 drops it, which is exactly why the sweep has
    // to cover that table before then.
    var old = System.currentTimeMillis() - 100_000;
    try (var conn = dataSource.getConnection()) {
      exec(
          conn,
          "INSERT INTO dbos.workflow_input(workflow_uuid, inputs, retention_timestamp)"
              + " VALUES ('ghost', '[]', ?)",
          old);
      exec(
          conn,
          "INSERT INTO dbos.workflow_output(workflow_uuid, output, retention_timestamp)"
              + " VALUES ('ghost', 'null', ?)",
          old);
    }
    // A collectable workflow, so the round has a cutoff to work from.
    seedWorkflow("done", "SUCCESS", old);

    WorkflowDAO.garbageCollect(ctx, Instant.now(), null, 10);

    assertEquals(0, count("workflow_input"), "pre-existing orphaned inputs should be swept");
    assertEquals(0, count("workflow_output"), "pre-existing orphaned outputs should be swept");
  }

  @Test
  void recentRowsSurviveTheCutoff() throws Exception {
    seedWorkflow("old", "SUCCESS", System.currentTimeMillis() - 100_000);
    seedWorkflow("new", "SUCCESS", System.currentTimeMillis() + 100_000);

    WorkflowDAO.garbageCollect(ctx, Instant.now(), null, 2);

    assertEquals(1, count("workflow_status"), "a workflow above the cutoff must survive");
    assertEquals(1, count("workflow_input"), "and keep its payload rows");
  }

  @Test
  void longRunningWorkflowsAreCollectedByCompletion() throws Exception {
    // Created well before the cutoff but completed after it: sweeping on created_at would drop the
    // status row and then strand its payloads, which the payload sweep's lower bound never
    // revisits.
    var created = System.currentTimeMillis() - 100_000;
    seedWorkflow("slow", "SUCCESS", created, System.currentTimeMillis() + 100_000);

    WorkflowDAO.garbageCollect(ctx, Instant.now(), null, 2);

    assertEquals(1, count("workflow_status"), "a workflow completed above the cutoff must survive");
    assertEquals(1, count("workflow_input"), "and keep its inputs");
    assertEquals(1, count("workflow_output"), "and keep its output");
    assertEquals(1, count("operation_outputs"), "and keep its steps");
  }

  @Test
  void theRoundIsSystemWide() throws Exception {
    // Retention applies to the whole system database, even where several applications share it.
    var old = System.currentTimeMillis() - 100_000;
    seedWorkflow("mine", "SUCCESS", old);
    seedWorkflow("theirs", "SUCCESS", old);
    try (var conn = dataSource.getConnection()) {
      exec(
          conn,
          "UPDATE dbos.workflow_status SET application_name = 'other-app'"
              + " WHERE workflow_uuid = 'theirs'");
    }

    WorkflowDAO.garbageCollect(ctx, Instant.now(), null, 2);

    assertEquals(0, count("workflow_status"), "another application's rows are collected too");
    assertEquals(0, count("workflow_input"));
  }

  @Test
  void rejectsNonPositiveBatchSize() {
    assertThrows(
        IllegalArgumentException.class,
        () -> WorkflowDAO.garbageCollect(ctx, Instant.now(), null, 0));
  }
}
