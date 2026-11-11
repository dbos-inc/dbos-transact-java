package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.dao.QueuesDAO;
import dev.dbos.transact.database.dao.StreamsDAO;
import dev.dbos.transact.database.signal.SignalKey;
import dev.dbos.transact.database.signal.SignalMap;
import dev.dbos.transact.database.signal.Subscription;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.exceptions.DBOSSystemDatabaseException;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.WorkflowStatusInternalBuilder;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.VersionInfo;
import dev.dbos.transact.workflow.WorkflowDelay;
import dev.dbos.transact.workflow.WorkflowSchedule;
import dev.dbos.transact.workflow.WorkflowState;

import java.io.PrintWriter;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

// Builds Queue values as fixtures or mock stubs, never to register one: the type stays,
// only authoring a Queue by hand is deprecated.
@SuppressWarnings("removal")
public class SystemDatabaseTest {

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

  @Test
  public void listenNotifyDisabledOnBothConstructionPaths() {
    // The data source path used to hardcode the listener on, so a config that turned
    // LISTEN/NOTIFY off was honoured only when DBOS created the pool from a URL.
    try (var fromUrl = SystemDatabase.create(dbosConfig.withUseListenNotify(false), null, null);
        var fromDataSource =
            SystemDatabase.create(
                dbosConfig.withDataSource(dataSource).withUseListenNotify(false), null, null)) {
      assertFalse(fromUrl.hasNotificationListener());
      assertFalse(fromDataSource.hasNotificationListener());
    }
  }

  @Test
  public void listenNotifyEnabledOnTheDataSourcePath() {
    Assumptions.assumeFalse(PgContainer.USE_COCKROACH_DB, "LISTEN/NOTIFY is PostgreSQL-only");
    try (var sysdb =
        SystemDatabase.create(
            dbosConfig.withDataSource(dataSource).withUseListenNotify(true), null, null)) {
      assertTrue(sysdb.hasNotificationListener());
    }
  }

  @Test
  public void testDeleteWorkflows() throws Exception {
    for (var i = 0; i < 5; i++) {
      var wfid = "wfid-%d".formatted(i);
      var status = WorkflowStatusInternalBuilder.create(wfid).build();
      sysdb.initWorkflowStatus(status, 5, null);
    }

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(5, rows.size());

    sysdb.deleteWorkflows(List.of("wfid-1", "wfid-3"), false, null);

    rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(3, rows.size());

    assertTrue(rows.stream().noneMatch(r -> r.workflowId().equals("wfid-1")));
    assertTrue(rows.stream().noneMatch(r -> r.workflowId().equals("wfid-3")));

    assertTrue(rows.stream().anyMatch(r -> r.workflowId().equals("wfid-0")));
    assertTrue(rows.stream().anyMatch(r -> r.workflowId().equals("wfid-2")));
    assertTrue(rows.stream().anyMatch(r -> r.workflowId().equals("wfid-4")));
  }

  @Test
  public void testCreateApplicationVersion() throws Exception {
    sysdb.createApplicationVersion("v1.0.0", null);

    List<VersionInfo> versions = sysdb.listApplicationVersions();
    assertEquals(1, versions.size());
    assertEquals("v1.0.0", versions.get(0).versionName());
    assertNotNull(versions.get(0).versionId());
    assertNotNull(versions.get(0).versionTimestamp());
    assertNotNull(versions.get(0).createdAt());
  }

  @Test
  public void testCreateApplicationVersionIdempotent() throws Exception {
    sysdb.createApplicationVersion("v1.0.0", null);
    sysdb.createApplicationVersion("v1.0.0", null);

    assertEquals(1, sysdb.listApplicationVersions().size());
  }

  @Test
  public void testListApplicationVersionsOrderedByTimestamp() throws Exception {
    Instant t1 = Instant.now();
    sysdb.createApplicationVersion("v1.0.0", null);
    sysdb.updateApplicationVersionTimestamp("v1.0.0", t1, null);

    Instant t2 = t1.plusSeconds(1);
    sysdb.createApplicationVersion("v2.0.0", null);
    sysdb.updateApplicationVersionTimestamp("v2.0.0", t2, null);

    Instant t3 = t1.plusSeconds(2);
    sysdb.createApplicationVersion("v3.0.0", null);
    sysdb.updateApplicationVersionTimestamp("v3.0.0", t3, null);

    List<VersionInfo> versions = sysdb.listApplicationVersions();
    assertEquals(3, versions.size());
    assertEquals("v3.0.0", versions.get(0).versionName());
    assertEquals("v2.0.0", versions.get(1).versionName());
    assertEquals("v1.0.0", versions.get(2).versionName());
  }

  @Test
  public void testGetLatestApplicationVersion() throws Exception {
    Instant t1 = Instant.now();
    sysdb.createApplicationVersion("v1.0.0", null);
    sysdb.updateApplicationVersionTimestamp("v1.0.0", t1, null);

    sysdb.createApplicationVersion("v2.0.0", null);
    sysdb.updateApplicationVersionTimestamp("v2.0.0", t1.plusSeconds(1), null);

    VersionInfo latest = sysdb.getLatestApplicationVersion();
    assertEquals("v2.0.0", latest.versionName());
  }

  @Test
  public void testGetLatestApplicationVersionThrowsWhenEmpty() {
    assertThrows(RuntimeException.class, () -> sysdb.getLatestApplicationVersion());
  }

  @Test
  public void testDeleteWorkflowsList() throws Exception {
    for (var i = 0; i < 5; i++) {
      var wfid = "wfid-%d".formatted(i);
      var status = WorkflowStatusInternalBuilder.create(wfid).build();
      sysdb.initWorkflowStatus(status, 5, null);
    }

    sysdb.deleteWorkflows(List.of("wfid-0", "wfid-2", "wfid-4"), false, null);

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(2, rows.size());
    assertTrue(rows.stream().anyMatch(r -> r.workflowId().equals("wfid-1")));
    assertTrue(rows.stream().anyMatch(r -> r.workflowId().equals("wfid-3")));
  }

  @Test
  public void testCancelWorkflows() throws Exception {
    // Create workflows in different states
    for (var wfid : List.of("wf-pending-1", "wf-pending-2", "wf-pending-3")) {
      sysdb.initWorkflowStatus(WorkflowStatusInternalBuilder.create(wfid).build(), 5, null);
    }
    sysdb.initWorkflowStatus(WorkflowStatusInternalBuilder.create("wf-success").build(), 5, null);
    sysdb.initWorkflowStatus(WorkflowStatusInternalBuilder.create("wf-error").build(), 5, null);
    DBUtils.setWorkflowState(dataSource, "wf-success", WorkflowState.SUCCESS.name());
    DBUtils.setWorkflowState(dataSource, "wf-error", WorkflowState.ERROR.name());

    // Record time before cancel so we can assert updated_at advances
    long beforeCancel = System.currentTimeMillis();

    // Cancel all five IDs in one call
    sysdb.cancelWorkflows(
        List.of("wf-pending-1", "wf-pending-2", "wf-pending-3", "wf-success", "wf-error"),
        false,
        null);

    // PENDING ones become CANCELLED, and updated_at is refreshed
    for (var wfid : List.of("wf-pending-1", "wf-pending-2", "wf-pending-3")) {
      var row = DBUtils.getWorkflowRow(dataSource, wfid);
      assertNotNull(row);
      assertEquals(WorkflowState.CANCELLED.name(), row.status());
      assertTrue(row.updatedAt() >= beforeCancel, "updated_at should be >= time before cancel");
    }

    // SUCCESS and ERROR are left untouched
    assertEquals(
        WorkflowState.SUCCESS.name(), DBUtils.getWorkflowRow(dataSource, "wf-success").status());
    assertEquals(
        WorkflowState.ERROR.name(), DBUtils.getWorkflowRow(dataSource, "wf-error").status());
  }

  List<String> insertResumableWorkflows() throws Exception {
    // Create workflows in different states
    for (var wfid : List.of("wf-cancelled-1", "wf-cancelled-2")) {
      sysdb.initWorkflowStatus(WorkflowStatusInternalBuilder.create(wfid).build(), 5, null);
      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.CANCELLED.name());
    }
    sysdb.initWorkflowStatus(WorkflowStatusInternalBuilder.create("wf-success").build(), 5, null);
    sysdb.initWorkflowStatus(WorkflowStatusInternalBuilder.create("wf-error").build(), 5, null);
    DBUtils.setWorkflowState(dataSource, "wf-success", WorkflowState.SUCCESS.name());
    DBUtils.setWorkflowState(dataSource, "wf-error", WorkflowState.ERROR.name());

    // Set non-null deadline and completed_at on the cancellable workflows so we can assert they are
    // cleared on resume
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "UPDATE dbos.workflow_status SET workflow_deadline_epoch_ms = ?, completed_at = ? WHERE workflow_uuid = ANY(?)")) {
      long deadline = System.currentTimeMillis() + 60_000;
      long completedAt = System.currentTimeMillis() - 1_000;
      stmt.setLong(1, deadline);
      stmt.setLong(2, completedAt);
      stmt.setArray(
          3, conn.createArrayOf("text", new String[] {"wf-cancelled-1", "wf-cancelled-2"}));
      stmt.executeUpdate();
    }

    return List.of("wf-cancelled-1", "wf-cancelled-2", "wf-success", "wf-error");
  }

  @Test
  public void testResumeWorkflows() throws Exception {

    var workflowIds = insertResumableWorkflows();
    long beforeResume = System.currentTimeMillis();

    // Resume all four IDs in one call
    sysdb.resumeWorkflows(workflowIds, null, null);

    // CANCELLED ones become ENQUEUED; updated_at advances; deadline and completed_at are cleared
    for (var wfid : List.of("wf-cancelled-1", "wf-cancelled-2")) {
      var row = DBUtils.getWorkflowRow(dataSource, wfid);
      assertEquals(WorkflowState.ENQUEUED.name(), row.status());
      assertEquals(Constants.DBOS_INTERNAL_QUEUE, row.queueName());
      assertTrue(row.updatedAt() >= beforeResume, "updated_at should be >= time before resume");
      assertNull(row.deadlineEpochMs(), "workflow_deadline_epoch_ms should be cleared on resume");
      assertNull(row.completedAt(), "completed_at should be cleared on resume");
    }

    // SUCCESS and ERROR are left untouched
    assertEquals(
        WorkflowState.SUCCESS.name(), DBUtils.getWorkflowRow(dataSource, "wf-success").status());
    assertEquals(
        WorkflowState.ERROR.name(), DBUtils.getWorkflowRow(dataSource, "wf-error").status());
  }

  @Test
  public void testResumeWorkflowsCustomQueue() throws Exception {

    var workflowIds = insertResumableWorkflows();
    long beforeResume = System.currentTimeMillis();

    // Resume all four IDs in one call
    sysdb.resumeWorkflows(workflowIds, "customQueue", null);

    // CANCELLED ones become ENQUEUED; updated_at advances; deadline and completed_at are cleared
    for (var wfid : List.of("wf-cancelled-1", "wf-cancelled-2")) {
      var row = DBUtils.getWorkflowRow(dataSource, wfid);
      assertEquals(WorkflowState.ENQUEUED.name(), row.status());
      assertEquals("customQueue", row.queueName());
      assertTrue(row.updatedAt() >= beforeResume, "updated_at should be >= time before resume");
      assertNull(row.deadlineEpochMs(), "workflow_deadline_epoch_ms should be cleared on resume");
      assertNull(row.completedAt(), "completed_at should be cleared on resume");
    }

    // SUCCESS and ERROR are left untouched
    assertEquals(
        WorkflowState.SUCCESS.name(), DBUtils.getWorkflowRow(dataSource, "wf-success").status());
    assertEquals(
        WorkflowState.ERROR.name(), DBUtils.getWorkflowRow(dataSource, "wf-error").status());
  }

  @Test
  public void testCancelWorkflowsNullInList() throws Exception {
    sysdb.initWorkflowStatus(WorkflowStatusInternalBuilder.create("wf-id").build(), 5, null);

    long beforeCancel = System.currentTimeMillis();
    sysdb.cancelWorkflows(Arrays.asList("wf-id", null), false, null);

    var row = DBUtils.getWorkflowRow(dataSource, "wf-id");
    assertEquals(WorkflowState.CANCELLED.name(), row.status());
    assertTrue(row.updatedAt() >= beforeCancel, "updated_at should be >= time before cancel");
  }

  @Test
  public void testCancelWorkflowsWithChildren() throws Exception {
    // Build a 3-level tree: parent -> 3 children -> 2 grandchildren each
    sysdb.initWorkflowStatus(WorkflowStatusInternalBuilder.create("parent").build(), 5, null);

    for (var i = 0; i < 3; i++) {
      var childId = "child-%d".formatted(i);
      sysdb.initWorkflowStatus(
          WorkflowStatusInternalBuilder.create(childId).parentWorkflowId("parent").build(),
          5,
          null);

      for (var j = 0; j < 2; j++) {
        var grandchildId = "grandchild-%d-%d".formatted(i, j);
        sysdb.initWorkflowStatus(
            WorkflowStatusInternalBuilder.create(grandchildId).parentWorkflowId(childId).build(),
            5,
            null);
      }
    }

    // Without cancelChildren=true, only the parent is cancelled
    sysdb.cancelWorkflows(List.of("parent"), false, null);
    assertEquals(
        WorkflowState.CANCELLED.name(), DBUtils.getWorkflowRow(dataSource, "parent").status());
    for (var i = 0; i < 3; i++) {
      assertEquals(
          WorkflowState.PENDING.name(),
          DBUtils.getWorkflowRow(dataSource, "child-%d".formatted(i)).status());
      for (var j = 0; j < 2; j++) {
        assertEquals(
            WorkflowState.PENDING.name(),
            DBUtils.getWorkflowRow(dataSource, "grandchild-%d-%d".formatted(i, j)).status());
      }
    }

    // Reset parent to PENDING
    DBUtils.setWorkflowState(dataSource, "parent", WorkflowState.PENDING.name());

    // With cancelChildren=true, parent + all descendants are cancelled
    sysdb.cancelWorkflows(List.of("parent"), true, null);
    assertEquals(
        WorkflowState.CANCELLED.name(), DBUtils.getWorkflowRow(dataSource, "parent").status());
    for (var i = 0; i < 3; i++) {
      assertEquals(
          WorkflowState.CANCELLED.name(),
          DBUtils.getWorkflowRow(dataSource, "child-%d".formatted(i)).status());
      for (var j = 0; j < 2; j++) {
        assertEquals(
            WorkflowState.CANCELLED.name(),
            DBUtils.getWorkflowRow(dataSource, "grandchild-%d-%d".formatted(i, j)).status());
      }
    }
  }

  @Test
  public void testResumeWorkflowsNullInList() throws Exception {
    sysdb.initWorkflowStatus(WorkflowStatusInternalBuilder.create("wf-id").build(), 5, null);
    DBUtils.setWorkflowState(dataSource, "wf-id", WorkflowState.CANCELLED.name());

    long beforeResume = System.currentTimeMillis();
    sysdb.resumeWorkflows(Arrays.asList("wf-id", null), null, null);

    var row = DBUtils.getWorkflowRow(dataSource, "wf-id");
    assertEquals(WorkflowState.ENQUEUED.name(), row.status());
    assertTrue(row.updatedAt() >= beforeResume, "updated_at should be >= time before resume");
    assertNull(row.deadlineEpochMs(), "workflow_deadline_epoch_ms should be cleared on resume");
  }

  @Test
  public void testDeleteWorkflowsNullInList() throws Exception {
    sysdb.initWorkflowStatus(WorkflowStatusInternalBuilder.create("wf-id").build(), 5, null);

    sysdb.deleteWorkflows(Arrays.asList("wf-id", null), false, null);

    assertNull(DBUtils.getWorkflowRow(dataSource, "wf-id"));
  }

  @Test
  public void testGetChildWorkflows() throws Exception {
    for (var i = 0; i < 5; i++) {
      var wfid = "wfid-%d".formatted(i);
      var status = WorkflowStatusInternalBuilder.create(wfid).build();
      sysdb.initWorkflowStatus(status, 5, null);
    }

    for (var i = 0; i < 5; i++) {
      var wfid = "childwfid-%d".formatted(i);
      sysdb.initWorkflowStatus(
          WorkflowStatusInternalBuilder.create(wfid).parentWorkflowId("wfid-2").build(), 5, null);
    }

    for (var i = 0; i < 5; i++) {
      var wfid = "grandchildwfid-%d".formatted(i);
      sysdb.initWorkflowStatus(
          WorkflowStatusInternalBuilder.create(wfid)
              .parentWorkflowId("childwfid-%d".formatted(i))
              .build(),
          5,
          null);
    }

    var children = sysdb.getWorkflowChildren("wfid-2");
    assertEquals(10, children.size());

    for (var i = 0; i < 5; i++) {
      var child = "childwfid-%d".formatted(i);
      var grandchild = "grandchildwfid-%d".formatted(i);
      assertTrue(children.stream().anyMatch(r -> r.equals(child)));
      assertTrue(children.stream().anyMatch(r -> r.equals(grandchild)));
    }
  }

  @Test
  public void testRetries() throws Exception {
    var wfid = "wfid-1";
    var status =
        WorkflowStatusInternalBuilder.create(wfid)
            .workflowName("wf-name")
            .inputs("wf-inputs")
            .build();

    // Only the queue's claim counts a dispatch, so repeating the status upsert does not.
    var first = sysdb.initWorkflowStatus(status, 5, null);
    assertEquals(WorkflowState.PENDING, first.status());
    assertTrue(first.shouldExecuteOnThisExecutor());
    assertEquals(1, DBUtils.getWorkflowRow(dataSource, wfid).recoveryAttempts());

    for (var i = 0; i < 5; i++) {
      // A caller that is not the row's first writer polls for the outcome instead of running it.
      assertFalse(sysdb.initWorkflowStatus(status, 5, null).shouldExecuteOnThisExecutor());
      var repeated = DBUtils.getWorkflowRow(dataSource, wfid);
      assertEquals(WorkflowState.PENDING.name(), repeated.status());
      assertEquals(1, repeated.recoveryAttempts());
    }

    // The dead-letter transition is its own write, guarded on PENDING and on the attempt count
    // the decision was read from.
    sysdb.deadLetterWorkflows(List.of(wfid), 2);
    var unmoved = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(
        WorkflowState.PENDING.name(),
        unmoved.status(),
        "a row with fewer attempts than the decision saw must be left alone");

    sysdb.deadLetterWorkflows(List.of(wfid), 1);
    var row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertNotNull(row);
    assertEquals(WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED.name(), row.status());
    assertNull(row.queueName());
    assertNull(row.startedAtEpochMs());
  }

  @Test
  public void theClaimCountsTheDispatch() throws Exception {
    // The queue's ENQUEUED -> PENDING claim is the only thing that counts a dispatch now, so it
    // is the only thing bounding recovery: without this increment a workflow whose executor keeps
    // dying is swept back, claimed and lost forever, never reaching the dead-letter threshold.
    var wfid = "wfid-claim-counts";
    var queue = new Queue("claim-count-q");
    var appVersion = "v-claim-counts";
    var status =
        WorkflowStatusInternalBuilder.create(wfid)
            .workflowName("wf-name")
            .inputs("wf-inputs")
            .queueName(queue.name())
            // Pinned, not left null: a null version is only claimable while this worker runs
            // the latest registered one, which depends on what other tests left behind in
            // application_versions. An exact match is claimable either way.
            .appVersion(appVersion)
            .build();

    assertEquals(WorkflowState.ENQUEUED, sysdb.initWorkflowStatus(status, 5, null).status());
    assertEquals(0, DBUtils.getWorkflowRow(dataSource, wfid).recoveryAttempts());

    // Poll the claim, as the queue service does, rather than asserting on one pass. The claim
    // selects FOR UPDATE SKIP LOCKED, and CockroachDB skips a key that still carries a write
    // intent even when the transaction that wrote it has committed
    // (cockroachdb/cockroach#167582). The insert's intents are resolved asynchronously after its
    // commit, so a claim issued right behind it can find nothing. Reading the row back above does
    // not settle that: the read resolves only the key it touches, and the claim's plan may scan an
    // index whose intent is still outstanding. What this test pins is that the claim counts the
    // dispatch, not that it wins the first pass.
    List<String> claimed = List.of();
    long deadline = System.currentTimeMillis() + 5_000;
    while (claimed.isEmpty() && System.currentTimeMillis() < deadline) {
      claimed =
          sysdb
              .startQueuedWorkflows(
                  queue, Constants.DEFAULT_EXECUTORID, appVersion, null, 0, 0, "test-claim")
              .workflowIds();
      if (claimed.isEmpty()) Thread.sleep(100);
    }
    assertEquals(List.of(wfid), claimed);

    var row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(WorkflowState.PENDING.name(), row.status());
    assertEquals(1, row.recoveryAttempts(), "the claim must count this dispatch");
  }

  @Test
  public void aStoredRateLimitNoWriteAcceptsHasADefinedMeaning() throws Exception {
    // A queue is loaded as its row stands, so the dequeue meets limits no write stores. A window
    // of no length counts no earlier starts, so each claim may take up to the max; a max of 0
    // leaves no room whatever the window.
    var appVersion = "v-stored-rate-limits";
    var noWindow =
        new Queue(
            "no-window-q",
            null,
            null,
            false,
            false,
            new Queue.RateLimit(2, Duration.ZERO),
            Queue.DEFAULT_POLLING_INTERVAL,
            null);
    var noMax =
        new Queue(
            "no-max-q",
            null,
            null,
            false,
            false,
            new Queue.RateLimit(0, Duration.ofSeconds(60)),
            Queue.DEFAULT_POLLING_INTERVAL,
            null);
    for (var queue : List.of(noWindow, noMax)) {
      for (int i = 0; i < 3; i++) {
        var status =
            WorkflowStatusInternalBuilder.create(queue.name() + "-" + i)
                .workflowName("wf-name")
                .inputs("wf-inputs")
                .queueName(queue.name())
                .appVersion(appVersion)
                .build();
        assertEquals(WorkflowState.ENQUEUED, sysdb.initWorkflowStatus(status, 5, null).status());
      }
    }

    // Polled, as in theClaimCountsTheDispatch, collecting each claim that takes anything. The
    // first takes the max, and the queue is not stuck: the next takes the rest.
    var claims = new ArrayList<List<String>>();
    int claimed = 0;
    long deadline = System.currentTimeMillis() + 5_000;
    while (claimed < 3 && System.currentTimeMillis() < deadline) {
      var batch =
          sysdb
              .startQueuedWorkflows(
                  noWindow, Constants.DEFAULT_EXECUTORID, appVersion, null, 0, 0, "test-claim")
              .workflowIds();
      if (batch.isEmpty()) {
        Thread.sleep(100);
      } else {
        claims.add(batch);
        claimed += batch.size();
      }
    }
    assertEquals(2, claims.get(0).size(), claims.toString());
    assertEquals(3, claimed, claims.toString());

    assertEquals(
        List.of(),
        sysdb
            .startQueuedWorkflows(
                noMax, Constants.DEFAULT_EXECUTORID, appVersion, null, 0, 0, "test-claim")
            .workflowIds());
  }

  @Test
  public void aStoredMinimumConcurrencyLeavesNoRoom() throws Exception {
    // A concurrency limit of Integer.MIN_VALUE, stored by no write but loaded as it stands, less
    // the pending count must leave no room rather than wrap around to a large one.
    var appVersion = "v-min-concurrency";
    for (boolean perPartition : List.of(false, true)) {
      var name = perPartition ? "min-partition-concurrency-q" : "min-concurrency-q";
      var partition = perPartition ? "p" : null;
      for (int i = 0; i < 2; i++) {
        var status =
            WorkflowStatusInternalBuilder.create(name + "-" + i)
                .workflowName("wf-name")
                .inputs("wf-inputs")
                .queueName(name)
                .queuePartitionKey(partition)
                .appVersion(appVersion)
                .build();
        assertEquals(WorkflowState.ENQUEUED, sysdb.initWorkflowStatus(status, 5, null).status());
      }

      // One is claimed under a limit of 1, so one is PENDING when the stored limit is applied.
      var limitOne = concurrencyLimited(name, perPartition, 1);
      assertEquals(
          1,
          sysdb
              .startQueuedWorkflows(
                  limitOne, Constants.DEFAULT_EXECUTORID, appVersion, partition, 0, 0, "test-claim")
              .workflowIds()
              .size());
      var stored = concurrencyLimited(name, perPartition, Integer.MIN_VALUE);
      assertEquals(
          List.of(),
          sysdb
              .startQueuedWorkflows(
                  stored, Constants.DEFAULT_EXECUTORID, appVersion, partition, 0, 0, "test-claim")
              .workflowIds(),
          name);
    }
  }

  private static Queue concurrencyLimited(String name, boolean perPartition, int limit) {
    return new Queue(
        name,
        perPartition ? null : limit,
        null,
        false,
        false,
        null,
        perPartition ? limit : null,
        null,
        null,
        Queue.DEFAULT_POLLING_INTERVAL,
        null);
  }

  @Test
  public void claimSaturatesAnOverflowingDeadline() throws Exception {
    // The claim adds the timeout to the database's clock. For a timeout within the current epoch
    // milliseconds of the bigint maximum the sum overflows, and since the claim is one UPDATE for
    // the whole batch, the error used to roll back every candidate and block the queue.
    var queue = new Queue("near-max-timeout-q");
    var appVersion = "v-near-max-timeout";

    // Written straight to the table, as another SDK's client could: nothing on this path checks
    // the timeout.
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                """
                  INSERT INTO "dbos".workflow_status
                      (workflow_uuid, status, name, queue_name, application_version,
                       workflow_timeout_ms, recovery_attempts, priority)
                  VALUES ('near-max', 'ENQUEUED', 'wf-name', ?, ?, ?, 0, 0)
                """)) {
      stmt.setString(1, queue.name());
      stmt.setString(2, appVersion);
      stmt.setLong(3, Long.MAX_VALUE - 1000);
      stmt.executeUpdate();
    }

    var ordinary =
        WorkflowStatusInternalBuilder.create("ordinary")
            .workflowName("wf-name")
            .inputs("wf-inputs")
            .queueName(queue.name())
            .appVersion(appVersion)
            .timeout(Duration.ofMillis(60_000))
            .build();
    assertEquals(WorkflowState.ENQUEUED, sysdb.initWorkflowStatus(ordinary, 5, null).status());

    // Polled, as in theClaimCountsTheDispatch.
    var claimed = new ArrayList<String>();
    long deadline = System.currentTimeMillis() + 5_000;
    while (claimed.size() < 2 && System.currentTimeMillis() < deadline) {
      var batch =
          sysdb
              .startQueuedWorkflows(
                  queue, Constants.DEFAULT_EXECUTORID, appVersion, null, 0, 0, "test-claim")
              .workflowIds();
      claimed.addAll(batch);
      if (batch.isEmpty()) Thread.sleep(100);
    }
    assertEquals(Set.of("near-max", "ordinary"), Set.copyOf(claimed));

    var nearMaxRow = DBUtils.getWorkflowRow(dataSource, "near-max");
    assertEquals(WorkflowState.PENDING.name(), nearMaxRow.status());
    assertEquals(Long.MAX_VALUE, nearMaxRow.deadlineEpochMs());

    var ordinaryRow = DBUtils.getWorkflowRow(dataSource, "ordinary");
    assertEquals(ordinaryRow.startedAtEpochMs() + 60_000L, ordinaryRow.deadlineEpochMs());
  }

  @Test
  public void directStartSaturatesAnOverflowingDeadline() throws Exception {
    // A direct start computes the deadline in the insert itself, from the timeout parameter.
    var nearMax =
        WorkflowStatusInternalBuilder.create("near-max-direct")
            .workflowName("wf-name")
            .inputs("wf-inputs")
            .timeout(Duration.ofMillis(Long.MAX_VALUE - 1000))
            .build();
    assertEquals(WorkflowState.PENDING, sysdb.initWorkflowStatus(nearMax, 5, null).status());
    var nearMaxRow = DBUtils.getWorkflowRow(dataSource, "near-max-direct");
    assertEquals(Long.MAX_VALUE - 1000, nearMaxRow.timeoutMs());
    assertEquals(Long.MAX_VALUE, nearMaxRow.deadlineEpochMs());

    // An ordinary timeout still lands on created_at + timeout, and no timeout means no deadline.
    var ordinary =
        WorkflowStatusInternalBuilder.create("ordinary-direct")
            .workflowName("wf-name")
            .inputs("wf-inputs")
            .timeout(Duration.ofMillis(60_000))
            .build();
    assertEquals(WorkflowState.PENDING, sysdb.initWorkflowStatus(ordinary, 5, null).status());
    var ordinaryRow = DBUtils.getWorkflowRow(dataSource, "ordinary-direct");
    assertEquals(ordinaryRow.createdAt() + 60_000L, ordinaryRow.deadlineEpochMs());

    var untimed =
        WorkflowStatusInternalBuilder.create("untimed-direct")
            .workflowName("wf-name")
            .inputs("wf-inputs")
            .build();
    assertEquals(WorkflowState.PENDING, sysdb.initWorkflowStatus(untimed, 5, null).status());
    assertNull(DBUtils.getWorkflowRow(dataSource, "untimed-direct").deadlineEpochMs());
  }

  @Test
  public void testDedupeId() throws Exception {
    var wfid = "wfid-1";
    var builder =
        WorkflowStatusInternalBuilder.create(wfid)
            .workflowName("wf-name")
            .inputs("wf-inputs")
            .queueName("queue-name")
            .deduplicationId("dedupe-id");

    var result1 = sysdb.initWorkflowStatus(builder.build(), 5, null);
    assertEquals(WorkflowState.ENQUEUED, result1.status());
    assertNull(result1.deadline());

    var before = DBUtils.getWorkflowRow(dataSource, wfid);
    assertThrows(
        DBOSQueueDuplicatedException.class,
        () -> sysdb.initWorkflowStatus(builder.workflowId("wfid-2").build(), 5, null));
    var after = DBUtils.getWorkflowRow(dataSource, wfid);

    assertTrue(before.equals(after));
  }

  // ── Schedule CRUD ──────────────────────────────────────────────────────────

  private static WorkflowSchedule makeSchedule(String name) {
    return new WorkflowSchedule(
        null,
        name,
        "myWorkflow",
        "com.example.MyClass",
        "0 * * * * *",
        ScheduleStatus.ACTIVE,
        "{}",
        null,
        false,
        null,
        null,
        null);
  }

  @Test
  public void testCreateAndGetSchedule() {
    sysdb.createSchedule(makeSchedule("sched-1"), null);

    var result = sysdb.getSchedule("sched-1");
    assertTrue(result.isPresent());
    var s = result.get();
    assertEquals("sched-1", s.scheduleName());
    assertEquals("myWorkflow", s.workflowName());
    assertEquals("com.example.MyClass", s.className());
    assertEquals("0 * * * * *", s.cron());
    assertEquals(ScheduleStatus.ACTIVE, s.status());
    assertEquals("{}", s.context());
    assertNull(s.lastFiredAt());
    assertFalse(s.automaticBackfill());
    assertNull(s.cronTimezone());
    assertNull(s.queueName());
    assertNotNull(s.id());
  }

  @Test
  public void testGetScheduleNotFound() {
    var result = sysdb.getSchedule("nonexistent");
    assertFalse(result.isPresent());
  }

  @Test
  public void testCreateScheduleDuplicate() {
    sysdb.createSchedule(makeSchedule("sched-dup"), null);
    // SchedulesDAO catches 23505 and raises its own message, so this never reaches dbRetry's
    // terminal path. What dbRetry throws is pinned by dbRetryThrowsTypedOnANonRetryableFailure.
    assertThrows(
        RuntimeException.class, () -> sysdb.createSchedule(makeSchedule("sched-dup"), null));
  }

  @Test
  @DisplayName("dbRetry hands a non-retryable failure back as a typed, chained exception")
  public void dbRetryThrowsTypedOnANonRetryableFailure() throws SQLException {
    // Driven through a real SystemDatabase call rather than by constructing the exception, so
    // that regressing the throw site to a bare RuntimeException fails this test. 42P01 is not a
    // state dbRetry retries, so it takes the terminal path on the first attempt and the test
    // does not sit through any backoff.
    var failure = new SQLException("relation \"dbos.workflow_schedules\" does not exist", "42P01");
    var meta = mock(DatabaseMetaData.class);
    when(meta.getDatabaseProductName()).thenReturn("PostgreSQL");
    var conn = mock(Connection.class);
    when(conn.getMetaData()).thenReturn(meta);
    when(conn.prepareStatement(anyString())).thenThrow(failure);
    when(conn.createStatement()).thenThrow(failure);
    var ds = mock(DataSource.class);
    when(ds.getConnection()).thenReturn(conn);

    try (var db = new SystemDatabase(ds, "dbos", null, false, "app")) {
      var thrown =
          assertThrows(DBOSSystemDatabaseException.class, () -> db.getSchedule("anything"));

      assertSame(failure, thrown.getCause(), "the driver's failure is the cause, one level down");
      assertEquals("42P01", thrown.sqlState());
    }
  }

  @Test
  @Timeout(30)
  @DisplayName("a queue poll fails on its first connection error rather than retrying it")
  public void queuePollDoesNotRetry() throws SQLException {
    // The poll loop is the retry: a failed poll is logged and the next comes a polling interval
    // later. Retried inside dbRetry, a connection error would hold the listener for as long as the
    // database stayed down. 08006 is a state dbRetry retries forever, so a regression hangs here
    // until the timeout rather than passing.
    var failure = new SQLException("connection failure", "08006");
    var meta = mock(DatabaseMetaData.class);
    when(meta.getDatabaseProductName()).thenReturn("PostgreSQL");
    var conn = mock(Connection.class);
    when(conn.getMetaData()).thenReturn(meta);
    when(conn.prepareStatement(anyString())).thenThrow(failure);
    when(conn.createStatement()).thenThrow(failure);
    var ds = mock(DataSource.class);
    when(ds.getConnection()).thenReturn(conn);
    var partitioned =
        new Queue(
            "q",
            null,
            null,
            false,
            false,
            null,
            1,
            null,
            null,
            Queue.DEFAULT_POLLING_INTERVAL,
            null);

    try (var db = new SystemDatabase(ds, "dbos", null, false, "app")) {
      List<Runnable> polls =
          List.of(
              () -> db.getQueuePartitions("q"),
              () -> db.startQueuedWorkflows(new Queue("q"), "exec", "v1", null, 0, 0, "test-claim"),
              () -> db.startQueuedPartitionedWorkflows(partitioned, "exec", "v1", 1, "test-claim"));
      for (var poll : polls) {
        clearInvocations(conn);
        var thrown = assertThrows(DBOSSystemDatabaseException.class, poll::run);
        assertSame(failure, thrown.getCause());
        verify(conn, times(1)).prepareStatement(anyString());
      }
    }
  }

  @Test
  public void testCreateScheduleNullStatusThrows() {
    assertThrows(
        NullPointerException.class,
        () ->
            new WorkflowSchedule(
                null,
                "sched-null-status",
                "myWorkflow",
                "com.example.MyClass",
                "0 * * * * *",
                null,
                "{}",
                null,
                false,
                null,
                null,
                null));
  }

  @Test
  public void testListSchedules() {
    sysdb.createSchedule(makeSchedule("alpha-1"), null);
    sysdb.createSchedule(makeSchedule("alpha-2"), null);
    sysdb.createSchedule(
        new WorkflowSchedule(
            null,
            "beta-1",
            "otherWorkflow",
            null,
            "0 * * * * *",
            ScheduleStatus.ACTIVE,
            "{}",
            null,
            false,
            null,
            null,
            null),
        null);
    sysdb.pauseSchedule("beta-1", null);

    // list all
    var all = sysdb.listSchedules(null, null, null);
    assertEquals(3, all.size());

    // filter by single status
    var active = sysdb.listSchedules(List.of(ScheduleStatus.ACTIVE), null, null);
    assertEquals(2, active.size());

    var paused = sysdb.listSchedules(List.of(ScheduleStatus.PAUSED), null, null);
    assertEquals(1, paused.size());
    assertEquals("beta-1", paused.get(0).scheduleName());

    // filter by multiple statuses
    var both =
        sysdb.listSchedules(List.of(ScheduleStatus.ACTIVE, ScheduleStatus.PAUSED), null, null);
    assertEquals(3, both.size());

    // filter by single workflow name
    var byOne = sysdb.listSchedules(null, List.of("myWorkflow"), null);
    assertEquals(2, byOne.size());

    var byOther = sysdb.listSchedules(null, List.of("otherWorkflow"), null);
    assertEquals(1, byOther.size());

    // filter by multiple workflow names
    var byBoth = sysdb.listSchedules(null, List.of("myWorkflow", "otherWorkflow"), null);
    assertEquals(3, byBoth.size());

    // filter by single prefix
    var byPrefix = sysdb.listSchedules(null, null, List.of("alpha-"));
    assertEquals(2, byPrefix.size());
    assertTrue(byPrefix.stream().allMatch(s -> s.scheduleName().startsWith("alpha-")));

    // filter by multiple prefixes
    var byBothPrefixes = sysdb.listSchedules(null, null, List.of("alpha-", "beta-"));
    assertEquals(3, byBothPrefixes.size());

    var byNone = sysdb.listSchedules(null, null, List.of("nonexistent-"));
    assertEquals(0, byNone.size());

    // combined status + prefix
    var activeAlpha = sysdb.listSchedules(List.of(ScheduleStatus.ACTIVE), null, List.of("alpha-"));
    assertEquals(2, activeAlpha.size());

    var pausedAlpha = sysdb.listSchedules(List.of(ScheduleStatus.PAUSED), null, List.of("alpha-"));
    assertEquals(0, pausedAlpha.size());
  }

  @Test
  public void testListSchedulesWithSqlLikeWildcards() {
    // Test that SQL LIKE wildcards in prefixes are escaped correctly
    sysdb.createSchedule(makeSchedule("test%s Sched"), null);
    sysdb.createSchedule(makeSchedule("test%sother"), null);
    sysdb.createSchedule(makeSchedule("test_schedule"), null);
    sysdb.createSchedule(makeSchedule("test_sched"), null);
    sysdb.createSchedule(makeSchedule("testA_schedB"), null);

    // "test%s" should only match "test%s Sched" and "test%sother" (literal %)
    var byPercent = sysdb.listSchedules(null, null, List.of("test%s"));
    assertEquals(2, byPercent.size());
    assertTrue(byPercent.stream().allMatch(s -> s.scheduleName().startsWith("test%s")));

    // "test_" should only match "test_schedule" and "test_sched" (literal _, not matching "testA_")
    var byUnderscore = sysdb.listSchedules(null, null, List.of("test_"));
    assertEquals(2, byUnderscore.size());
    assertTrue(byUnderscore.stream().allMatch(s -> s.scheduleName().startsWith("test_")));

    // "test" without wildcard matches all (since all start with "test")
    var byExact = sysdb.listSchedules(null, null, List.of("test"));
    assertEquals(5, byExact.size());
  }

  @Test
  public void testPauseAndResumeSchedule() {
    sysdb.createSchedule(makeSchedule("sched-pause"), null);

    sysdb.pauseSchedule("sched-pause", null);
    assertEquals(ScheduleStatus.PAUSED, sysdb.getSchedule("sched-pause").get().status());

    sysdb.resumeSchedule("sched-pause", null);
    assertEquals(ScheduleStatus.ACTIVE, sysdb.getSchedule("sched-pause").get().status());
  }

  @Test
  public void testUpdateLastFiredAt() {
    sysdb.createSchedule(makeSchedule("sched-fired"), null);
    assertNull(sysdb.getSchedule("sched-fired").get().lastFiredAt());

    sysdb.updateScheduleLastFiredAt("sched-fired", Instant.parse("2026-03-26T10:00:00Z"));
    assertEquals(
        Instant.parse("2026-03-26T10:00:00Z"),
        sysdb.getSchedule("sched-fired").get().lastFiredAt());
  }

  @Test
  public void testDeleteSchedule() {
    sysdb.createSchedule(makeSchedule("sched-del-1"), null);
    sysdb.createSchedule(makeSchedule("sched-del-2"), null);
    assertEquals(2, sysdb.listSchedules(null, null, null).size());

    sysdb.deleteSchedule("sched-del-1", null);
    assertFalse(sysdb.getSchedule("sched-del-1").isPresent());
    assertEquals(1, sysdb.listSchedules(null, null, null).size());

    sysdb.deleteSchedule("sched-del-2", null);
    assertFalse(sysdb.getSchedule("sched-del-2").isPresent());
    assertEquals(0, sysdb.listSchedules(null, null, null).size());
  }

  @Test
  public void testCreateScheduleWithAllFields() {
    var schedule =
        new WorkflowSchedule(
            "my-id-123",
            "sched-full",
            "fullWorkflow",
            "com.example.Full",
            "*/5 * * * * *",
            ScheduleStatus.ACTIVE,
            "{\"key\":\"val\"}",
            Instant.parse("2026-03-01T00:00:00Z"),
            true,
            ZoneId.of("America/New_York"),
            "my-queue",
            null);
    sysdb.createSchedule(schedule, null);

    var s = sysdb.getSchedule("sched-full").get();
    assertEquals("my-id-123", s.id());
    assertEquals("sched-full", s.scheduleName());
    assertEquals("fullWorkflow", s.workflowName());
    assertEquals("com.example.Full", s.className());
    assertEquals("*/5 * * * * *", s.cron());
    assertEquals("{\"key\":\"val\"}", s.context());
    assertEquals(Instant.parse("2026-03-01T00:00:00Z"), s.lastFiredAt());
    assertTrue(s.automaticBackfill());
    assertEquals(ZoneId.of("America/New_York"), s.cronTimezone());
    assertEquals("my-queue", s.queueName());
  }

  @Test
  public void testWorkflowStatusAuthenticationFields() throws Exception {
    var workflowId = "test-auth-workflow";
    var authenticatedUser = "user@example.com";
    var assumedRole = "admin";
    var authenticatedRoles = new String[] {"admin", "operator", "viewer"};

    // Create workflow status with authentication fields
    var status =
        WorkflowStatusInternalBuilder.create(workflowId)
            .workflowName("TestWorkflow")
            .className("com.example.TestWorkflow")
            .authenticatedUser(authenticatedUser)
            .assumedRole(assumedRole)
            .authenticatedRoles(authenticatedRoles)
            .build();

    // Insert into database
    sysdb.initWorkflowStatus(status, null, null);

    // Retrieve via SystemDatabase API and validate object mapping
    var retrievedStatus = sysdb.getWorkflowStatus(workflowId);
    assertNotNull(retrievedStatus);
    assertEquals(authenticatedUser, retrievedStatus.authenticatedUser());
    assertEquals(assumedRole, retrievedStatus.assumedRole());
    assertEquals(List.of(authenticatedRoles), retrievedStatus.authenticatedRoles());

    // Validate raw database values using DBUtils
    var rawRow = DBUtils.getWorkflowRow(dataSource, workflowId);
    assertNotNull(rawRow);
    assertEquals(authenticatedUser, rawRow.authenticatedUser());
    assertEquals(assumedRole, rawRow.assumedRole());

    // Verify the authenticated_roles are stored as JSON in the database
    assertEquals("[\"admin\",\"operator\",\"viewer\"]", rawRow.authenticatedRoles());

    // Verify other fields are correctly stored
    assertEquals(workflowId, rawRow.workflowId());
    assertEquals(WorkflowState.PENDING.name(), rawRow.status());
    assertEquals("TestWorkflow", rawRow.workflowName());
    assertEquals("com.example.TestWorkflow", rawRow.className());
  }

  @Test
  public void testWorkflowStatusAuthenticationFieldsWithNulls() throws Exception {
    var workflowId = "test-auth-null-workflow";

    // Create workflow status with null authentication fields
    var status =
        WorkflowStatusInternalBuilder.create(workflowId)
            .workflowName("TestNullAuthWorkflow")
            .className("com.example.TestNullAuthWorkflow")
            .authenticatedUser(null)
            .assumedRole(null)
            .authenticatedRoles((String[]) null)
            .build();

    // Insert into database
    sysdb.initWorkflowStatus(status, null, null);

    // Retrieve via SystemDatabase API and validate null handling
    var retrievedStatus = sysdb.getWorkflowStatus(workflowId);
    assertNotNull(retrievedStatus);
    assertNull(retrievedStatus.authenticatedUser());
    assertNull(retrievedStatus.assumedRole());
    assertNull(retrievedStatus.authenticatedRoles());

    // Validate raw database values are null
    var rawRow = DBUtils.getWorkflowRow(dataSource, workflowId);
    assertNotNull(rawRow);
    assertNull(rawRow.authenticatedUser());
    assertNull(rawRow.assumedRole());
    assertNull(rawRow.authenticatedRoles());
  }

  @Test
  public void testWorkflowStatusEmptyAuthenticatedRoles() throws Exception {
    var workflowId = "test-auth-empty-roles-workflow";
    var authenticatedUser = "user@example.com";
    var assumedRole = "basic";
    var authenticatedRoles = new String[0]; // Empty array

    // Create workflow status with empty authenticated roles
    var status =
        WorkflowStatusInternalBuilder.create(workflowId)
            .workflowName("TestEmptyRolesWorkflow")
            .className("com.example.TestEmptyRolesWorkflow")
            .authenticatedUser(authenticatedUser)
            .assumedRole(assumedRole)
            .authenticatedRoles(authenticatedRoles)
            .build();

    // Insert into database
    sysdb.initWorkflowStatus(status, null, null);

    // Retrieve via SystemDatabase API and validate empty list handling
    var retrievedStatus = sysdb.getWorkflowStatus(workflowId);
    assertNotNull(retrievedStatus);
    assertEquals(authenticatedUser, retrievedStatus.authenticatedUser());
    assertEquals(assumedRole, retrievedStatus.assumedRole());
    assertEquals(0, retrievedStatus.authenticatedRoles().size());

    // Validate raw database values
    var rawRow = DBUtils.getWorkflowRow(dataSource, workflowId);
    assertNotNull(rawRow);
    assertEquals(authenticatedUser, rawRow.authenticatedUser());
    assertEquals(assumedRole, rawRow.assumedRole());
    assertEquals("[]", rawRow.authenticatedRoles()); // Empty JSON array
  }

  @Test
  public void testForkWorkflowWithAuthenticationFields() throws Exception {
    var originalWorkflowId = "test-fork-original-workflow";
    var authenticatedUser = "user@example.com";
    var assumedRole = "admin";
    var authenticatedRoles = new String[] {"admin", "operator", "viewer"};

    // Create original workflow status with authentication fields
    var originalStatus =
        WorkflowStatusInternalBuilder.create(originalWorkflowId)
            .workflowName("OriginalTestWorkflow")
            .className("com.example.OriginalTestWorkflow")
            .authenticatedUser(authenticatedUser)
            .assumedRole(assumedRole)
            .authenticatedRoles(authenticatedRoles)
            .build();

    // Insert original workflow into database
    sysdb.initWorkflowStatus(originalStatus, null, null);

    // Verify original workflow has correct authentication fields
    var originalRetrieved = sysdb.getWorkflowStatus(originalWorkflowId);
    assertNotNull(originalRetrieved);
    assertEquals(authenticatedUser, originalRetrieved.authenticatedUser());
    assertEquals(assumedRole, originalRetrieved.assumedRole());
    assertEquals(List.of(authenticatedRoles), originalRetrieved.authenticatedRoles());

    // Fork the workflow
    var forkOptions = new ForkOptions().withApplicationVersion("1.0.0");
    var forkedWorkflowId = sysdb.forkWorkflow(originalWorkflowId, 0, forkOptions, null, null);
    assertNotNull(forkedWorkflowId);
    assertNotEquals(originalWorkflowId, forkedWorkflowId);

    // Retrieve forked workflow and validate authentication fields are copied
    var forkedStatus = sysdb.getWorkflowStatus(forkedWorkflowId);
    assertNotNull(forkedStatus);
    assertEquals(authenticatedUser, forkedStatus.authenticatedUser());
    assertEquals(assumedRole, forkedStatus.assumedRole());
    assertEquals(List.of(authenticatedRoles), forkedStatus.authenticatedRoles());

    // Verify other forked workflow properties
    assertEquals("OriginalTestWorkflow", forkedStatus.workflowName());
    assertEquals("com.example.OriginalTestWorkflow", forkedStatus.className());
    assertEquals(
        WorkflowState.ENQUEUED, forkedStatus.status()); // Forked workflows start as ENQUEUED
    assertEquals(originalWorkflowId, forkedStatus.forkedFrom());

    // Validate raw database values for both workflows
    var originalRawRow = DBUtils.getWorkflowRow(dataSource, originalWorkflowId);
    var forkedRawRow = DBUtils.getWorkflowRow(dataSource, forkedWorkflowId);

    assertNotNull(originalRawRow);
    assertNotNull(forkedRawRow);

    // Authentication fields should be identical in raw DB
    assertEquals(originalRawRow.authenticatedUser(), forkedRawRow.authenticatedUser());
    assertEquals(originalRawRow.assumedRole(), forkedRawRow.assumedRole());
    assertEquals(originalRawRow.authenticatedRoles(), forkedRawRow.authenticatedRoles());
    assertEquals("[\"admin\",\"operator\",\"viewer\"]", forkedRawRow.authenticatedRoles());

    // Verify forked_from field is set correctly
    assertNull(originalRawRow.forkedFrom());
    assertEquals(originalWorkflowId, forkedRawRow.forkedFrom());
  }

  @Test
  public void testForkWorkflowWithNullAuthenticationFields() throws Exception {
    var originalWorkflowId = "test-fork-null-auth-workflow";

    // Create original workflow status with null authentication fields
    var originalStatus =
        WorkflowStatusInternalBuilder.create(originalWorkflowId)
            .workflowName("NullAuthTestWorkflow")
            .className("com.example.NullAuthTestWorkflow")
            .authenticatedUser(null)
            .assumedRole(null)
            .authenticatedRoles((String[]) null)
            .build();

    // Insert original workflow into database
    sysdb.initWorkflowStatus(originalStatus, null, null);

    // Fork the workflow
    var forkOptions = new ForkOptions().withApplicationVersion("1.0.0");
    var forkedWorkflowId = sysdb.forkWorkflow(originalWorkflowId, 0, forkOptions, null, null);
    assertNotNull(forkedWorkflowId);

    // Retrieve forked workflow and validate null authentication fields are preserved
    var forkedStatus = sysdb.getWorkflowStatus(forkedWorkflowId);
    assertNotNull(forkedStatus);
    assertNull(forkedStatus.authenticatedUser());
    assertNull(forkedStatus.assumedRole());
    assertNull(forkedStatus.authenticatedRoles());

    // Validate raw database values
    var originalRawRow = DBUtils.getWorkflowRow(dataSource, originalWorkflowId);
    var forkedRawRow = DBUtils.getWorkflowRow(dataSource, forkedWorkflowId);

    assertNotNull(originalRawRow);
    assertNotNull(forkedRawRow);

    // Null authentication fields should be preserved
    assertNull(originalRawRow.authenticatedUser());
    assertNull(forkedRawRow.authenticatedUser());
    assertNull(originalRawRow.assumedRole());
    assertNull(forkedRawRow.assumedRole());
    assertNull(originalRawRow.authenticatedRoles());
    assertNull(forkedRawRow.authenticatedRoles());
  }

  @Test
  public void testForkWorkflowWithEmptyAuthenticatedRoles() throws Exception {
    var originalWorkflowId = "test-fork-empty-auth-roles-workflow";
    var authenticatedUser = "user@example.com";
    var assumedRole = "basic";
    var authenticatedRoles = new String[0]; // Empty array

    // Create original workflow status with empty authenticated roles
    var originalStatus =
        WorkflowStatusInternalBuilder.create(originalWorkflowId)
            .workflowName("EmptyAuthRolesTestWorkflow")
            .className("com.example.EmptyAuthRolesTestWorkflow")
            .authenticatedUser(authenticatedUser)
            .assumedRole(assumedRole)
            .authenticatedRoles(authenticatedRoles)
            .build();

    // Insert original workflow into database
    sysdb.initWorkflowStatus(originalStatus, null, null);

    // Verify original workflow has correct authentication fields including empty roles
    var originalRetrieved = sysdb.getWorkflowStatus(originalWorkflowId);
    assertNotNull(originalRetrieved);
    assertEquals(authenticatedUser, originalRetrieved.authenticatedUser());
    assertEquals(assumedRole, originalRetrieved.assumedRole());
    assertEquals(0, originalRetrieved.authenticatedRoles().size());

    // Fork the workflow
    var forkOptions = new ForkOptions().withApplicationVersion("1.0.0");
    var forkedWorkflowId = sysdb.forkWorkflow(originalWorkflowId, 0, forkOptions, null, null);
    assertNotNull(forkedWorkflowId);
    assertNotEquals(originalWorkflowId, forkedWorkflowId);

    // Retrieve forked workflow and validate empty authentication roles are preserved
    var forkedStatus = sysdb.getWorkflowStatus(forkedWorkflowId);
    assertNotNull(forkedStatus);
    assertEquals(authenticatedUser, forkedStatus.authenticatedUser());
    assertEquals(assumedRole, forkedStatus.assumedRole());
    assertEquals(0, forkedStatus.authenticatedRoles().size());

    // Verify other forked workflow properties
    assertEquals("EmptyAuthRolesTestWorkflow", forkedStatus.workflowName());
    assertEquals("com.example.EmptyAuthRolesTestWorkflow", forkedStatus.className());
    assertEquals(WorkflowState.ENQUEUED, forkedStatus.status());
    assertEquals(originalWorkflowId, forkedStatus.forkedFrom());

    // Validate raw database values for both workflows
    var originalRawRow = DBUtils.getWorkflowRow(dataSource, originalWorkflowId);
    var forkedRawRow = DBUtils.getWorkflowRow(dataSource, forkedWorkflowId);

    assertNotNull(originalRawRow);
    assertNotNull(forkedRawRow);

    // Empty authentication roles should be stored as empty JSON array and preserved in fork
    assertEquals(authenticatedUser, originalRawRow.authenticatedUser());
    assertEquals(authenticatedUser, forkedRawRow.authenticatedUser());
    assertEquals(assumedRole, originalRawRow.assumedRole());
    assertEquals(assumedRole, forkedRawRow.assumedRole());
    assertEquals("[]", originalRawRow.authenticatedRoles()); // Empty JSON array
    assertEquals("[]", forkedRawRow.authenticatedRoles()); // Empty JSON array preserved in fork

    // Verify forked_from field is set correctly
    assertNull(originalRawRow.forkedFrom());
    assertEquals(originalWorkflowId, forkedRawRow.forkedFrom());
  }

  @Test
  public void testWriteStreamAndReadStream() throws Exception {
    String workflowId = "stream-wf-1";
    var status = WorkflowStatusInternalBuilder.create(workflowId).build();
    sysdb.initWorkflowStatus(status, 5, null);
    int functionId = 1;

    sysdb.writeStreamFromStep(
        workflowId,
        functionId,
        "key1",
        "value1",
        "portable_json",
        DBTestAccess.takeOwnership(sysdb, workflowId));
    sysdb.writeStreamFromStep(
        workflowId,
        functionId,
        "key1",
        "value2",
        "portable_json",
        DBTestAccess.takeOwnership(sysdb, workflowId));

    Object result = sysdb.readStream(workflowId, "key1", 0);
    assertEquals("value1", result);

    result = sysdb.readStream(workflowId, "key1", 1);
    assertEquals("value2", result);
  }

  @Test
  public void testWriteStreamFromWorkflow() throws Exception {
    String workflowId = "stream-wf-2";
    var status = WorkflowStatusInternalBuilder.create(workflowId).build();
    sysdb.initWorkflowStatus(status, 5, null);
    int functionId = 1;

    sysdb.writeStreamFromWorkflow(
        workflowId,
        functionId,
        "key1",
        "value1",
        "portable_json",
        DBTestAccess.takeOwnership(sysdb, workflowId));

    Object result = sysdb.readStream(workflowId, "key1", 0);
    assertEquals("value1", result);
  }

  @Test
  public void testCloseStream() throws Exception {
    String workflowId = "stream-wf-3";
    var status = WorkflowStatusInternalBuilder.create(workflowId).build();
    sysdb.initWorkflowStatus(status, 5, null);

    sysdb.writeStreamFromWorkflow(
        workflowId,
        1,
        "key1",
        "value1",
        "portable_json",
        DBTestAccess.takeOwnership(sysdb, workflowId));
    sysdb.closeStream(workflowId, 2, "key1", DBTestAccess.takeOwnership(sysdb, workflowId));

    assertEquals(SystemDatabase.END_OF_STREAM, sysdb.readStream(workflowId, "key1", 1));
  }

  @Test
  public void testReadStreamDrainsValueCommittedBeforeProducerInactive() throws Exception {
    // A reader must not drop a value the producer commits between the reader's stream read and its
    // status check. Once the producer is terminal every one of its writes is committed, so the
    // reader makes one more pass before ending the stream.
    String workflowId = "stream-wf-drain";
    var status = WorkflowStatusInternalBuilder.create(workflowId).build();
    sysdb.initWorkflowStatus(status, 5, null);
    sysdb.recordWorkflowOutput(workflowId, null, DBTestAccess.takeOwnership(sysdb, workflowId));

    var ctx =
        new DbContext(dataSource, "dbos", null, () -> false, null, null, new PollingLimiter(0));
    var signals = new SignalMap();
    var passes = new AtomicInteger();

    // readStream subscribes once per pass, so the second call is exactly the window between the
    // first read finding nothing and the drain pass. Commit the producer's last value there, as a
    // producer racing its own completion would.
    Function<SignalKey, Subscription> subscribeRacingAWrite =
        key -> {
          if (passes.incrementAndGet() == 2) {
            sysdb.writeStreamFromWorkflow(
                workflowId,
                1,
                "key1",
                "late",
                "portable_json",
                DBTestAccess.takeOwnership(sysdb, workflowId));
          }
          return signals.subscribe(key.toString());
        };

    var value =
        StreamsDAO.readStream(
            ctx, workflowId, "key1", 0, Duration.ofSeconds(1), subscribeRacingAWrite);

    assertEquals("late", value);
    assertEquals(2, passes.get(), "the drain pass is the second and last read");
  }

  @Test
  public void testReadStreamEndsWhenTheDrainPassFindsNothing() throws Exception {
    String workflowId = "stream-wf-drain-empty";
    var status = WorkflowStatusInternalBuilder.create(workflowId).build();
    sysdb.initWorkflowStatus(status, 5, null);
    sysdb.recordWorkflowOutput(workflowId, null, DBTestAccess.takeOwnership(sysdb, workflowId));

    assertEquals(SystemDatabase.END_OF_STREAM, sysdb.readStream(workflowId, "key1", 0));
  }

  @Test
  public void testReadStreamNotFound() throws Exception {
    String workflowId = "stream-wf-5";
    var status = WorkflowStatusInternalBuilder.create(workflowId).build();
    sysdb.initWorkflowStatus(status, 5, null);

    DBUtils.setWorkflowState(dataSource, workflowId, WorkflowState.SUCCESS.name());
    assertEquals(SystemDatabase.END_OF_STREAM, sysdb.readStream(workflowId, "key", 0));
  }

  @Test
  public void testInsertWorkflowStatusValidation() throws Exception {
    // Test null workflowId
    assertThrows(
        NullPointerException.class,
        () -> {
          var status = WorkflowStatusInternalBuilder.create(null).build();
          sysdb.initWorkflowStatus(status, null, null);
        });
  }

  @Test
  public void testInsertWorkflowStatusEmptyStringValidation() throws Exception {
    // Test empty workflowName
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          var status = WorkflowStatusInternalBuilder.create("test-wf-1").workflowName("").build();
          sysdb.initWorkflowStatus(status, null, null);
        });

    // Test empty className
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          var status = WorkflowStatusInternalBuilder.create("test-wf-2").className("").build();
          sysdb.initWorkflowStatus(status, null, null);
        });

    // Test empty instanceName
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          var status = WorkflowStatusInternalBuilder.create("test-wf-3").instanceName("").build();
          sysdb.initWorkflowStatus(status, null, null);
        });

    // Test empty queueName
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          var status = WorkflowStatusInternalBuilder.create("test-wf-4").queueName("").build();
          sysdb.initWorkflowStatus(status, null, null);
        });

    // Test empty deduplicationId
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          var status =
              WorkflowStatusInternalBuilder.create("test-wf-5").deduplicationId("").build();
          sysdb.initWorkflowStatus(status, null, null);
        });

    // Test empty queuePartitionKey
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          var status =
              WorkflowStatusInternalBuilder.create("test-wf-6").queuePartitionKey("").build();
          sysdb.initWorkflowStatus(status, null, null);
        });
  }

  @Test
  public void testInsertWorkflowStatusValidNullValues() throws Exception {
    // Test that null values (except for required fields) are allowed
    var status =
        WorkflowStatusInternalBuilder.create("test-valid-nulls")
            .workflowName(null)
            .className(null)
            .instanceName(null)
            .queueName(null)
            .deduplicationId(null)
            .queuePartitionKey(null)
            .build();

    // This should not throw an exception
    var result = sysdb.initWorkflowStatus(status, null, null);
    assertEquals(WorkflowState.PENDING, result.status());
  }

  @Test
  public void testInsertWorkflowStatusValidNonEmptyValues() throws Exception {
    // Test that non-empty values are allowed
    var status =
        WorkflowStatusInternalBuilder.create("test-valid-values")
            .workflowName("TestWorkflow")
            .className("com.example.TestWorkflow")
            .instanceName("test-instance")
            .queueName("test-queue")
            .deduplicationId("dedupe-123")
            .queuePartitionKey("partition-key")
            .build();

    // This should not throw an exception
    var result = sysdb.initWorkflowStatus(status, null, null);
    assertEquals(WorkflowState.ENQUEUED, result.status());

    // Verify the values were stored correctly
    var retrievedStatus = sysdb.getWorkflowStatus("test-valid-values");
    assertEquals("TestWorkflow", retrievedStatus.workflowName());
    assertEquals("com.example.TestWorkflow", retrievedStatus.className());
    assertEquals("test-instance", retrievedStatus.instanceName());
  }

  // ── Postgres validation ──────────────────────────────────────────────────

  @Test
  public void testNonPostgresDataSourceThrows() throws SQLException {
    var ds = mockDataSource("MySQL");
    var ex =
        assertThrows(
            IllegalStateException.class, () -> new SystemDatabase(ds, "dbos", null, true, null));
    assertTrue(ex.getMessage().contains("PostgreSQL"));
    assertTrue(ex.getMessage().contains("MySQL"));
  }

  @Test
  public void testNonPostgresDataSourceViaCreateThrows() throws SQLException {
    var ds = mockDataSource("SQLite");
    var config = DBOSConfig.defaults("test-app").withDataSource(ds);
    var ex =
        assertThrows(
            IllegalStateException.class,
            () -> SystemDatabase.create(config, null, config.appName()));
    assertTrue(ex.getMessage().contains("PostgreSQL"));
    assertTrue(ex.getMessage().contains("SQLite"));
  }

  @Test
  public void testSqliteDataSourceThrows() {
    var ds = new org.sqlite.SQLiteDataSource();
    ds.setUrl("jdbc:sqlite::memory:");
    var ex =
        assertThrows(
            IllegalStateException.class, () -> new SystemDatabase(ds, "dbos", null, true, null));
    assertTrue(ex.getMessage().contains("PostgreSQL"));
    assertTrue(ex.getMessage().contains("SQLite"));
  }

  @Test
  public void testPostgresDataSourceIsAccepted() {
    // The @BeforeEach sysdb was created successfully from a real Postgres datasource —
    // just confirm it is non-null as evidence that validation passed.
    assertNotNull(sysdb);
  }

  private static DataSource mockDataSource(String productName) throws SQLException {
    var meta = mock(DatabaseMetaData.class);
    when(meta.getDatabaseProductName()).thenReturn(productName);
    var conn = mock(Connection.class);
    when(conn.getMetaData()).thenReturn(meta);
    var ds = mock(DataSource.class);
    when(ds.getConnection()).thenReturn(conn);
    return ds;
  }

  // ── Workflow state based on queue/delay ──────────────────────────────────

  @Test
  public void testInitWorkflowStatusStateNoQueue() throws Exception {
    var wfid = "wf-state-no-queue";
    var result =
        sysdb.initWorkflowStatus(WorkflowStatusInternalBuilder.create(wfid).build(), 5, null);
    assertEquals(WorkflowState.PENDING, result.status());
    var row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(WorkflowState.PENDING.name(), row.status());
    assertNull(row.delayUntilEpochMs());
  }

  @Test
  public void testInitWorkflowStatusStateQueueNoDelay() throws Exception {
    var wfid = "wf-state-queue-no-delay";
    var result =
        sysdb.initWorkflowStatus(
            WorkflowStatusInternalBuilder.create(wfid).queueName("test-queue").build(), 5, null);
    assertEquals(WorkflowState.ENQUEUED, result.status());
    var row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(WorkflowState.ENQUEUED.name(), row.status());
    assertNull(row.delayUntilEpochMs());
  }

  @Test
  public void testInitWorkflowStatusStateQueueWithDelay() throws Exception {
    var wfid = "wf-state-queue-delay";
    var delay = Duration.ofSeconds(60);
    long before = System.currentTimeMillis();
    var result =
        sysdb.initWorkflowStatus(
            WorkflowStatusInternalBuilder.create(wfid).queueName("test-queue").delay(delay).build(),
            5,
            null);
    assertEquals(WorkflowState.DELAYED, result.status());

    var row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(WorkflowState.DELAYED.name(), row.status());
    // delay_until_epoch_ms should be an absolute epoch timestamp ~60 seconds from now
    assertNotNull(row.delayUntilEpochMs());
    assertTrue(row.delayUntilEpochMs() >= before + delay.toMillis() - 1_000);
    assertTrue(row.delayUntilEpochMs() <= before + delay.toMillis() + 1_000);
  }

  // ── Workflow Delay ────────────────────────────────────────────────────────

  @Test
  public void testSetWorkflowDelayWithDuration() throws Exception {
    var wfid = "wf-delay-duration";
    sysdb.initWorkflowStatus(
        WorkflowStatusInternalBuilder.create(wfid)
            .queueName("test-queue")
            .delay(Duration.ofSeconds(60))
            .build(),
        5,
        null);

    long before = System.currentTimeMillis();
    sysdb.setWorkflowDelay(wfid, new WorkflowDelay.Delay(Duration.ofSeconds(30)), null);

    var row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertNotNull(row.delayUntilEpochMs());
    assertTrue(row.delayUntilEpochMs() >= before + 29_000);
    assertTrue(row.delayUntilEpochMs() <= before + 31_000);
  }

  @Test
  public void testSetWorkflowDelayWithInstant() throws Exception {
    var wfid = "wf-delay-instant";
    sysdb.initWorkflowStatus(
        WorkflowStatusInternalBuilder.create(wfid)
            .queueName("test-queue")
            .delay(Duration.ofSeconds(60))
            .build(),
        5,
        null);

    var targetInstant = Instant.now().plusSeconds(120);
    sysdb.setWorkflowDelay(wfid, new WorkflowDelay.DelayUntil(targetInstant), null);

    var row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(targetInstant.toEpochMilli(), row.delayUntilEpochMs());
  }

  @Test
  public void testSetWorkflowDelayIgnoresNonDelayedWorkflow() throws Exception {
    var targetDelay = new WorkflowDelay.DelayUntil(Instant.now().plusSeconds(60));

    for (var state :
        List.of(
            WorkflowState.PENDING,
            WorkflowState.ENQUEUED,
            WorkflowState.SUCCESS,
            WorkflowState.ERROR,
            WorkflowState.CANCELLED)) {
      var wfid = "wf-delay-non-delayed-" + state.name().toLowerCase();
      sysdb.initWorkflowStatus(
          WorkflowStatusInternalBuilder.create(wfid).queueName("test-queue").build(), 5, null);
      DBUtils.setWorkflowState(dataSource, wfid, state.name());

      sysdb.setWorkflowDelay(wfid, targetDelay, null);

      var row = DBUtils.getWorkflowRow(dataSource, wfid);
      assertNull(row.delayUntilEpochMs(), "Expected no delay for status " + state);
    }
  }

  @Test
  public void testTransitionDelayedWorkflowsPastDeadline() throws Exception {
    var wfid = "wf-transition-past";
    sysdb.initWorkflowStatus(
        WorkflowStatusInternalBuilder.create(wfid)
            .queueName("test-queue")
            .delay(Duration.ofSeconds(60))
            .build(),
        5,
        null);

    sysdb.setWorkflowDelay(wfid, new WorkflowDelay.DelayUntil(Instant.now().minusSeconds(5)), null);
    sysdb.transitionDelayedWorkflows();

    assertEquals(WorkflowState.ENQUEUED.name(), DBUtils.getWorkflowRow(dataSource, wfid).status());
  }

  @Test
  public void testTransitionDelayedWorkflowsFutureDeadlineNotTransitioned() throws Exception {
    var wfid = "wf-transition-future";
    sysdb.initWorkflowStatus(
        WorkflowStatusInternalBuilder.create(wfid)
            .queueName("test-queue")
            .delay(Duration.ofSeconds(60))
            .build(),
        5,
        null);

    sysdb.setWorkflowDelay(wfid, new WorkflowDelay.DelayUntil(Instant.now().plusSeconds(60)), null);
    sysdb.transitionDelayedWorkflows();

    assertEquals(WorkflowState.DELAYED.name(), DBUtils.getWorkflowRow(dataSource, wfid).status());
  }

  @Test
  public void testTransitionDelayedWorkflowsMixed() throws Exception {
    var pastWfid = "wf-transition-mixed-past";
    var futureWfid = "wf-transition-mixed-future";

    for (var wfid : List.of(pastWfid, futureWfid)) {
      sysdb.initWorkflowStatus(
          WorkflowStatusInternalBuilder.create(wfid)
              .queueName("test-queue")
              .delay(Duration.ofSeconds(60))
              .build(),
          5,
          null);
    }

    sysdb.setWorkflowDelay(
        pastWfid, new WorkflowDelay.DelayUntil(Instant.now().minusSeconds(5)), null);
    sysdb.setWorkflowDelay(
        futureWfid, new WorkflowDelay.DelayUntil(Instant.now().plusSeconds(60)), null);

    sysdb.transitionDelayedWorkflows();

    assertEquals(
        WorkflowState.ENQUEUED.name(), DBUtils.getWorkflowRow(dataSource, pastWfid).status());
    assertEquals(
        WorkflowState.DELAYED.name(), DBUtils.getWorkflowRow(dataSource, futureWfid).status());
  }

  // ── insertWorkflowStatus ON CONFLICT behavior ──────────────────────────────

  @Test
  public void testInsertWorkflowStatusConflictPending() throws Exception {
    // PENDING (no queue): a second writer changes nothing at all. It does not own the row, so it
    // is told to poll for the outcome and its upsert is rolled back.
    var wfid = "wf-conflict-pending";
    var first = WorkflowStatusInternalBuilder.create(wfid).executorId("executor-1").build();
    sysdb.initWorkflowStatus(first, 5, null);

    var row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(WorkflowState.PENDING.name(), row.status());
    assertEquals("executor-1", row.executorId());
    assertEquals(1L, row.recoveryAttempts()); // PENDING starts at 1

    var second = WorkflowStatusInternalBuilder.create(wfid).executorId("executor-2").build();
    assertFalse(sysdb.initWorkflowStatus(second, 5, null).shouldExecuteOnThisExecutor());

    row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(1L, row.recoveryAttempts()); // no dispatch counted: only the claim does that
    assertEquals("executor-1", row.executorId()); // the running executor keeps the row
  }

  @Test
  public void testInsertWorkflowStatusConflictEnqueued() throws Exception {
    // ENQUEUED (queue, no delay): on conflict, recovery_attempts and executor_id are both preserved
    var wfid = "wf-conflict-enqueued";
    var first =
        WorkflowStatusInternalBuilder.create(wfid)
            .queueName("myqueue")
            .executorId("executor-1")
            .build();
    sysdb.initWorkflowStatus(first, 5, null);

    var row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(WorkflowState.ENQUEUED.name(), row.status());
    assertEquals("executor-1", row.executorId());
    assertEquals(0L, row.recoveryAttempts()); // ENQUEUED starts at 0

    // Re-insert as a recovery request — ON CONFLICT should preserve both values
    var second =
        WorkflowStatusInternalBuilder.create(wfid)
            .queueName("myqueue")
            .executorId("executor-2")
            .build();
    sysdb.initWorkflowStatus(second, 5, null);

    row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(0L, row.recoveryAttempts()); // preserved — existing status was ENQUEUED
    assertEquals("executor-1", row.executorId()); // preserved — incoming status was ENQUEUED
  }

  @Test
  public void testInsertWorkflowStatusConflictDelayed() throws Exception {
    // DELAYED (queue + delay): on conflict, recovery_attempts and executor_id are both preserved
    var wfid = "wf-conflict-delayed";
    var first =
        WorkflowStatusInternalBuilder.create(wfid)
            .queueName("myqueue")
            .delay(Duration.ofHours(1))
            .executorId("executor-1")
            .build();
    sysdb.initWorkflowStatus(first, 5, null);

    var row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(WorkflowState.DELAYED.name(), row.status());
    assertEquals("executor-1", row.executorId());
    assertEquals(0L, row.recoveryAttempts()); // DELAYED starts at 0

    // Re-insert as a recovery request — ON CONFLICT should preserve both values
    var second =
        WorkflowStatusInternalBuilder.create(wfid)
            .queueName("myqueue")
            .delay(Duration.ofHours(1))
            .executorId("executor-2")
            .build();
    sysdb.initWorkflowStatus(second, 5, null);

    row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(0L, row.recoveryAttempts()); // preserved — existing status was DELAYED
    assertEquals("executor-1", row.executorId()); // preserved — incoming status was DELAYED
  }

  // ── F-4: Workflow Data Queries ────────────────────────────────────────────

  @Test
  public void testGetAllEvents() throws Exception {
    var wfId = "get-all-events-wf-1";
    sysdb.initWorkflowStatus(
        WorkflowStatusInternalBuilder.create(wfId).workflowName("TestWorkflow").build(), 5, null);

    sysdb.setEvent(wfId, 0, "key1", "value1", false, null, DBTestAccess.takeOwnership(sysdb, wfId));
    sysdb.setEvent(wfId, 1, "key2", 42, false, null, DBTestAccess.takeOwnership(sysdb, wfId));

    var events = sysdb.getAllEvents(wfId);

    assertEquals(2, events.size());
    assertEquals("value1", events.get("key1"));
    assertEquals(42, events.get("key2"));
  }

  @Test
  public void testGetAllEventsEmpty() throws Exception {
    var wfId = "get-all-events-empty-wf-1";
    sysdb.initWorkflowStatus(
        WorkflowStatusInternalBuilder.create(wfId).workflowName("TestWorkflow").build(), 5, null);

    var events = sysdb.getAllEvents(wfId);
    assertTrue(events.isEmpty());
  }

  // --- Queue CRUD tests ---

  @Test
  public void testUpsertQueueInsert() {
    var options =
        new QueueOptions()
            .withConcurrency(5)
            .withWorkerConcurrency(2)
            .withRateLimit(10, 60, java.util.concurrent.TimeUnit.SECONDS);

    boolean inserted = sysdb.upsertQueue("q-insert", options, true, null);
    assertTrue(inserted, "upsertQueue should return true when the row is new");

    var fetched = sysdb.findQueue("q-insert");
    assertTrue(fetched.isPresent());
    var q = fetched.get();
    assertEquals("q-insert", q.name());
    assertEquals(5, q.concurrency());
    assertEquals(2, q.workerConcurrency());
    assertNotNull(q.rateLimit());
    assertEquals(10, q.rateLimit().limit());
    assertEquals(Duration.ofSeconds(60), q.rateLimit().period());
  }

  @Test
  public void testUpsertQueueOptionsExisting() {
    sysdb.upsertQueue("q-update", new QueueOptions().withConcurrency(3), true, null);

    boolean inserted =
        sysdb.upsertQueue(
            "q-update", new QueueOptions().withConcurrency(7).withWorkerConcurrency(4), true, null);
    assertFalse(inserted, "upsertQueue should return false when the row already existed");

    var fetched = sysdb.findQueue("q-update").orElseThrow();
    assertEquals(7, fetched.concurrency());
    assertEquals(4, fetched.workerConcurrency());
  }

  @Test
  public void testUpsertQueueNoUpdateExisting() {
    sysdb.upsertQueue("q-no-update", new QueueOptions().withConcurrency(3), true, null);

    boolean inserted =
        sysdb.upsertQueue("q-no-update", new QueueOptions().withConcurrency(99), false, null);
    assertFalse(inserted, "upsertQueue should return false when the row already existed");

    var fetched = sysdb.findQueue("q-no-update").orElseThrow();
    assertEquals(
        3, fetched.concurrency(), "concurrency should be unchanged when updateExisting=false");
  }

  @Test
  public void testGetQueueFromDBMissing() {
    var result = sysdb.findQueue("does-not-exist");
    assertTrue(result.isEmpty());
  }

  @Test
  public void testListQueuesFromDB() {
    sysdb.upsertQueue("q-list-a", new QueueOptions().withConcurrency(1), true, null);
    sysdb.upsertQueue("q-list-b", new QueueOptions().withConcurrency(2), true, null);
    sysdb.upsertQueue("q-list-c", new QueueOptions(), true, null);

    var queues = sysdb.listQueues();
    var names = queues.stream().map(Queue::name).toList();
    assertTrue(names.contains("q-list-a"));
    assertTrue(names.contains("q-list-b"));
    assertTrue(names.contains("q-list-c"));
  }

  @Test
  public void testDeleteQueue() {
    sysdb.upsertQueue("q-delete", new QueueOptions().withConcurrency(1), true, null);
    assertTrue(sysdb.findQueue("q-delete").isPresent());

    boolean deleted = sysdb.deleteQueue("q-delete");
    assertTrue(deleted);
    assertTrue(sysdb.findQueue("q-delete").isEmpty());
  }

  @Test
  public void testDeleteQueueMissing() {
    assertFalse(sysdb.deleteQueue("q-never-existed"));
  }

  @Test
  public void testUpdateQueuePartialConcurrency() {
    sysdb.upsertQueue(
        "q-partial",
        new QueueOptions()
            .withConcurrency(5)
            .withRateLimit(10, 60, java.util.concurrent.TimeUnit.SECONDS),
        true,
        null);

    sysdb.updateQueue("q-partial", new QueueOptions().withConcurrency(99));

    var q = sysdb.findQueue("q-partial").orElseThrow();
    assertEquals(99, q.concurrency(), "concurrency should be updated");
    assertNotNull(q.rateLimit(), "rateLimit should be unchanged");
    assertEquals(10, q.rateLimit().limit());
  }

  @Test
  public void testUpdateQueueClearConcurrency() {
    sysdb.upsertQueue("q-clear-conc", new QueueOptions().withConcurrency(5), true, null);

    sysdb.updateQueue("q-clear-conc", new QueueOptions().withConcurrency((Integer) null));

    var q = sysdb.findQueue("q-clear-conc").orElseThrow();
    assertNull(q.concurrency(), "concurrency should be cleared to null");
  }

  @Test
  public void testUpdateQueueClearRateLimit() {
    sysdb.upsertQueue(
        "q-clear-rate",
        new QueueOptions().withRateLimit(5, 30, java.util.concurrent.TimeUnit.SECONDS),
        true,
        null);

    sysdb.updateQueue("q-clear-rate", new QueueOptions().withRateLimit(null, null));

    var q = sysdb.findQueue("q-clear-rate").orElseThrow();
    assertNull(q.rateLimit(), "rateLimit should be cleared to null");
  }

  @Test
  public void testPriorityEnabledIsAlwaysStoredTrue() throws Exception {
    // Every queue dispatches in priority order. The column is vestigial, but other SDKs and
    // Conductor still read it, so every write stores TRUE whatever the options ask for.
    sysdb.upsertQueue("q-prio", QueueOptions.setPriorityEnabled(false), true, null);
    assertTrue(storedPriorityEnabled("q-prio"));
    var owner = sysdb.findQueue("q-prio").orElseThrow().applicationName();

    // A row an earlier version stored as false is healed by the next partial update...
    forcePriorityEnabledFalse("q-prio");
    sysdb.updateQueue("q-prio", new QueueOptions().withConcurrency(3));
    assertTrue(storedPriorityEnabled("q-prio"));

    // ...and by the next re-registration, which binds every column through the update statement.
    forcePriorityEnabledFalse("q-prio");
    sysdb.upsertQueue(
        "q-prio",
        new QueueOptions()
            .withConcurrency(10)
            .withWorkerConcurrency(5)
            .withRateLimit(20, Duration.ofSeconds(30))
            .withPartitionConcurrency(4)
            .withPartitionWorkerConcurrency(2)
            .withPartitionRateLimit(3, Duration.ofSeconds(7))
            .withPollingInterval(Duration.ofSeconds(5)),
        true,
        null);
    assertTrue(storedPriorityEnabled("q-prio"));
    var q = sysdb.findQueue("q-prio").orElseThrow();
    assertEquals(10, q.concurrency());
    assertEquals(5, q.workerConcurrency());
    assertEquals(new Queue.RateLimit(20, Duration.ofSeconds(30)), q.rateLimit());
    assertEquals(4, q.partitionConcurrency());
    assertEquals(2, q.partitionWorkerConcurrency());
    assertEquals(new Queue.RateLimit(3, Duration.ofSeconds(7)), q.partitionRateLimit());
    assertTrue(q.isPartitioned());
    assertEquals(Duration.ofSeconds(5), q.pollingInterval());
    assertEquals(owner, q.applicationName());

    // Options that set only the ignored flag are empty, so an update with them changes nothing.
    assertTrue(QueueOptions.setPriorityEnabled(false).isEmpty());
    assertEquals(new QueueOptions(), QueueOptions.setPriorityEnabled(false));
  }

  private void forcePriorityEnabledFalse(String name) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps =
            conn.prepareStatement(
                "UPDATE dbos.queues SET priority_enabled = FALSE WHERE name = ?")) {
      ps.setString(1, name);
      ps.executeUpdate();
    }
  }

  private boolean storedPriorityEnabled(String name) throws Exception {
    try (var conn = dataSource.getConnection();
        var ps = conn.prepareStatement("SELECT priority_enabled FROM dbos.queues WHERE name = ?")) {
      ps.setString(1, name);
      try (var rs = ps.executeQuery()) {
        assertTrue(rs.next());
        return rs.getBoolean(1);
      }
    }
  }

  @Test
  public void testUpdateQueueEmpty() {
    sysdb.upsertQueue("q-empty-update", new QueueOptions().withConcurrency(5), true, null);

    // Empty update should be a no-op (no exception, no change)
    var emptyUpdate = new QueueOptions();
    sysdb.updateQueue("q-empty-update", emptyUpdate);

    var q = sysdb.findQueue("q-empty-update").orElseThrow();
    assertEquals(5, q.concurrency());
  }

  @Test
  public void testUpsertQueueRoundTrip() {
    sysdb.upsertQueue(
        "q-roundtrip",
        new QueueOptions()
            .withConcurrency(8)
            .withWorkerConcurrency(4)
            .andPartitionQueue(true)
            .withRateLimit(20, 30, java.util.concurrent.TimeUnit.SECONDS)
            .withPollingInterval(Duration.ofSeconds(5)),
        true,
        null);
    var fetched = sysdb.findQueue("q-roundtrip").orElseThrow();

    assertEquals("q-roundtrip", fetched.name());
    assertEquals(8, fetched.concurrency());
    assertEquals(4, fetched.workerConcurrency());
    assertTrue(fetched.partitioningEnabled());
    assertNotNull(fetched.rateLimit());
    assertEquals(20, fetched.rateLimit().limit());
    assertEquals(Duration.ofSeconds(30), fetched.rateLimit().period());
    assertEquals(Duration.ofSeconds(5), fetched.pollingInterval());
  }

  // --- Transaction isolation tests ---

  /**
   * Wraps a real DataSource and records the isolation level passed to {@link
   * Connection#setTransactionIsolation} on each connection it vends.
   */
  private static class IsolationRecordingDataSource implements DataSource {
    private final DataSource delegate;
    volatile int lastIsolationLevel = Connection.TRANSACTION_READ_COMMITTED; // postgres default
    final List<String> preparedSql = Collections.synchronizedList(new ArrayList<>());

    IsolationRecordingDataSource(DataSource delegate) {
      this.delegate = delegate;
    }

    @Override
    public Connection getConnection() throws SQLException {
      Connection real = delegate.getConnection();
      return (Connection)
          Proxy.newProxyInstance(
              Connection.class.getClassLoader(),
              new Class<?>[] {Connection.class},
              (proxy, method, args) -> {
                if ("setTransactionIsolation".equals(method.getName())) {
                  lastIsolationLevel = (int) args[0];
                }
                if ("prepareStatement".equals(method.getName())) {
                  preparedSql.add((String) args[0]);
                }
                try {
                  return method.invoke(real, args);
                } catch (java.lang.reflect.InvocationTargetException e) {
                  throw e.getCause();
                }
              });
    }

    @Override
    public Connection getConnection(String u, String p) throws SQLException {
      return delegate.getConnection(u, p);
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
      return delegate.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter w) throws SQLException {
      delegate.setLogWriter(w);
    }

    @Override
    public void setLoginTimeout(int s) throws SQLException {
      delegate.setLoginTimeout(s);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
      return delegate.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return delegate.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> i) throws SQLException {
      return delegate.unwrap(i);
    }

    @Override
    public boolean isWrapperFor(Class<?> i) throws SQLException {
      return delegate.isWrapperFor(i);
    }
  }

  private DbContext recordingCtx(IsolationRecordingDataSource ds) {
    String schema = SystemDatabase.sanitizeSchema(dbosConfig.databaseSchema());
    return new DbContext(ds, schema, null, () -> false, null, null, new PollingLimiter(0));
  }

  @Test
  public void testWorkerConcurrencyOnlyUsesReadCommitted() throws SQLException {
    // workerConcurrency only → local in-memory tracking, no global state → READ COMMITTED
    Queue queue = new Queue("iso-wc").withWorkerConcurrency(2);
    var ds = new IsolationRecordingDataSource(dataSource);

    QueuesDAO.startQueuedWorkflows(recordingCtx(ds), queue, "exec", "v1", null, 0, 0, "test-claim");

    assertEquals(
        Connection.TRANSACTION_READ_COMMITTED,
        ds.lastIsolationLevel,
        "workerConcurrency-only queue must not escalate to REPEATABLE READ");
  }

  @Test
  public void testGlobalConcurrencyUsesRepeatableRead() throws SQLException {
    // concurrency (global) → needs a consistent snapshot across workers → REPEATABLE READ
    Queue queue = new Queue("iso-gc").withConcurrency(3);
    var ds = new IsolationRecordingDataSource(dataSource);

    QueuesDAO.startQueuedWorkflows(recordingCtx(ds), queue, "exec", "v1", null, 0, 0, "test-claim");

    assertEquals(
        Connection.TRANSACTION_REPEATABLE_READ,
        ds.lastIsolationLevel,
        "global-concurrency queue must use REPEATABLE READ");
  }

  /** A queue partitioned by its limits, which only the full constructor can express. */
  private static Queue partitionedQueue(
      String name, Integer concurrency, Integer partitionConcurrency) {
    return new Queue(
        name,
        concurrency,
        null,
        false,
        false,
        null,
        partitionConcurrency,
        null,
        null,
        Queue.DEFAULT_POLLING_INTERVAL,
        null);
  }

  @Test
  public void testBatchedPartitionDequeueSetsReadCommitted() throws SQLException {
    // Nothing in the batched sweep spends a shared budget, so READ COMMITTED is enough, but it is
    // set rather than inherited: a user-supplied pool need not default to it.
    Queue queue = partitionedQueue("iso-batched", null, 1);
    var ds = new IsolationRecordingDataSource(dataSource);
    ds.lastIsolationLevel = Connection.TRANSACTION_NONE;

    QueuesDAO.startQueuedPartitionedWorkflows(
        recordingCtx(ds), queue, "exec", "v1", 1, 1, "test-claim");

    assertEquals(Connection.TRANSACTION_READ_COMMITTED, ds.lastIsolationLevel);
  }

  @Test
  public void testQueueWideBudgetInAPartitionUsesSerializable() throws SQLException {
    // A queue-wide budget spent one partition at a time is a write skew: each partition's sweep
    // reads the same budget and claims against its own snapshot, so together they overshoot it.
    // REPEATABLE READ does not rule that out; only serializability does. Without a partition
    // there is nothing to skew across, which the two tests above cover at their own levels.
    Queue queue = partitionedQueue("iso-partition-gc", 3, 1);
    var ds = new IsolationRecordingDataSource(dataSource);

    QueuesDAO.startQueuedWorkflows(
        recordingCtx(ds), queue, "exec", "v1", "key-a", 0, 0, "test-claim");

    assertEquals(
        Connection.TRANSACTION_SERIALIZABLE,
        ds.lastIsolationLevel,
        "a queue-wide budget claimed within one partition must use SERIALIZABLE");
  }

  @Test
  public void testPartitionLimitAloneUsesRepeatableRead() throws SQLException {
    // A per-partition budget is counted within the partition being swept, so the sweeps do not
    // share it and a consistent snapshot is enough. This pins the boundary: the escalation above
    // must follow the queue-wide budget, not the presence of a partition key.
    Queue queue = partitionedQueue("iso-partition-only", null, 2);
    var ds = new IsolationRecordingDataSource(dataSource);

    QueuesDAO.startQueuedWorkflows(
        recordingCtx(ds), queue, "exec", "v1", "key-a", 0, 0, "test-claim");

    assertEquals(
        Connection.TRANSACTION_REPEATABLE_READ,
        ds.lastIsolationLevel,
        "a partition-scoped budget must not escalate to SERIALIZABLE");
  }

  @Test
  public void testRateLimitBoundsDequeueBatch() throws SQLException {
    // A rate-limited queue must ask for only as many rows as its window still has slots for.
    // The backlog used to be selected and locked in full, with the surplus discarded after the
    // claim, so one sweep of a busy queue held a row lock on every enqueued workflow.
    int limit = 3;
    Queue queue = new Queue("rl-batch").withRateLimit(limit, Duration.ofSeconds(60));
    var ds = new IsolationRecordingDataSource(dataSource);

    QueuesDAO.startQueuedWorkflows(recordingCtx(ds), queue, "exec", "v1", null, 0, 0, "test-claim");

    var candidateSelect =
        ds.preparedSql.stream()
            .filter(sql -> sql.contains("FOR UPDATE"))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no candidate SELECT was issued"));
    assertTrue(
        candidateSelect.contains("LIMIT " + limit),
        "candidate SELECT must be bounded by the limiter's remaining slots: " + candidateSelect);
    assertTrue(
        candidateSelect.contains("FOR UPDATE NOWAIT"),
        "a rate limit is a global budget, so the claim must not SKIP LOCKED: " + candidateSelect);
  }

  @Test
  public void testRateLimitDequeueBlocksOnPeerClaim() throws Exception {
    // A rate limit is a global budget like concurrency: under SKIP LOCKED a peer mid-claim would
    // be skipped past, and both dequeuers would spend the same budget against their own snapshot.
    int limit = 2;
    Queue queue = new Queue("rl-peer").withRateLimit(limit, Duration.ofSeconds(60));

    for (int i = 0; i < limit * 2; i++) {
      sysdb.initWorkflowStatus(
          WorkflowStatusInternalBuilder.create("rl-peer-" + i)
              .queueName(queue.name())
              // Distinct priorities so that `ORDER BY priority, created_at LIMIT n` picks the same
              // rows for the peer and for the dequeue. Left to created_at they would tie: the rows
              // are inserted within one millisecond of each other, and the test needs the two
              // statements to contend rather than to lock disjoint halves.
              .priority(i + 1)
              .appVersion("v1")
              .build(),
          5,
          null);
    }

    String schema = SystemDatabase.sanitizeSchema(dbosConfig.databaseSchema());
    try (Connection peer = dataSource.getConnection()) {
      peer.setAutoCommit(false);
      // A peer dequeuer holding an open claim on the whole limiter budget.
      try (var ps =
          peer.prepareStatement(
              """
                SELECT workflow_uuid FROM "%s".workflow_status
                WHERE queue_name = ? AND status = ?
                ORDER BY priority ASC, created_at ASC
                LIMIT %d FOR UPDATE
              """
                  .formatted(schema, limit))) {
        ps.setString(1, queue.name());
        ps.setString(2, WorkflowState.ENQUEUED.name());
        try (var rs = ps.executeQuery()) {
          int locked = 0;
          while (rs.next()) locked++;
          assertEquals(limit, locked);
        }
      }

      assertThrows(
          Exception.class,
          () -> sysdb.startQueuedWorkflows(queue, "exec", "v1", null, 0, 0, "test-claim"),
          "a rate-limited dequeue must not skip past a peer's open claim");
      peer.rollback();
    }

    // Nothing was admitted behind the peer's back.
    for (int i = 0; i < limit * 2; i++) {
      assertEquals(
          WorkflowState.ENQUEUED.name(),
          DBUtils.getWorkflowRow(dataSource, "rl-peer-" + i).status());
    }
  }

  @Test
  public void testContentionErrorMatchesLockNotAvailable() {
    // The 55P03 a NOWAIT claim raises reaches the queue listener wrapped by dbRetry, so the
    // classifier has to walk the cause chain rather than look at the top-level exception.
    var lockNotAvailable = new SQLException("could not obtain lock on row", "55P03");
    assertTrue(SystemDatabase.isContentionError(lockNotAvailable));
    assertTrue(SystemDatabase.isContentionError(new RuntimeException(lockNotAvailable)));

    // A REPEATABLE READ dequeue loses the same race as 40001, and dbRetry no longer absorbs it.
    assertTrue(
        SystemDatabase.isContentionError(new SQLException("serialization failure", "40001")));

    // A deadlock is class 40 too, but nothing in the dequeue expects one. Callers that can replay
    // their transaction handle it themselves; see ConflictRetryTest.
    assertFalse(SystemDatabase.isContentionError(new SQLException("deadlock detected", "40P01")));
    assertFalse(SystemDatabase.isContentionError(new SQLException("no state")));
    assertFalse(SystemDatabase.isContentionError(new RuntimeException("boom")));
  }

  @Test
  public void testSerializationErrorMatchesClass40Codes() {
    // Retention and transactional steps replay these; nothing else may. See
    // WorkflowDAO.retryOnSerializationError and PostgresStepFactory.runTxStep.
    assertTrue(
        SystemDatabase.isSerializationError(new SQLException("serialization failure", "40001")));
    assertTrue(SystemDatabase.isSerializationError(new SQLException("deadlock detected", "40P01")));
    // Wrapped, as a step factory sees it: the JDBC exception arrives inside whatever the
    // datasource layer threw, so the predicate has to walk the cause chain.
    assertTrue(
        SystemDatabase.isSerializationError(
            new RuntimeException(new SQLException("serialization failure", "40001"))));

    assertFalse(
        SystemDatabase.isSerializationError(new SQLException("lock not available", "55P03")));
    assertFalse(
        SystemDatabase.isSerializationError(new SQLException("too many connections", "53300")));
    assertFalse(SystemDatabase.isSerializationError(new SQLException("no state")));
    assertFalse(SystemDatabase.isSerializationError(new RuntimeException("boom")));

    // JDBC's standard type for exactly these rollbacks extends SQLTransientException, which
    // dbRetry retries blind. It has to be recognised by SQLSTATE before that branch is reached,
    // or a driver that throws this type would put a conflict back into the unbounded loop.
    assertTrue(
        SystemDatabase.isSerializationError(
            new java.sql.SQLTransactionRollbackException("serialization failure", "40001")));
  }

  @Test
  public void testRateLimitUsesRepeatableRead() throws SQLException {
    // rateLimit → count must be consistent across concurrent dequeue attempts → REPEATABLE READ
    Queue queue = new Queue("iso-rl").withRateLimit(5, Duration.ofSeconds(1));
    var ds = new IsolationRecordingDataSource(dataSource);

    QueuesDAO.startQueuedWorkflows(recordingCtx(ds), queue, "exec", "v1", null, 0, 0, "test-claim");

    assertEquals(
        Connection.TRANSACTION_REPEATABLE_READ,
        ds.lastIsolationLevel,
        "rate-limited queue must use REPEATABLE READ");
  }
}
