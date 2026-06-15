package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.dao.QueuesDAO;
import dev.dbos.transact.exceptions.DBOSMaxRecoveryAttemptsExceededException;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.WorkflowStatusBuilder;
import dev.dbos.transact.utils.WorkflowStatusInternalBuilder;
import dev.dbos.transact.workflow.ExportedWorkflow;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.GetStepAggregatesInput;
import dev.dbos.transact.workflow.GetWorkflowAggregatesInput;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.SendMessage;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.VersionInfo;
import dev.dbos.transact.workflow.WorkflowAggregateRow;
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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SystemDatabaseTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose SystemDatabase sysdb;

  @AutoClose HikariDataSource dataSource;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    MigrationManager.runMigrations(dbosConfig);
    sysdb = SystemDatabase.create(dbosConfig);
    dataSource = pgContainer.dataSource();
  }

  @Test
  public void testDeleteWorkflows() throws Exception {
    for (var i = 0; i < 5; i++) {
      var wfid = "wfid-%d".formatted(i);
      var status = WorkflowStatusInternalBuilder.create(wfid).build();
      sysdb.initWorkflowStatus(status, 5, false, false);
    }

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(5, rows.size());

    sysdb.deleteWorkflows(List.of("wfid-1", "wfid-3"), false);

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
    sysdb.createApplicationVersion("v1.0.0");

    List<VersionInfo> versions = sysdb.listApplicationVersions();
    assertEquals(1, versions.size());
    assertEquals("v1.0.0", versions.get(0).versionName());
    assertNotNull(versions.get(0).versionId());
    assertNotNull(versions.get(0).versionTimestamp());
    assertNotNull(versions.get(0).createdAt());
  }

  @Test
  public void testCreateApplicationVersionIdempotent() throws Exception {
    sysdb.createApplicationVersion("v1.0.0");
    sysdb.createApplicationVersion("v1.0.0");

    assertEquals(1, sysdb.listApplicationVersions().size());
  }

  @Test
  public void testListApplicationVersionsOrderedByTimestamp() throws Exception {
    Instant t1 = Instant.now();
    sysdb.createApplicationVersion("v1.0.0");
    sysdb.updateApplicationVersionTimestamp("v1.0.0", t1);

    Instant t2 = t1.plusSeconds(1);
    sysdb.createApplicationVersion("v2.0.0");
    sysdb.updateApplicationVersionTimestamp("v2.0.0", t2);

    Instant t3 = t1.plusSeconds(2);
    sysdb.createApplicationVersion("v3.0.0");
    sysdb.updateApplicationVersionTimestamp("v3.0.0", t3);

    List<VersionInfo> versions = sysdb.listApplicationVersions();
    assertEquals(3, versions.size());
    assertEquals("v3.0.0", versions.get(0).versionName());
    assertEquals("v2.0.0", versions.get(1).versionName());
    assertEquals("v1.0.0", versions.get(2).versionName());
  }

  @Test
  public void testGetLatestApplicationVersion() throws Exception {
    Instant t1 = Instant.now();
    sysdb.createApplicationVersion("v1.0.0");
    sysdb.updateApplicationVersionTimestamp("v1.0.0", t1);

    sysdb.createApplicationVersion("v2.0.0");
    sysdb.updateApplicationVersionTimestamp("v2.0.0", t1.plusSeconds(1));

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
      sysdb.initWorkflowStatus(status, 5, false, false);
    }

    sysdb.deleteWorkflows(List.of("wfid-0", "wfid-2", "wfid-4"), false);

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(2, rows.size());
    assertTrue(rows.stream().anyMatch(r -> r.workflowId().equals("wfid-1")));
    assertTrue(rows.stream().anyMatch(r -> r.workflowId().equals("wfid-3")));
  }

  @Test
  public void testCancelWorkflows() throws Exception {
    // Create workflows in different states
    for (var wfid : List.of("wf-pending-1", "wf-pending-2", "wf-pending-3")) {
      sysdb.initWorkflowStatus(WorkflowStatusInternalBuilder.create(wfid).build(), 5, false, false);
    }
    sysdb.initWorkflowStatus(
        WorkflowStatusInternalBuilder.create("wf-success").build(), 5, false, false);
    sysdb.initWorkflowStatus(
        WorkflowStatusInternalBuilder.create("wf-error").build(), 5, false, false);
    DBUtils.setWorkflowState(dataSource, "wf-success", WorkflowState.SUCCESS.name());
    DBUtils.setWorkflowState(dataSource, "wf-error", WorkflowState.ERROR.name());

    // Record time before cancel so we can assert updated_at advances
    long beforeCancel = System.currentTimeMillis();

    // Cancel all five IDs in one call
    sysdb.cancelWorkflows(
        List.of("wf-pending-1", "wf-pending-2", "wf-pending-3", "wf-success", "wf-error"), false);

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
      sysdb.initWorkflowStatus(WorkflowStatusInternalBuilder.create(wfid).build(), 5, false, false);
      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.CANCELLED.name());
    }
    sysdb.initWorkflowStatus(
        WorkflowStatusInternalBuilder.create("wf-success").build(), 5, false, false);
    sysdb.initWorkflowStatus(
        WorkflowStatusInternalBuilder.create("wf-error").build(), 5, false, false);
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
    sysdb.resumeWorkflows(workflowIds, null);

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
    sysdb.resumeWorkflows(workflowIds, "customQueue");

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
    sysdb.initWorkflowStatus(
        WorkflowStatusInternalBuilder.create("wf-id").build(), 5, false, false);

    long beforeCancel = System.currentTimeMillis();
    sysdb.cancelWorkflows(Arrays.asList("wf-id", null), false);

    var row = DBUtils.getWorkflowRow(dataSource, "wf-id");
    assertEquals(WorkflowState.CANCELLED.name(), row.status());
    assertTrue(row.updatedAt() >= beforeCancel, "updated_at should be >= time before cancel");
  }

  @Test
  public void testCancelWorkflowsWithChildren() throws Exception {
    // Build a 3-level tree: parent -> 3 children -> 2 grandchildren each
    sysdb.initWorkflowStatus(
        WorkflowStatusInternalBuilder.create("parent").build(), 5, false, false);

    for (var i = 0; i < 3; i++) {
      var childId = "child-%d".formatted(i);
      sysdb.initWorkflowStatus(
          WorkflowStatusInternalBuilder.create(childId).parentWorkflowId("parent").build(),
          5,
          false,
          false);

      for (var j = 0; j < 2; j++) {
        var grandchildId = "grandchild-%d-%d".formatted(i, j);
        sysdb.initWorkflowStatus(
            WorkflowStatusInternalBuilder.create(grandchildId).parentWorkflowId(childId).build(),
            5,
            false,
            false);
      }
    }

    // Without cancelChildren=true, only the parent is cancelled
    sysdb.cancelWorkflows(List.of("parent"), false);
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
    sysdb.cancelWorkflows(List.of("parent"), true);
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
    sysdb.initWorkflowStatus(
        WorkflowStatusInternalBuilder.create("wf-id").build(), 5, false, false);
    DBUtils.setWorkflowState(dataSource, "wf-id", WorkflowState.CANCELLED.name());

    long beforeResume = System.currentTimeMillis();
    sysdb.resumeWorkflows(Arrays.asList("wf-id", null), null);

    var row = DBUtils.getWorkflowRow(dataSource, "wf-id");
    assertEquals(WorkflowState.ENQUEUED.name(), row.status());
    assertTrue(row.updatedAt() >= beforeResume, "updated_at should be >= time before resume");
    assertNull(row.deadlineEpochMs(), "workflow_deadline_epoch_ms should be cleared on resume");
  }

  @Test
  public void testDeleteWorkflowsNullInList() throws Exception {
    sysdb.initWorkflowStatus(
        WorkflowStatusInternalBuilder.create("wf-id").build(), 5, false, false);

    sysdb.deleteWorkflows(Arrays.asList("wf-id", null), false);

    assertNull(DBUtils.getWorkflowRow(dataSource, "wf-id"));
  }

  @Test
  public void testGetChildWorkflows() throws Exception {
    for (var i = 0; i < 5; i++) {
      var wfid = "wfid-%d".formatted(i);
      var status = WorkflowStatusInternalBuilder.create(wfid).build();
      sysdb.initWorkflowStatus(status, 5, false, false);
    }

    for (var i = 0; i < 5; i++) {
      var wfid = "childwfid-%d".formatted(i);
      sysdb.initWorkflowStatus(
          WorkflowStatusInternalBuilder.create(wfid).parentWorkflowId("wfid-2").build(),
          5,
          false,
          false);
    }

    for (var i = 0; i < 5; i++) {
      var wfid = "grandchildwfid-%d".formatted(i);
      sysdb.initWorkflowStatus(
          WorkflowStatusInternalBuilder.create(wfid)
              .parentWorkflowId("childwfid-%d".formatted(i))
              .build(),
          5,
          false,
          false);
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

    for (var i = 1; i <= 6; i++) {
      var result1 = sysdb.initWorkflowStatus(status, 5, true, false);
      assertEquals(WorkflowState.PENDING, result1.status());
      assertNull(result1.deadline());

      var row = DBUtils.getWorkflowRow(dataSource, wfid);
      assertNotNull(row);
      assertEquals(WorkflowState.PENDING.name(), row.status());
      assertEquals(i, row.recoveryAttempts());
    }

    assertThrows(
        DBOSMaxRecoveryAttemptsExceededException.class,
        () -> sysdb.initWorkflowStatus(status, 5, true, false));
    var row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertNotNull(row);
    assertEquals(WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED.name(), row.status());
    assertEquals(7, row.recoveryAttempts());
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

    var result1 = sysdb.initWorkflowStatus(builder.build(), 5, false, false);
    assertEquals(WorkflowState.ENQUEUED, result1.status());
    assertNull(result1.deadline());

    var before = DBUtils.getWorkflowRow(dataSource, wfid);
    assertThrows(
        DBOSQueueDuplicatedException.class,
        () -> sysdb.initWorkflowStatus(builder.workflowId("wfid-2").build(), 5, false, false));
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
        null);
  }

  @Test
  public void testCreateAndGetSchedule() {
    sysdb.createSchedule(makeSchedule("sched-1"));

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
    sysdb.createSchedule(makeSchedule("sched-dup"));
    assertThrows(RuntimeException.class, () -> sysdb.createSchedule(makeSchedule("sched-dup")));
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
                null));
  }

  @Test
  public void testListSchedules() {
    sysdb.createSchedule(makeSchedule("alpha-1"));
    sysdb.createSchedule(makeSchedule("alpha-2"));
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
            null));
    sysdb.pauseSchedule("beta-1");

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
    sysdb.createSchedule(makeSchedule("test%s Sched"));
    sysdb.createSchedule(makeSchedule("test%sother"));
    sysdb.createSchedule(makeSchedule("test_schedule"));
    sysdb.createSchedule(makeSchedule("test_sched"));
    sysdb.createSchedule(makeSchedule("testA_schedB"));

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
    sysdb.createSchedule(makeSchedule("sched-pause"));

    sysdb.pauseSchedule("sched-pause");
    assertEquals(ScheduleStatus.PAUSED, sysdb.getSchedule("sched-pause").get().status());

    sysdb.resumeSchedule("sched-pause");
    assertEquals(ScheduleStatus.ACTIVE, sysdb.getSchedule("sched-pause").get().status());
  }

  @Test
  public void testUpdateLastFiredAt() {
    sysdb.createSchedule(makeSchedule("sched-fired"));
    assertNull(sysdb.getSchedule("sched-fired").get().lastFiredAt());

    sysdb.updateScheduleLastFiredAt("sched-fired", Instant.parse("2026-03-26T10:00:00Z"));
    assertEquals(
        Instant.parse("2026-03-26T10:00:00Z"),
        sysdb.getSchedule("sched-fired").get().lastFiredAt());
  }

  @Test
  public void testDeleteSchedule() {
    sysdb.createSchedule(makeSchedule("sched-del-1"));
    sysdb.createSchedule(makeSchedule("sched-del-2"));
    assertEquals(2, sysdb.listSchedules(null, null, null).size());

    sysdb.deleteSchedule("sched-del-1");
    assertFalse(sysdb.getSchedule("sched-del-1").isPresent());
    assertEquals(1, sysdb.listSchedules(null, null, null).size());

    sysdb.deleteSchedule("sched-del-2");
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
            "my-queue");
    sysdb.createSchedule(schedule);

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
    sysdb.initWorkflowStatus(status, null, false, false);

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
    sysdb.initWorkflowStatus(status, null, false, false);

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
    sysdb.initWorkflowStatus(status, null, false, false);

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
    sysdb.initWorkflowStatus(originalStatus, null, false, false);

    // Verify original workflow has correct authentication fields
    var originalRetrieved = sysdb.getWorkflowStatus(originalWorkflowId);
    assertNotNull(originalRetrieved);
    assertEquals(authenticatedUser, originalRetrieved.authenticatedUser());
    assertEquals(assumedRole, originalRetrieved.assumedRole());
    assertEquals(List.of(authenticatedRoles), originalRetrieved.authenticatedRoles());

    // Fork the workflow
    var forkOptions = new ForkOptions().withApplicationVersion("1.0.0");
    var forkedWorkflowId = sysdb.forkWorkflow(originalWorkflowId, 0, forkOptions);
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
    sysdb.initWorkflowStatus(originalStatus, null, false, false);

    // Fork the workflow
    var forkOptions = new ForkOptions().withApplicationVersion("1.0.0");
    var forkedWorkflowId = sysdb.forkWorkflow(originalWorkflowId, 0, forkOptions);
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
    sysdb.initWorkflowStatus(originalStatus, null, false, false);

    // Verify original workflow has correct authentication fields including empty roles
    var originalRetrieved = sysdb.getWorkflowStatus(originalWorkflowId);
    assertNotNull(originalRetrieved);
    assertEquals(authenticatedUser, originalRetrieved.authenticatedUser());
    assertEquals(assumedRole, originalRetrieved.assumedRole());
    assertEquals(0, originalRetrieved.authenticatedRoles().size());

    // Fork the workflow
    var forkOptions = new ForkOptions().withApplicationVersion("1.0.0");
    var forkedWorkflowId = sysdb.forkWorkflow(originalWorkflowId, 0, forkOptions);
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
    sysdb.initWorkflowStatus(status, 5, false, false);
    int functionId = 1;

    sysdb.writeStreamFromStep(workflowId, functionId, "key1", "value1", "portable_json");
    sysdb.writeStreamFromStep(workflowId, functionId, "key1", "value2", "portable_json");

    Object result = sysdb.readStream(workflowId, "key1", 0);
    assertEquals("value1", result);

    result = sysdb.readStream(workflowId, "key1", 1);
    assertEquals("value2", result);
  }

  @Test
  public void testWriteStreamFromWorkflow() throws Exception {
    String workflowId = "stream-wf-2";
    var status = WorkflowStatusInternalBuilder.create(workflowId).build();
    sysdb.initWorkflowStatus(status, 5, false, false);
    int functionId = 1;

    sysdb.writeStreamFromWorkflow(workflowId, functionId, "key1", "value1", "portable_json");

    Object result = sysdb.readStream(workflowId, "key1", 0);
    assertEquals("value1", result);
  }

  @Test
  public void testCloseStream() throws Exception {
    String workflowId = "stream-wf-3";
    var status = WorkflowStatusInternalBuilder.create(workflowId).build();
    sysdb.initWorkflowStatus(status, 5, false, false);

    sysdb.writeStreamFromWorkflow(workflowId, 1, "key1", "value1", "portable_json");
    sysdb.closeStream(workflowId, 2, "key1");

    assertEquals(SystemDatabase.END_OF_STREAM, sysdb.readStream(workflowId, "key1", 1));
  }

  @Test
  public void testGetAllStreamEntries() throws Exception {
    String workflowId = "stream-wf-4";
    var status = WorkflowStatusInternalBuilder.create(workflowId).build();
    sysdb.initWorkflowStatus(status, 5, false, false);
    int functionId = 1;

    sysdb.writeStreamFromStep(workflowId, functionId, "key1", "value1", "portable_json");
    sysdb.writeStreamFromStep(workflowId, functionId, "key1", "value2", "portable_json");
    sysdb.writeStreamFromStep(workflowId, functionId, "key2", "value3", "portable_json");
    sysdb.closeStream(workflowId, functionId, "key2");

    Map<String, List<Object>> entries = sysdb.getAllStreamEntries(workflowId);

    assertEquals(2, entries.size());
    assertEquals(List.of("value1", "value2"), entries.get("key1"));
    assertEquals(List.of("value3"), entries.get("key2"));
  }

  @Test
  public void testReadStreamNotFound() throws Exception {
    String workflowId = "stream-wf-5";
    var status = WorkflowStatusInternalBuilder.create(workflowId).build();
    sysdb.initWorkflowStatus(status, 5, false, false);

    DBUtils.setWorkflowState(dataSource, workflowId, WorkflowState.SUCCESS.name());
    assertEquals(SystemDatabase.END_OF_STREAM, sysdb.readStream(workflowId, "key", 0));
  }

  public void testInsertWorkflowStatusValidation() throws Exception {
    // Test null workflowId
    assertThrows(
        NullPointerException.class,
        () -> {
          var status = WorkflowStatusInternalBuilder.create(null).build();
          sysdb.initWorkflowStatus(status, null, false, false);
        });
  }

  @Test
  public void testInsertWorkflowStatusEmptyStringValidation() throws Exception {
    // Test empty workflowName
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          var status = WorkflowStatusInternalBuilder.create("test-wf-1").workflowName("").build();
          sysdb.initWorkflowStatus(status, null, false, false);
        });

    // Test empty className
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          var status = WorkflowStatusInternalBuilder.create("test-wf-2").className("").build();
          sysdb.initWorkflowStatus(status, null, false, false);
        });

    // Test empty instanceName
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          var status = WorkflowStatusInternalBuilder.create("test-wf-3").instanceName("").build();
          sysdb.initWorkflowStatus(status, null, false, false);
        });

    // Test empty queueName
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          var status = WorkflowStatusInternalBuilder.create("test-wf-4").queueName("").build();
          sysdb.initWorkflowStatus(status, null, false, false);
        });

    // Test empty deduplicationId
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          var status =
              WorkflowStatusInternalBuilder.create("test-wf-5").deduplicationId("").build();
          sysdb.initWorkflowStatus(status, null, false, false);
        });

    // Test empty queuePartitionKey
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          var status =
              WorkflowStatusInternalBuilder.create("test-wf-6").queuePartitionKey("").build();
          sysdb.initWorkflowStatus(status, null, false, false);
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
    var result = sysdb.initWorkflowStatus(status, null, false, false);
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
    var result = sysdb.initWorkflowStatus(status, null, false, false);
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
    var ex = assertThrows(IllegalStateException.class, () -> new SystemDatabase(ds, "dbos"));
    assertTrue(ex.getMessage().contains("PostgreSQL"));
    assertTrue(ex.getMessage().contains("MySQL"));
  }

  @Test
  public void testNonPostgresDataSourceViaCreateThrows() throws SQLException {
    var ds = mockDataSource("SQLite");
    var config = DBOSConfig.defaults("test-app").withDataSource(ds);
    var ex = assertThrows(IllegalStateException.class, () -> SystemDatabase.create(config));
    assertTrue(ex.getMessage().contains("PostgreSQL"));
    assertTrue(ex.getMessage().contains("SQLite"));
  }

  @Test
  public void testSqliteDataSourceThrows() {
    var ds = new org.sqlite.SQLiteDataSource();
    ds.setUrl("jdbc:sqlite::memory:");
    var ex = assertThrows(IllegalStateException.class, () -> new SystemDatabase(ds, "dbos"));
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

  private static ExportedWorkflow buildEmptyWorkflow(String wfId) {
    return buildNamedWorkflow(wfId, "TestWorkflow", WorkflowState.SUCCESS);
  }

  private static ExportedWorkflow buildNamedWorkflow(
      String wfId, String workflowName, WorkflowState state) {
    var now = Instant.now();
    boolean isTerminal =
        state == WorkflowState.SUCCESS
            || state == WorkflowState.ERROR
            || state == WorkflowState.CANCELLED;
    var builder =
        new WorkflowStatusBuilder(wfId)
            .status(state)
            .workflowName(workflowName)
            .appVersion("1.0.0")
            .recoveryAttempts(0)
            .priority(0)
            .createdAt(now)
            .updatedAt(now);
    if (isTerminal) {
      builder.completedAt(now);
    }
    return new ExportedWorkflow(builder.build(), List.of(), List.of(), List.of(), List.of());
  }

  // ── Workflow state based on queue/delay ──────────────────────────────────

  @Test
  public void testInitWorkflowStatusStateNoQueue() throws Exception {
    var wfid = "wf-state-no-queue";
    var result =
        sysdb.initWorkflowStatus(
            WorkflowStatusInternalBuilder.create(wfid).build(), 5, false, false);
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
            WorkflowStatusInternalBuilder.create(wfid).queueName("test-queue").build(),
            5,
            false,
            false);
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
            false,
            false);
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
        false,
        false);

    long before = System.currentTimeMillis();
    sysdb.setWorkflowDelay(wfid, new WorkflowDelay.Delay(Duration.ofSeconds(30)));

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
        false,
        false);

    var targetInstant = Instant.now().plusSeconds(120);
    sysdb.setWorkflowDelay(wfid, new WorkflowDelay.DelayUntil(targetInstant));

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
          WorkflowStatusInternalBuilder.create(wfid).queueName("test-queue").build(),
          5,
          false,
          false);
      DBUtils.setWorkflowState(dataSource, wfid, state.name());

      sysdb.setWorkflowDelay(wfid, targetDelay);

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
        false,
        false);

    sysdb.setWorkflowDelay(wfid, new WorkflowDelay.DelayUntil(Instant.now().minusSeconds(5)));
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
        false,
        false);

    sysdb.setWorkflowDelay(wfid, new WorkflowDelay.DelayUntil(Instant.now().plusSeconds(60)));
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
          false,
          false);
    }

    sysdb.setWorkflowDelay(pastWfid, new WorkflowDelay.DelayUntil(Instant.now().minusSeconds(5)));
    sysdb.setWorkflowDelay(futureWfid, new WorkflowDelay.DelayUntil(Instant.now().plusSeconds(60)));

    sysdb.transitionDelayedWorkflows();

    assertEquals(
        WorkflowState.ENQUEUED.name(), DBUtils.getWorkflowRow(dataSource, pastWfid).status());
    assertEquals(
        WorkflowState.DELAYED.name(), DBUtils.getWorkflowRow(dataSource, futureWfid).status());
  }

  // ── insertWorkflowStatus ON CONFLICT behavior ──────────────────────────────

  @Test
  public void testInsertWorkflowStatusConflictPending() throws Exception {
    // PENDING (no queue): on conflict, recovery_attempts is incremented and executor_id is updated
    var wfid = "wf-conflict-pending";
    var first = WorkflowStatusInternalBuilder.create(wfid).executorId("executor-1").build();
    sysdb.initWorkflowStatus(first, 5, false, false);

    var row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(WorkflowState.PENDING.name(), row.status());
    assertEquals("executor-1", row.executorId());
    assertEquals(1L, row.recoveryAttempts()); // PENDING starts at 1

    // Re-insert as a recovery request — ON CONFLICT should increment recovery_attempts and update
    // executor_id
    var second = WorkflowStatusInternalBuilder.create(wfid).executorId("executor-2").build();
    sysdb.initWorkflowStatus(second, 5, true, false);

    row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(2L, row.recoveryAttempts()); // 1 + 1 = 2
    assertEquals("executor-2", row.executorId()); // updated to new executor
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
    sysdb.initWorkflowStatus(first, 5, false, false);

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
    sysdb.initWorkflowStatus(second, 5, true, false);

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
    sysdb.initWorkflowStatus(first, 5, false, false);

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
    sysdb.initWorkflowStatus(second, 5, true, false);

    row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertEquals(0L, row.recoveryAttempts()); // preserved — existing status was DELAYED
    assertEquals("executor-1", row.executorId()); // preserved — incoming status was DELAYED
  }

  @Test
  public void testGetWorkflowAggregatesBasic() throws Exception {
    sysdb.importWorkflow(
        List.of(
            buildNamedWorkflow("agg-wf-1", "WorkflowA", WorkflowState.SUCCESS),
            buildNamedWorkflow("agg-wf-2", "WorkflowA", WorkflowState.SUCCESS),
            buildNamedWorkflow("agg-wf-3", "WorkflowA", WorkflowState.ERROR),
            buildNamedWorkflow("agg-wf-4", "WorkflowB", WorkflowState.PENDING)));

    var input = new GetWorkflowAggregatesInput().withGroupByName(true).withGroupByStatus(true);
    var rows = sysdb.getWorkflowAggregates(input);

    var successA =
        rows.stream()
            .filter(
                r ->
                    "WorkflowA".equals(r.group().get("name"))
                        && WorkflowState.SUCCESS.name().equals(r.group().get("status")))
            .findFirst();
    assertTrue(successA.isPresent());
    assertEquals(2, successA.get().count());

    var errorA =
        rows.stream()
            .filter(
                r ->
                    "WorkflowA".equals(r.group().get("name"))
                        && WorkflowState.ERROR.name().equals(r.group().get("status")))
            .findFirst();
    assertTrue(errorA.isPresent());
    assertEquals(1, errorA.get().count());

    var pendingB =
        rows.stream()
            .filter(
                r ->
                    "WorkflowB".equals(r.group().get("name"))
                        && WorkflowState.PENDING.name().equals(r.group().get("status")))
            .findFirst();
    assertTrue(pendingB.isPresent());
    assertEquals(1, pendingB.get().count());
  }

  @Test
  public void testGetWorkflowAggregatesWithFilter() throws Exception {
    sysdb.importWorkflow(
        List.of(
            buildNamedWorkflow("agg-filter-wf-1", "WorkflowA", WorkflowState.SUCCESS),
            buildNamedWorkflow("agg-filter-wf-2", "WorkflowA", WorkflowState.ERROR),
            buildNamedWorkflow("agg-filter-wf-3", "WorkflowB", WorkflowState.SUCCESS)));

    var input =
        new GetWorkflowAggregatesInput()
            .withGroupByName(true)
            .withGroupByStatus(true)
            .withWorkflowName(List.of("WorkflowA"));
    var rows = sysdb.getWorkflowAggregates(input);

    assertEquals(2, rows.size());
    assertTrue(rows.stream().allMatch(r -> "WorkflowA".equals(r.group().get("name"))));
  }

  @Test
  public void testGetWorkflowAggregatesIdPrefix() throws Exception {
    sysdb.importWorkflow(
        List.of(
            buildNamedWorkflow("prefix-aaa-1", "WorkflowA", WorkflowState.SUCCESS),
            buildNamedWorkflow("prefix-aaa-2", "WorkflowA", WorkflowState.SUCCESS),
            buildNamedWorkflow("prefix-bbb-1", "WorkflowB", WorkflowState.SUCCESS)));

    var input =
        new GetWorkflowAggregatesInput()
            .withGroupByName(true)
            .withWorkflowIdPrefix(List.of("prefix-aaa"));
    var rows = sysdb.getWorkflowAggregates(input);

    assertEquals(1, rows.size());
    assertEquals("WorkflowA", rows.get(0).group().get("name"));
    assertEquals(2, rows.get(0).count());
  }

  @Test
  public void testGetWorkflowAggregatesIdPrefixNoMatch() throws Exception {
    sysdb.importWorkflow(
        List.of(
            buildNamedWorkflow("pref-neg-aaa-1", "WorkflowA", WorkflowState.SUCCESS),
            buildNamedWorkflow("pref-neg-aaa-2", "WorkflowA", WorkflowState.SUCCESS)));

    var rows =
        sysdb.getWorkflowAggregates(
            new GetWorkflowAggregatesInput()
                .withGroupByName(true)
                .withWorkflowIdPrefix(List.of("prefix-xyz"))
                .withSelectCount(true));
    assertTrue(rows.isEmpty());
  }

  @Test
  public void testGetWorkflowAggregatesNoGroupByThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> sysdb.getWorkflowAggregates(new GetWorkflowAggregatesInput()));
  }

  @Test
  public void testGetWorkflowAggregatesEmpty() throws Exception {
    var input = new GetWorkflowAggregatesInput().withGroupByStatus(true);
    var rows = sysdb.getWorkflowAggregates(input);
    assertTrue(rows.isEmpty());
  }

  @Test
  public void testGetWorkflowAggregatesNoSelectThrows() {
    var input =
        new GetWorkflowAggregatesInput()
            .withGroupByStatus(true)
            .withSelectCount(false); // explicitly disable the default
    assertThrows(IllegalArgumentException.class, () -> sysdb.getWorkflowAggregates(input));
  }

  @Test
  public void testGetWorkflowAggregatesTimeBucketInvalidThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new GetWorkflowAggregatesInput()
                .withTimeBucketSize(Duration.ZERO)
                .withSelectCount(true));
  }

  @Test
  public void testGetWorkflowAggregatesSelectFlags() throws Exception {
    sysdb.importWorkflow(
        List.of(
            buildNamedWorkflow("agg-sel-1", "WorkflowA", WorkflowState.SUCCESS),
            buildNamedWorkflow("agg-sel-2", "WorkflowA", WorkflowState.SUCCESS)));

    // count=true, min_created_at=false → min_created_at must be null
    var rows = sysdb.getWorkflowAggregates(new GetWorkflowAggregatesInput().withGroupByName(true));
    assertEquals(1, rows.size());
    assertNotNull(rows.get(0).count());
    assertEquals(2L, rows.get(0).count());
    assertNull(rows.get(0).minCreatedAt());
    assertNull(rows.get(0).maxQueueWait());
    assertNull(rows.get(0).maxTotalLatency());

    // min_created_at=true, count=false
    var rows2 =
        sysdb.getWorkflowAggregates(
            new GetWorkflowAggregatesInput()
                .withGroupByName(true)
                .withSelectCount(false)
                .withSelectMinCreatedAt(true));
    assertEquals(1, rows2.size());
    assertNull(rows2.get(0).count());
    assertNotNull(rows2.get(0).minCreatedAt());
    assertTrue(rows2.get(0).minCreatedAt().toEpochMilli() > 0);

    // Both count and min_created_at → both populated
    var rows3 =
        sysdb.getWorkflowAggregates(
            new GetWorkflowAggregatesInput().withGroupByName(true).withSelectMinCreatedAt(true));
    assertEquals(1, rows3.size());
    assertNotNull(rows3.get(0).count());
    assertEquals(2L, rows3.get(0).count());
    assertNotNull(rows3.get(0).minCreatedAt());
    assertTrue(rows3.get(0).minCreatedAt().toEpochMilli() > 0);
  }

  @Test
  public void testGetWorkflowAggregatesTimeBucket() throws Exception {
    sysdb.importWorkflow(
        List.of(
            buildNamedWorkflow("agg-tb-1", "WorkflowA", WorkflowState.SUCCESS),
            buildNamedWorkflow("agg-tb-2", "WorkflowA", WorkflowState.ERROR)));

    long bucketMs = 3_600_000L; // 1 hour
    var rows =
        sysdb.getWorkflowAggregates(
            new GetWorkflowAggregatesInput()
                .withTimeBucketSize(Duration.ofMillis(bucketMs))
                .withSelectCount(true));
    assertFalse(rows.isEmpty());
    for (var r : rows) {
      assertTrue(r.group().containsKey("time_bucket"));
      long tb = Long.parseLong(r.group().get("time_bucket"));
      assertEquals(0, tb % bucketMs);
    }
    long total = rows.stream().mapToLong(r -> r.count()).sum();
    assertTrue(total >= 2);
  }

  @Test
  public void testGetWorkflowAggregatesTimeBucketWithFilters() throws Exception {
    sysdb.importWorkflow(
        List.of(
            buildNamedWorkflow("agg-tbf-1", "WorkflowA", WorkflowState.SUCCESS),
            buildNamedWorkflow("agg-tbf-2", "WorkflowA", WorkflowState.ERROR)));

    long bucketMs = 3_600_000L;

    // Time bucket + group_by_status combined
    var rows =
        sysdb.getWorkflowAggregates(
            new GetWorkflowAggregatesInput()
                .withTimeBucketSize(Duration.ofMillis(bucketMs))
                .withGroupByStatus(true)
                .withSelectCount(true)
                .withWorkflowIdPrefix(List.of("agg-tbf-")));
    assertEquals(2, rows.size());
    for (var r : rows) {
      assertTrue(r.group().containsKey("status"));
      assertTrue(r.group().containsKey("time_bucket"));
      long tb = Long.parseLong(r.group().get("time_bucket"));
      assertEquals(0, tb % bucketMs);
    }

    // Time bucket + status filter (no status group-by)
    var errRows =
        sysdb.getWorkflowAggregates(
            new GetWorkflowAggregatesInput()
                .withTimeBucketSize(Duration.ofMillis(bucketMs))
                .withStatus(WorkflowState.ERROR)
                .withSelectCount(true)
                .withWorkflowIdPrefix(List.of("agg-tbf-")));
    assertEquals(1, errRows.size());
    assertTrue(errRows.get(0).group().containsKey("time_bucket"));
    assertEquals(1L, errRows.get(0).count());
  }

  @Test
  public void testGetWorkflowAggregatesQueueGroupBy() throws Exception {
    var now = Instant.now();

    var q1Status =
        new WorkflowStatusBuilder("agg-q-1")
            .status(WorkflowState.SUCCESS)
            .workflowName("WorkflowA")
            .appVersion("1.0.0")
            .recoveryAttempts(0)
            .priority(0)
            .createdAt(now)
            .updatedAt(now)
            .queueName("queue1")
            .build();
    var q2Status =
        new WorkflowStatusBuilder("agg-q-2")
            .status(WorkflowState.SUCCESS)
            .workflowName("WorkflowA")
            .appVersion("1.0.0")
            .recoveryAttempts(0)
            .priority(0)
            .createdAt(now)
            .updatedAt(now)
            .queueName("queue1")
            .build();
    var q3Status =
        new WorkflowStatusBuilder("agg-q-3")
            .status(WorkflowState.SUCCESS)
            .workflowName("WorkflowB")
            .appVersion("1.0.0")
            .recoveryAttempts(0)
            .priority(0)
            .createdAt(now)
            .updatedAt(now)
            .queueName("queue2")
            .build();
    sysdb.importWorkflow(
        List.of(
            new ExportedWorkflow(q1Status, List.of(), List.of(), List.of(), List.of()),
            new ExportedWorkflow(q2Status, List.of(), List.of(), List.of(), List.of()),
            new ExportedWorkflow(q3Status, List.of(), List.of(), List.of(), List.of())));

    // Group by queue_name with select_min_created_at (common "oldest item" pattern)
    var rows =
        sysdb.getWorkflowAggregates(
            new GetWorkflowAggregatesInput()
                .withGroupByQueueName(true)
                .withQueueName(List.of("queue1"))
                .withSelectCount(false)
                .withSelectMinCreatedAt(true));
    assertEquals(1, rows.size());
    assertEquals("queue1", rows.get(0).group().get("queue_name"));
    assertNull(rows.get(0).count());
    assertNotNull(rows.get(0).minCreatedAt());
    assertTrue(rows.get(0).minCreatedAt().toEpochMilli() > 0);
  }

  @Test
  public void testGetWorkflowAggregatesCompletedFilters() throws Exception {
    // Workflows imported with completedAt=now for terminal states
    Instant before = Instant.now().minusMillis(60_000);
    sysdb.importWorkflow(
        List.of(
            buildNamedWorkflow("agg-cf-1", "WorkflowA", WorkflowState.SUCCESS),
            buildNamedWorkflow("agg-cf-2", "WorkflowA", WorkflowState.ERROR)));
    Instant after = Instant.now().plusMillis(60_000);

    // Both workflows completed, so completed_after=before and completed_before=after covers them
    var rows =
        sysdb.getWorkflowAggregates(
            new GetWorkflowAggregatesInput()
                .withGroupByStatus(true)
                .withCompletedAfter(before)
                .withCompletedBefore(after));
    // Both should be in range
    long total = rows.stream().mapToLong(r -> r.count()).sum();
    assertEquals(2, total);

    // No match: completed_before=before (before the workflows were created)
    var noRows =
        sysdb.getWorkflowAggregates(
            new GetWorkflowAggregatesInput().withGroupByStatus(true).withCompletedBefore(before));
    assertTrue(noRows.isEmpty());
  }

  @Test
  public void testGetWorkflowAggregatesDequeuedFilters() throws Exception {
    var beforeAll = Instant.now().minusMillis(10_000);

    // 3 sync SUCCESS (started_at=null), 2 sync ERROR (started_at=null)
    for (int i = 0; i < 3; i++) {
      sysdb.importWorkflow(
          List.of(buildNamedWorkflow("agg-dq-ok-" + i, "WorkflowA", WorkflowState.SUCCESS)));
    }
    for (int i = 0; i < 2; i++) {
      sysdb.importWorkflow(
          List.of(buildNamedWorkflow("agg-dq-fail-" + i, "WorkflowA", WorkflowState.ERROR)));
    }
    var afterSync = Instant.now();

    // 1 queued SUCCESS (started_at set strictly after afterSync)
    var queuedStartedAt = afterSync.plusMillis(1);
    var queuedStatus =
        new WorkflowStatusBuilder("agg-dq-queued-1")
            .status(WorkflowState.SUCCESS)
            .workflowName("WorkflowB")
            .appVersion("1.0.0")
            .recoveryAttempts(0)
            .priority(0)
            .createdAt(queuedStartedAt)
            .updatedAt(queuedStartedAt)
            .startedAt(queuedStartedAt)
            .completedAt(queuedStartedAt)
            .build();
    sysdb.importWorkflow(
        List.of(new ExportedWorkflow(queuedStatus, List.of(), List.of(), List.of(), List.of())));
    var afterAll = queuedStartedAt.plusMillis(1);

    // completed_after/completed_before covers all 6
    var rows =
        sysdb.getWorkflowAggregates(
            new GetWorkflowAggregatesInput()
                .withGroupByStatus(true)
                .withWorkflowIdPrefix(List.of("agg-dq-"))
                .withCompletedAfter(beforeAll)
                .withCompletedBefore(afterAll)
                .withSelectCount(true));
    var byStatusSum = rows.stream().mapToLong(r -> r.count()).sum();
    assertEquals(6, byStatusSum);

    // completed_before before all → no match
    var noRows =
        sysdb.getWorkflowAggregates(
            new GetWorkflowAggregatesInput()
                .withGroupByStatus(true)
                .withWorkflowIdPrefix(List.of("agg-dq-"))
                .withCompletedBefore(beforeAll)
                .withSelectCount(true));
    assertTrue(noRows.isEmpty());

    // dequeued_after/dequeued_before: only queued workflow has started_at
    var deqRows =
        sysdb.getWorkflowAggregates(
            new GetWorkflowAggregatesInput()
                .withGroupByStatus(true)
                .withWorkflowIdPrefix(List.of("agg-dq-"))
                .withDequeuedAfter(beforeAll)
                .withDequeuedBefore(afterAll)
                .withSelectCount(true));
    var deqTotal = deqRows.stream().mapToLong(r -> r.count()).sum();
    assertEquals(1, deqTotal);

    // dequeued window before the enqueue → no match
    var noDeq =
        sysdb.getWorkflowAggregates(
            new GetWorkflowAggregatesInput()
                .withGroupByStatus(true)
                .withWorkflowIdPrefix(List.of("agg-dq-"))
                .withDequeuedAfter(beforeAll)
                .withDequeuedBefore(afterSync)
                .withSelectCount(true));
    assertTrue(noDeq.isEmpty());
  }

  @Test
  public void testGetWorkflowAggregatesSelectMaxDurations() throws Exception {
    var now = Instant.now();

    // 2 sync SUCCESS (started_at=null → max_queue_wait_ms will be NULL)
    // Use different names so we can group by name and distinguish sync vs queued
    for (int i = 0; i < 2; i++) {
      var status =
          new WorkflowStatusBuilder("agg-md-sync-" + i)
              .status(WorkflowState.SUCCESS)
              .workflowName("syncWorkflow")
              .appVersion("1.0.0")
              .recoveryAttempts(0)
              .priority(0)
              .createdAt(now)
              .updatedAt(now)
              .completedAt(now)
              .build();
      sysdb.importWorkflow(
          List.of(new ExportedWorkflow(status, List.of(), List.of(), List.of(), List.of())));
    }

    // 2 queued SUCCESS (started_at set → max_queue_wait_ms populated)
    for (int i = 0; i < 2; i++) {
      var status =
          new WorkflowStatusBuilder("agg-md-q-" + i)
              .status(WorkflowState.SUCCESS)
              .workflowName("queuedWorkflow")
              .appVersion("1.0.0")
              .recoveryAttempts(0)
              .priority(0)
              .createdAt(now)
              .updatedAt(now)
              .startedAt(now)
              .completedAt(now)
              .build();
      sysdb.importWorkflow(
          List.of(new ExportedWorkflow(status, List.of(), List.of(), List.of(), List.of())));
    }

    // Only select max_queue_wait_ms + max_total_latency_ms — count must be null
    var results =
        sysdb.getWorkflowAggregates(
            new GetWorkflowAggregatesInput()
                .withGroupByName(true)
                .withSelectCount(false)
                .withSelectMaxQueueWait(true)
                .withSelectMaxTotalLatency(true)
                .withWorkflowIdPrefix(List.of("agg-md-"))
                .withStatus(WorkflowState.SUCCESS));
    assertEquals(2, results.size());
    WorkflowAggregateRow syncRow = null;
    WorkflowAggregateRow queuedRow = null;
    for (var r : results) {
      if ("syncWorkflow".equals(r.group().get("name"))) syncRow = r;
      if ("queuedWorkflow".equals(r.group().get("name"))) queuedRow = r;
    }

    // Sync workflow: count=null, max_queue_wait=null, max_total_latency>=0
    assertNotNull(syncRow);
    assertNull(syncRow.count());
    assertNull(syncRow.maxQueueWait());
    assertNotNull(syncRow.maxTotalLatency());
    assertTrue(syncRow.maxTotalLatency().toMillis() >= 0);

    // Queued workflow: count=null, both maxes populated, total >= wait
    assertNotNull(queuedRow);
    assertNull(queuedRow.count());
    assertNotNull(queuedRow.maxQueueWait());
    assertTrue(queuedRow.maxQueueWait().toMillis() >= 0);
    assertNotNull(queuedRow.maxTotalLatency());
    assertTrue(queuedRow.maxTotalLatency().toMillis() >= queuedRow.maxQueueWait().toMillis());
  }

  // ── Step aggregates ────────────────────────────────────────────────────────

  private static ExportedWorkflow buildWorkflowWithSteps(
      String wfId, String workflowName, WorkflowState state, List<StepInfo> steps) {
    var now = Instant.now();
    var status =
        new WorkflowStatusBuilder(wfId)
            .status(state)
            .workflowName(workflowName)
            .appVersion("1.0.0")
            .recoveryAttempts(0)
            .priority(0)
            .createdAt(now)
            .updatedAt(now)
            .build();
    return new ExportedWorkflow(status, steps, List.of(), List.of(), List.of());
  }

  @Test
  public void testGetStepAggregatesBasic() throws Exception {
    var now = Instant.now();
    var steps =
        List.of(
            new StepInfo(0, "stepA", "ok", null, null, now.minusMillis(10), now, null),
            new StepInfo(1, "stepA", "ok", null, null, now.minusMillis(5), now, null),
            new StepInfo(
                2,
                "stepB",
                null,
                new dev.dbos.transact.workflow.ErrorResult(
                    "Exception", "err", "{\"message\":\"err\"}", null, null),
                null,
                now.minusMillis(3),
                now,
                null));
    sysdb.importWorkflow(
        List.of(buildWorkflowWithSteps("step-agg-wf-1", "WF", WorkflowState.ERROR, steps)));

    // Group by function_name - scope to this test's workflow
    var rows =
        sysdb.getStepAggregates(
            new GetStepAggregatesInput()
                .withGroupByFunctionName(true)
                .withWorkflowIdPrefix(List.of("step-agg-wf-"))
                .withSelectCount(true));
    var byFn =
        rows.stream()
            .collect(Collectors.toMap(r -> r.group().get("function_name"), r -> r.count()));
    assertEquals(2L, byFn.get("stepA"));
    assertEquals(1L, byFn.get("stepB"));

    // Group by status (derived from error IS NULL) - scope to this test's workflow
    var statusRows =
        sysdb.getStepAggregates(
            new GetStepAggregatesInput()
                .withGroupByStatus(true)
                .withWorkflowIdPrefix(List.of("step-agg-wf-"))
                .withSelectCount(true));
    var byStatus =
        statusRows.stream().collect(Collectors.toMap(r -> r.group().get("status"), r -> r.count()));
    assertEquals(2L, byStatus.get("SUCCESS"));
    assertEquals(1L, byStatus.get("ERROR"));

    // Combined group by function_name and status
    var combined =
        sysdb.getStepAggregates(
            new GetStepAggregatesInput()
                .withGroupByFunctionName(true)
                .withGroupByStatus(true)
                .withWorkflowIdPrefix(List.of("step-agg-wf-"))
                .withSelectCount(true));
    assertEquals(2, combined.size());
    for (var r : combined) {
      assertTrue(r.group().containsKey("function_name"));
      assertTrue(r.group().containsKey("status"));
    }
    var byFnStatus =
        combined.stream()
            .collect(
                Collectors.toMap(
                    r -> r.group().get("function_name") + "/" + r.group().get("status"),
                    r -> r.count()));
    assertEquals(2L, byFnStatus.get("stepA/SUCCESS"));
    assertEquals(1L, byFnStatus.get("stepB/ERROR"));
  }

  @Test
  public void testGetStepAggregatesFilters() throws Exception {
    var now = Instant.now();
    var steps =
        List.of(
            new StepInfo(0, "stepX", "ok", null, null, now.minusMillis(10), now, null),
            new StepInfo(
                1,
                "stepY",
                null,
                new dev.dbos.transact.workflow.ErrorResult(
                    "Exception", "err", "{\"message\":\"err\"}", null, null),
                null,
                now.minusMillis(5),
                now,
                null));
    sysdb.importWorkflow(
        List.of(buildWorkflowWithSteps("step-filter-wf-1", "WF", WorkflowState.ERROR, steps)));

    // Filter by function_name - scope to this test's workflow
    var rows =
        sysdb.getStepAggregates(
            new GetStepAggregatesInput()
                .withGroupByFunctionName(true)
                .withWorkflowIdPrefix(List.of("step-filter-wf-"))
                .withFunctionName(List.of("stepX"))
                .withSelectCount(true));
    assertEquals(1, rows.size());
    assertEquals("stepX", rows.get(0).group().get("function_name"));
    assertEquals(1L, rows.get(0).count());

    // Filter by status=ERROR - scope to this test's workflow
    var errRows =
        sysdb.getStepAggregates(
            new GetStepAggregatesInput()
                .withGroupByFunctionName(true)
                .withWorkflowIdPrefix(List.of("step-filter-wf-"))
                .withStatus(GetStepAggregatesInput.Status.ERROR)
                .withSelectCount(true));
    assertEquals(1, errRows.size());
    assertEquals("stepY", errRows.get(0).group().get("function_name"));
  }

  @Test
  public void testGetStepAggregatesIdPrefix() throws Exception {
    var now = Instant.now();
    var step = List.of(new StepInfo(0, "myStep", "ok", null, null, now.minusMillis(5), now, null));
    sysdb.importWorkflow(
        List.of(
            buildWorkflowWithSteps("step-prefix-aaa-1", "WF", WorkflowState.SUCCESS, step),
            buildWorkflowWithSteps("step-prefix-aaa-2", "WF", WorkflowState.SUCCESS, step),
            buildWorkflowWithSteps("step-prefix-bbb-1", "WF", WorkflowState.SUCCESS, step)));

    var rows =
        sysdb.getStepAggregates(
            new GetStepAggregatesInput()
                .withGroupByFunctionName(true)
                .withWorkflowIdPrefix(List.of("step-prefix-aaa"))
                .withSelectCount(true));
    assertEquals(1, rows.size());
    assertEquals(2L, rows.get(0).count());

    var emptyRows =
        sysdb.getStepAggregates(
            new GetStepAggregatesInput()
                .withGroupByFunctionName(true)
                .withWorkflowIdPrefix(List.of("nonexistent"))
                .withSelectCount(true));
    assertTrue(emptyRows.isEmpty());
  }

  @Test
  public void testGetStepAggregatesMaxDuration() throws Exception {
    var now = Instant.now();
    var steps =
        List.of(
            new StepInfo(0, "stepZ", "ok", null, null, now.minusMillis(200), now, null),
            new StepInfo(1, "stepZ", "ok", null, null, now.minusMillis(50), now, null));
    sysdb.importWorkflow(
        List.of(buildWorkflowWithSteps("step-dur-wf-1", "WF", WorkflowState.SUCCESS, steps)));

    var rows =
        sysdb.getStepAggregates(
            new GetStepAggregatesInput()
                .withGroupByFunctionName(true)
                .withSelectCount(false)
                .withSelectMaxDuration(true));
    assertEquals(1, rows.size());
    assertNull(rows.get(0).count());
    assertNotNull(rows.get(0).maxDuration());
    assertTrue(rows.get(0).maxDuration().toMillis() >= 100); // at least the longer step
  }

  @Test
  public void testGetStepAggregatesNoGroupByThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            sysdb.getStepAggregates(
                new GetStepAggregatesInput().withSelectCount(true).withGroupByFunctionName(false)));
  }

  @Test
  public void testGetStepAggregatesNoSelectThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            sysdb.getStepAggregates(
                new GetStepAggregatesInput().withGroupByFunctionName(true).withSelectCount(false)));
  }

  @Test
  public void testGetStepAggregatesTimeBucket() throws Exception {
    var now = Instant.now();
    var step = List.of(new StepInfo(0, "tbStep", "ok", null, null, now.minusMillis(10), now, null));
    sysdb.importWorkflow(
        List.of(buildWorkflowWithSteps("step-tb-wf-1", "WF", WorkflowState.SUCCESS, step)));

    long bucketMs = 3_600_000L;
    var rows =
        sysdb.getStepAggregates(
            new GetStepAggregatesInput()
                .withTimeBucketSize(Duration.ofMillis(bucketMs))
                .withSelectCount(true));
    assertFalse(rows.isEmpty());
    for (var r : rows) {
      assertTrue(r.group().containsKey("time_bucket"));
      long tb = Long.parseLong(r.group().get("time_bucket"));
      assertEquals(0, tb % bucketMs);
    }
  }

  @Test
  public void testGetStepAggregatesTimeBucketInvalidThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new GetStepAggregatesInput().withTimeBucketSize(Duration.ZERO).withSelectCount(true));
  }

  @Test
  public void testGetStepAggregatesCompletedFilters() throws Exception {
    var now = Instant.now();
    var step = List.of(new StepInfo(0, "cfStep", "ok", null, null, now.minusMillis(50), now, null));
    sysdb.importWorkflow(
        List.of(buildWorkflowWithSteps("step-cf-wf-1", "WF", WorkflowState.SUCCESS, step)));

    Instant before = now.minusMillis(60_000);
    Instant after = now.plusMillis(60_000);

    // completed_after=before, completed_before=after covers the step
    var rows =
        sysdb.getStepAggregates(
            new GetStepAggregatesInput()
                .withGroupByFunctionName(true)
                .withCompletedAfter(before)
                .withCompletedBefore(after)
                .withSelectCount(true));
    assertEquals(1, rows.size());
    assertEquals(1L, rows.get(0).count());

    // completed_before=before → no match
    var noRows =
        sysdb.getStepAggregates(
            new GetStepAggregatesInput()
                .withGroupByFunctionName(true)
                .withCompletedBefore(before)
                .withSelectCount(true));
    assertTrue(noRows.isEmpty());
  }

  @Test
  public void testGetStepAggregatesCompletedWindowAndMax() throws Exception {
    var now = Instant.now();

    // Real steps with timestamps
    var quickStep = new StepInfo(0, "quickStep", "ok", null, null, now.minusMillis(20), now, null);
    var slowStep = new StepInfo(1, "slowStep", "ok", null, null, now.minusMillis(100), now, null);

    // Bookkeeping rows (child workflow markers) have NULL timestamps
    var childMarker = new StepInfo(2, "childWorkflow", null, null, "child-wf-id", null, null, null);

    var steps = List.of(quickStep, slowStep, childMarker);
    sysdb.importWorkflow(
        List.of(buildWorkflowWithSteps("step-max-cw-1", "WF", WorkflowState.SUCCESS, steps)));

    var beforeAll = now.minusMillis(10_000);
    var afterAll = now.plusMillis(10_000);

    // completed_after/completed_before window → only real steps appear (bookkeeping
    // rows have NULL completed_at_epoch_ms and are filtered out by WHERE clause)
    var rows =
        sysdb.getStepAggregates(
            new GetStepAggregatesInput()
                .withGroupByFunctionName(true)
                .withCompletedAfter(beforeAll)
                .withCompletedBefore(afterAll)
                .withSelectCount(true)
                .withSelectMaxDuration(true));
    var byFn = rows.stream().collect(Collectors.toMap(r -> r.group().get("function_name"), r -> r));

    // Real steps have count=1 and max_duration populated
    var quickRow = byFn.get("quickStep");
    assertEquals(1L, quickRow.count());
    assertNotNull(quickRow.maxDuration());
    assertTrue(quickRow.maxDuration().toMillis() >= 0);

    var slowRow = byFn.get("slowStep");
    assertEquals(1L, slowRow.count());
    assertNotNull(slowRow.maxDuration());
    assertTrue(slowRow.maxDuration().toMillis() >= 80);

    // Bookkeeping rows are excluded from the completed window
    assertNull(byFn.get("childWorkflow"));

    // Without completed_at filter, bookkeeping rows appear but have null duration
    var allRows =
        sysdb.getStepAggregates(
            new GetStepAggregatesInput()
                .withGroupByFunctionName(true)
                .withSelectCount(true)
                .withSelectMaxDuration(true));
    var allByFn =
        allRows.stream().collect(Collectors.toMap(r -> r.group().get("function_name"), r -> r));
    var childRow = allByFn.get("childWorkflow");
    assertEquals(1L, childRow.count());
    assertNull(childRow.maxDuration());

    // completed_before before all → no match
    var noRows =
        sysdb.getStepAggregates(
            new GetStepAggregatesInput()
                .withGroupByFunctionName(true)
                .withCompletedBefore(beforeAll)
                .withSelectCount(true));
    assertTrue(noRows.isEmpty());

    // select_max_duration_ms alone → count is null
    var maxOnly =
        sysdb.getStepAggregates(
            new GetStepAggregatesInput()
                .withGroupByFunctionName(true)
                .withFunctionName(List.of("quickStep", "slowStep"))
                .withSelectCount(false)
                .withSelectMaxDuration(true));
    for (var r : maxOnly) {
      assertNull(r.count());
      assertNotNull(r.maxDuration());
    }
  }

  // ── F-4: Workflow Data Queries ────────────────────────────────────────────

  @Test
  public void testGetAllEvents() throws Exception {
    var wfId = "get-all-events-wf-1";
    sysdb.importWorkflow(List.of(buildEmptyWorkflow(wfId)));

    sysdb.setEvent(wfId, 0, "key1", "value1", false, null);
    sysdb.setEvent(wfId, 1, "key2", 42, false, null);

    var events = sysdb.getAllEvents(wfId);

    assertEquals(2, events.size());
    assertEquals("value1", events.get("key1"));
    assertEquals(42, events.get("key2"));
  }

  @Test
  public void testGetAllEventsEmpty() throws Exception {
    var wfId = "get-all-events-empty-wf-1";
    sysdb.importWorkflow(List.of(buildEmptyWorkflow(wfId)));

    var events = sysdb.getAllEvents(wfId);
    assertTrue(events.isEmpty());
  }

  @Test
  public void testGetAllNotifications() throws Exception {
    var wfId = "get-all-notifications-wf-1";
    sysdb.importWorkflow(List.of(buildEmptyWorkflow(wfId)));

    sysdb.sendBulk(
        List.of(new SendMessage(wfId, "message1", "topic1", "notif-uuid-1")),
        null,
        -1,
        "DBOS.send",
        false,
        null);
    sysdb.sendBulk(
        List.of(new SendMessage(wfId, "message2", "topic2", "notif-uuid-2")),
        null,
        -1,
        "DBOS.send",
        false,
        null);

    var notifications = sysdb.getAllNotifications(wfId);

    assertEquals(2, notifications.size());
    assertTrue(notifications.stream().anyMatch(n -> "topic1".equals(n.topic())));
    assertTrue(notifications.stream().anyMatch(n -> "topic2".equals(n.topic())));
    notifications.forEach(n -> assertNotNull(n.message()));
    notifications.forEach(n -> assertFalse(n.consumed()));
    notifications.forEach(n -> assertTrue(n.createdAtEpochMs() > 0));
  }

  @Test
  public void testGetAllNotificationsEmpty() throws Exception {
    var wfId = "get-all-notifications-empty-wf-1";
    sysdb.importWorkflow(List.of(buildEmptyWorkflow(wfId)));

    var notifications = sysdb.getAllNotifications(wfId);
    assertTrue(notifications.isEmpty());
  }

  // --- Queue CRUD tests ---

  @Test
  public void testUpsertQueueInsert() {
    var options =
        QueueOptions.setConcurrency(5)
            .andWorkerConcurrency(2)
            .andPriorityEnabled(true)
            .andRateLimit(10, 60, java.util.concurrent.TimeUnit.SECONDS);

    boolean inserted = sysdb.upsertQueue("q-insert", options, true);
    assertTrue(inserted, "upsertQueue should return true when the row is new");

    var fetched = sysdb.findQueue("q-insert");
    assertTrue(fetched.isPresent());
    var q = fetched.get();
    assertEquals("q-insert", q.name());
    assertEquals(5, q.concurrency());
    assertEquals(2, q.workerConcurrency());
    assertTrue(q.priorityEnabled());
    assertNotNull(q.rateLimit());
    assertEquals(10, q.rateLimit().limit());
    assertEquals(Duration.ofSeconds(60), q.rateLimit().period());
  }

  @Test
  public void testUpsertQueueOptionsExisting() {
    sysdb.upsertQueue("q-update", QueueOptions.setConcurrency(3), true);

    boolean inserted =
        sysdb.upsertQueue("q-update", QueueOptions.setConcurrency(7).andWorkerConcurrency(4), true);
    assertFalse(inserted, "upsertQueue should return false when the row already existed");

    var fetched = sysdb.findQueue("q-update").orElseThrow();
    assertEquals(7, fetched.concurrency());
    assertEquals(4, fetched.workerConcurrency());
  }

  @Test
  public void testUpsertQueueNoUpdateExisting() {
    sysdb.upsertQueue("q-no-update", QueueOptions.setConcurrency(3), true);

    boolean inserted = sysdb.upsertQueue("q-no-update", QueueOptions.setConcurrency(99), false);
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
    sysdb.upsertQueue("q-list-a", QueueOptions.setConcurrency(1), true);
    sysdb.upsertQueue("q-list-b", QueueOptions.setConcurrency(2), true);
    sysdb.upsertQueue("q-list-c", QueueOptions.empty(), true);

    var queues = sysdb.listQueues();
    var names = queues.stream().map(Queue::name).toList();
    assertTrue(names.contains("q-list-a"));
    assertTrue(names.contains("q-list-b"));
    assertTrue(names.contains("q-list-c"));
  }

  @Test
  public void testDeleteQueue() {
    sysdb.upsertQueue("q-delete", QueueOptions.setConcurrency(1), true);
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
        QueueOptions.setConcurrency(5)
            .andPriorityEnabled(true)
            .andRateLimit(10, 60, java.util.concurrent.TimeUnit.SECONDS),
        true);

    sysdb.updateQueue("q-partial", QueueOptions.setConcurrency(99));

    var q = sysdb.findQueue("q-partial").orElseThrow();
    assertEquals(99, q.concurrency(), "concurrency should be updated");
    assertTrue(q.priorityEnabled(), "priorityEnabled should be unchanged");
    assertNotNull(q.rateLimit(), "rateLimit should be unchanged");
    assertEquals(10, q.rateLimit().limit());
  }

  @Test
  public void testUpdateQueueClearConcurrency() {
    sysdb.upsertQueue("q-clear-conc", QueueOptions.setConcurrency(5), true);

    sysdb.updateQueue("q-clear-conc", QueueOptions.setConcurrency(null));

    var q = sysdb.findQueue("q-clear-conc").orElseThrow();
    assertNull(q.concurrency(), "concurrency should be cleared to null");
  }

  @Test
  public void testUpdateQueueClearRateLimit() {
    sysdb.upsertQueue(
        "q-clear-rate",
        QueueOptions.setRateLimit(5, 30, java.util.concurrent.TimeUnit.SECONDS),
        true);

    sysdb.updateQueue("q-clear-rate", QueueOptions.setRateLimit(null, null));

    var q = sysdb.findQueue("q-clear-rate").orElseThrow();
    assertNull(q.rateLimit(), "rateLimit should be cleared to null");
  }

  @Test
  public void testUpdateQueueEmpty() {
    sysdb.upsertQueue("q-empty-update", QueueOptions.setConcurrency(5), true);

    // Empty update should be a no-op (no exception, no change)
    var emptyUpdate = QueueOptions.empty();
    sysdb.updateQueue("q-empty-update", emptyUpdate);

    var q = sysdb.findQueue("q-empty-update").orElseThrow();
    assertEquals(5, q.concurrency());
  }

  @Test
  public void testUpsertQueueRoundTrip() {
    sysdb.upsertQueue(
        "q-roundtrip",
        QueueOptions.setConcurrency(8)
            .andWorkerConcurrency(4)
            .andPriorityEnabled(true)
            .andPartitionQueue(true)
            .andRateLimit(20, 30, java.util.concurrent.TimeUnit.SECONDS)
            .andPollingInterval(Duration.ofSeconds(5)),
        true);
    var fetched = sysdb.findQueue("q-roundtrip").orElseThrow();

    assertEquals("q-roundtrip", fetched.name());
    assertEquals(8, fetched.concurrency());
    assertEquals(4, fetched.workerConcurrency());
    assertTrue(fetched.priorityEnabled());
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
    return new DbContext(ds, schema, null, () -> false);
  }

  @Test
  public void testWorkerConcurrencyOnlyUsesReadCommitted() throws SQLException {
    // workerConcurrency only → local in-memory tracking, no global state → READ COMMITTED
    Queue queue = new Queue("iso-wc").withWorkerConcurrency(2);
    var ds = new IsolationRecordingDataSource(dataSource);

    QueuesDAO.startQueuedWorkflows(recordingCtx(ds), queue, "exec", "v1", null, 0);

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

    QueuesDAO.startQueuedWorkflows(recordingCtx(ds), queue, "exec", "v1", null, 0);

    assertEquals(
        Connection.TRANSACTION_REPEATABLE_READ,
        ds.lastIsolationLevel,
        "global-concurrency queue must use REPEATABLE READ");
  }

  @Test
  public void testRateLimitUsesRepeatableRead() throws SQLException {
    // rateLimit → count must be consistent across concurrent dequeue attempts → REPEATABLE READ
    Queue queue = new Queue("iso-rl").withRateLimit(5, Duration.ofSeconds(1));
    var ds = new IsolationRecordingDataSource(dataSource);

    QueuesDAO.startQueuedWorkflows(recordingCtx(ds), queue, "exec", "v1", null, 0);

    assertEquals(
        Connection.TRANSACTION_REPEATABLE_READ,
        ds.lastIsolationLevel,
        "rate-limited queue must use REPEATABLE READ");
  }
}
