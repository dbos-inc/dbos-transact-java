package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.DBOSMaxRecoveryAttemptsExceededException;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.WorkflowStatusBuilder;
import dev.dbos.transact.workflow.ExportedWorkflow;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.GetWorkflowAggregatesInput;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.VersionInfo;
import dev.dbos.transact.workflow.WorkflowSchedule;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
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
      var status = WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING).build();
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
      var status = WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING).build();
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
      sysdb.initWorkflowStatus(
          WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING).build(), 5, false, false);
    }
    sysdb.initWorkflowStatus(
        WorkflowStatusInternal.builder("wf-success", WorkflowState.PENDING).build(),
        5,
        false,
        false);
    sysdb.initWorkflowStatus(
        WorkflowStatusInternal.builder("wf-error", WorkflowState.PENDING).build(), 5, false, false);
    DBUtils.setWorkflowState(dataSource, "wf-success", WorkflowState.SUCCESS.name());
    DBUtils.setWorkflowState(dataSource, "wf-error", WorkflowState.ERROR.name());

    // Cancel all five IDs in one call
    sysdb.cancelWorkflows(
        List.of("wf-pending-1", "wf-pending-2", "wf-pending-3", "wf-success", "wf-error"));

    // PENDING ones become CANCELLED
    for (var wfid : List.of("wf-pending-1", "wf-pending-2", "wf-pending-3")) {
      var row = DBUtils.getWorkflowRow(dataSource, wfid);
      assertNotNull(row);
      assertEquals(WorkflowState.CANCELLED.name(), row.status());
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
      sysdb.initWorkflowStatus(
          WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING).build(), 5, false, false);
      DBUtils.setWorkflowState(dataSource, wfid, WorkflowState.CANCELLED.name());
    }
    sysdb.initWorkflowStatus(
        WorkflowStatusInternal.builder("wf-success", WorkflowState.PENDING).build(),
        5,
        false,
        false);
    sysdb.initWorkflowStatus(
        WorkflowStatusInternal.builder("wf-error", WorkflowState.PENDING).build(), 5, false, false);
    DBUtils.setWorkflowState(dataSource, "wf-success", WorkflowState.SUCCESS.name());
    DBUtils.setWorkflowState(dataSource, "wf-error", WorkflowState.ERROR.name());

    return List.of("wf-cancelled-1", "wf-cancelled-2", "wf-success", "wf-error");
  }

  @Test
  public void testResumeWorkflows() throws Exception {

    var workflowIds = insertResumableWorkflows();

    // Resume all four IDs in one call
    sysdb.resumeWorkflows(workflowIds, null);

    // CANCELLED ones become ENQUEUED
    for (var wfid : List.of("wf-cancelled-1", "wf-cancelled-2")) {
      var row = DBUtils.getWorkflowRow(dataSource, wfid);
      assertEquals(WorkflowState.ENQUEUED.name(), row.status());
      assertEquals(Constants.DBOS_INTERNAL_QUEUE, row.queueName());
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

    // Resume all four IDs in one call
    sysdb.resumeWorkflows(workflowIds, "customQueue");

    // CANCELLED ones become ENQUEUED
    for (var wfid : List.of("wf-cancelled-1", "wf-cancelled-2")) {
      var row = DBUtils.getWorkflowRow(dataSource, wfid);
      assertEquals(WorkflowState.ENQUEUED.name(), row.status());
      assertEquals("customQueue", row.queueName());
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
        WorkflowStatusInternal.builder("wf-id", WorkflowState.PENDING).build(), 5, false, false);

    sysdb.cancelWorkflows(Arrays.asList("wf-id", null));

    assertEquals(
        WorkflowState.CANCELLED.name(), DBUtils.getWorkflowRow(dataSource, "wf-id").status());
  }

  @Test
  public void testResumeWorkflowsNullInList() throws Exception {
    sysdb.initWorkflowStatus(
        WorkflowStatusInternal.builder("wf-id", WorkflowState.PENDING).build(), 5, false, false);
    DBUtils.setWorkflowState(dataSource, "wf-id", WorkflowState.CANCELLED.name());

    sysdb.resumeWorkflows(Arrays.asList("wf-id", null), null);

    assertEquals(
        WorkflowState.ENQUEUED.name(), DBUtils.getWorkflowRow(dataSource, "wf-id").status());
  }

  @Test
  public void testDeleteWorkflowsNullInList() throws Exception {
    sysdb.initWorkflowStatus(
        WorkflowStatusInternal.builder("wf-id", WorkflowState.PENDING).build(), 5, false, false);

    sysdb.deleteWorkflows(Arrays.asList("wf-id", null), false);

    assertNull(DBUtils.getWorkflowRow(dataSource, "wf-id"));
  }

  @Test
  public void testGetChildWorkflows() throws Exception {
    for (var i = 0; i < 5; i++) {
      var wfid = "wfid-%d".formatted(i);
      var status = WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING).build();
      sysdb.initWorkflowStatus(status, 5, false, false);
    }

    for (var i = 0; i < 5; i++) {
      var parentWfId = "wfid-2";
      var wfid = "childwfid-%d".formatted(i);
      var status = WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING).build();
      sysdb.initWorkflowStatus(status, 5, false, false);
      sysdb.recordChildWorkflow(
          parentWfId, wfid, i, "step-%d".formatted(i), System.currentTimeMillis());
    }

    for (var i = 0; i < 5; i++) {
      var parentWfId = "childwfid-%d".formatted(i);
      var wfid = "grandchildwfid-%d".formatted(i);
      var status = WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING).build();
      sysdb.initWorkflowStatus(status, 5, false, false);
      sysdb.recordChildWorkflow(
          parentWfId, wfid, i, "step-%d".formatted(i), System.currentTimeMillis());
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
        WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING)
            .workflowName("wf-name")
            .inputs("wf-inputs")
            .build();

    for (var i = 1; i <= 6; i++) {
      var result1 = sysdb.initWorkflowStatus(status, 5, true, false);
      assertEquals(WorkflowState.PENDING.name(), result1.status());
      assertEquals(wfid, result1.workflowId());
      assertEquals(0, result1.deadlineEpochMS());

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
        WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING)
            .workflowName("wf-name")
            .inputs("wf-inputs")
            .queueName("queue-name")
            .deduplicationId("dedupe-id");

    var result1 = sysdb.initWorkflowStatus(builder.build(), 5, false, false);
    assertEquals(WorkflowState.PENDING.name(), result1.status());
    assertEquals(wfid, result1.workflowId());
    assertEquals(0, result1.deadlineEpochMS());

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
        "0 * * * *",
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
    assertEquals("0 * * * *", s.cron());
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
    var schedule =
        new WorkflowSchedule(
            null,
            "sched-null-status",
            "myWorkflow",
            "com.example.MyClass",
            "0 * * * *",
            null,
            "{}",
            null,
            false,
            null,
            null);
    assertThrows(NullPointerException.class, () -> sysdb.createSchedule(schedule));
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
            "0 * * * *",
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
            "*/5 * * * *",
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
    assertEquals("*/5 * * * *", s.cron());
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
        WorkflowStatusInternal.builder(workflowId, WorkflowState.PENDING)
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
    assertArrayEquals(authenticatedRoles, retrievedStatus.authenticatedRoles());

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
        WorkflowStatusInternal.builder(workflowId, WorkflowState.PENDING)
            .workflowName("TestNullAuthWorkflow")
            .className("com.example.TestNullAuthWorkflow")
            .authenticatedUser(null)
            .assumedRole(null)
            .authenticatedRoles(null)
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
        WorkflowStatusInternal.builder(workflowId, WorkflowState.PENDING)
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
    assertEquals(0, retrievedStatus.authenticatedRoles().length);

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
        WorkflowStatusInternal.builder(originalWorkflowId, WorkflowState.SUCCESS)
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
    assertArrayEquals(authenticatedRoles, originalRetrieved.authenticatedRoles());

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
    assertArrayEquals(authenticatedRoles, forkedStatus.authenticatedRoles());

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
        WorkflowStatusInternal.builder(originalWorkflowId, WorkflowState.SUCCESS)
            .workflowName("NullAuthTestWorkflow")
            .className("com.example.NullAuthTestWorkflow")
            .authenticatedUser(null)
            .assumedRole(null)
            .authenticatedRoles(null)
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
        WorkflowStatusInternal.builder(originalWorkflowId, WorkflowState.SUCCESS)
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
    assertEquals(0, originalRetrieved.authenticatedRoles().length);

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
    assertEquals(0, forkedStatus.authenticatedRoles().length);

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
    var status = WorkflowStatusInternal.builder(workflowId, WorkflowState.PENDING).build();
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
    var status = WorkflowStatusInternal.builder(workflowId, WorkflowState.PENDING).build();
    sysdb.initWorkflowStatus(status, 5, false, false);
    int functionId = 1;

    sysdb.writeStreamFromWorkflow(workflowId, functionId, "key1", "value1", "portable_json");

    Object result = sysdb.readStream(workflowId, "key1", 0);
    assertEquals("value1", result);
  }

  @Test
  public void testCloseStream() throws Exception {
    String workflowId = "stream-wf-3";
    var status = WorkflowStatusInternal.builder(workflowId, WorkflowState.PENDING).build();
    sysdb.initWorkflowStatus(status, 5, false, false);

    sysdb.writeStreamFromWorkflow(workflowId, 1, "key1", "value1", "portable_json");
    sysdb.closeStream(workflowId, 2, "key1");

    assertThrows(IllegalStateException.class, () -> sysdb.readStream(workflowId, "key1", 1));
  }

  @Test
  public void testGetAllStreamEntries() throws Exception {
    String workflowId = "stream-wf-4";
    var status = WorkflowStatusInternal.builder(workflowId, WorkflowState.PENDING).build();
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
    var status = WorkflowStatusInternal.builder(workflowId, WorkflowState.PENDING).build();
    sysdb.initWorkflowStatus(status, 5, false, false);

    assertThrows(IllegalArgumentException.class, () -> sysdb.readStream(workflowId, "key", 0));
  }

  public void testInsertWorkflowStatusValidation() throws Exception {
    // Test null workflowId
    assertThrows(
        NullPointerException.class,
        () -> {
          var status = WorkflowStatusInternal.builder(null, WorkflowState.PENDING).build();
          sysdb.initWorkflowStatus(status, null, false, false);
        });

    // Test null status
    assertThrows(
        NullPointerException.class,
        () -> {
          var status = WorkflowStatusInternal.builder("test-wf-id", null).build();
          sysdb.initWorkflowStatus(status, null, false, false);
        });
  }

  @Test
  public void testInsertWorkflowStatusEmptyStringValidation() throws Exception {
    // Test empty workflowName
    assertThrows(
        IllegalStateException.class,
        () -> {
          var status =
              WorkflowStatusInternal.builder("test-wf-1", WorkflowState.PENDING)
                  .workflowName("")
                  .build();
          sysdb.initWorkflowStatus(status, null, false, false);
        });

    // Test empty className
    assertThrows(
        IllegalStateException.class,
        () -> {
          var status =
              WorkflowStatusInternal.builder("test-wf-2", WorkflowState.PENDING)
                  .className("")
                  .build();
          sysdb.initWorkflowStatus(status, null, false, false);
        });

    // Test empty instanceName
    assertThrows(
        IllegalStateException.class,
        () -> {
          var status =
              WorkflowStatusInternal.builder("test-wf-3", WorkflowState.PENDING)
                  .instanceName("")
                  .build();
          sysdb.initWorkflowStatus(status, null, false, false);
        });

    // Test empty queueName
    assertThrows(
        IllegalStateException.class,
        () -> {
          var status =
              WorkflowStatusInternal.builder("test-wf-4", WorkflowState.PENDING)
                  .queueName("")
                  .build();
          sysdb.initWorkflowStatus(status, null, false, false);
        });

    // Test empty deduplicationId
    assertThrows(
        IllegalStateException.class,
        () -> {
          var status =
              WorkflowStatusInternal.builder("test-wf-5", WorkflowState.PENDING)
                  .deduplicationId("")
                  .build();
          sysdb.initWorkflowStatus(status, null, false, false);
        });

    // Test empty queuePartitionKey
    assertThrows(
        IllegalStateException.class,
        () -> {
          var status =
              WorkflowStatusInternal.builder("test-wf-6", WorkflowState.PENDING)
                  .queuePartitionKey("")
                  .build();
          sysdb.initWorkflowStatus(status, null, false, false);
        });
  }

  @Test
  public void testInsertWorkflowStatusValidNullValues() throws Exception {
    // Test that null values (except for required fields) are allowed
    var status =
        WorkflowStatusInternal.builder("test-valid-nulls", WorkflowState.PENDING)
            .workflowName(null)
            .className(null)
            .instanceName(null)
            .queueName(null)
            .deduplicationId(null)
            .queuePartitionKey(null)
            .build();

    // This should not throw an exception
    var result = sysdb.initWorkflowStatus(status, null, false, false);
    assertEquals("test-valid-nulls", result.workflowId());
    assertEquals(WorkflowState.PENDING.name(), result.status());
  }

  @Test
  public void testInsertWorkflowStatusValidNonEmptyValues() throws Exception {
    // Test that non-empty values are allowed
    var status =
        WorkflowStatusInternal.builder("test-valid-values", WorkflowState.PENDING)
            .workflowName("TestWorkflow")
            .className("com.example.TestWorkflow")
            .instanceName("test-instance")
            .queueName("test-queue")
            .deduplicationId("dedupe-123")
            .queuePartitionKey("partition-key")
            .build();

    // This should not throw an exception
    var result = sysdb.initWorkflowStatus(status, null, false, false);
    assertEquals("test-valid-values", result.workflowId());
    assertEquals(WorkflowState.PENDING.name(), result.status());

    // Verify the values were stored correctly
    var retrievedStatus = sysdb.getWorkflowStatus("test-valid-values");
    assertEquals("TestWorkflow", retrievedStatus.workflowName());
    assertEquals("com.example.TestWorkflow", retrievedStatus.className());
    assertEquals("test-instance", retrievedStatus.instanceName());
  }

  private static ExportedWorkflow buildEmptyWorkflow(String wfId) {
    return buildNamedWorkflow(wfId, "TestWorkflow", WorkflowState.SUCCESS);
  }

  private static ExportedWorkflow buildNamedWorkflow(
      String wfId, String workflowName, WorkflowState state) {
    long now = System.currentTimeMillis();
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
    return new ExportedWorkflow(status, List.of(), List.of(), List.of(), List.of());
  }

  // ── F-11: Workflow Aggregates ─────────────────────────────────────────────

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

    sysdb.sendDirect(wfId, "message1", "topic1", "notif-uuid-1", null);
    sysdb.sendDirect(wfId, "message2", "topic2", "notif-uuid-2", null);

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
}
