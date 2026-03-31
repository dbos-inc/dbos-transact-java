package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.DBOSMaxRecoveryAttemptsExceededException;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.WorkflowStatusBuilder;
import dev.dbos.transact.workflow.ExportedWorkflow;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.VersionInfo;
import dev.dbos.transact.workflow.WorkflowEvent;
import dev.dbos.transact.workflow.WorkflowEventHistory;
import dev.dbos.transact.workflow.WorkflowSchedule;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStream;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class SystemDatabaseTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose SystemDatabase sysdb;

  @AutoClose DBOS dbos;
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

  @Test
  public void testResumeWorkflows() throws Exception {
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

    // Resume all four IDs in one call
    sysdb.resumeWorkflows(List.of("wf-cancelled-1", "wf-cancelled-2", "wf-success", "wf-error"));

    // CANCELLED ones become ENQUEUED
    for (var wfid : List.of("wf-cancelled-1", "wf-cancelled-2")) {
      var row = DBUtils.getWorkflowRow(dataSource, wfid);
      assertEquals(WorkflowState.ENQUEUED.name(), row.status());
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

    sysdb.resumeWorkflows(Arrays.asList("wf-id", null));

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
            .name("wf-name")
            .inputs("wf-inputs")
            .build();

    for (var i = 1; i <= 6; i++) {
      var result1 = sysdb.initWorkflowStatus(status, 5, true, false);
      assertEquals(WorkflowState.PENDING.toString(), result1.status());
      assertEquals(wfid, result1.workflowId());
      assertEquals(0, result1.deadlineEpochMS());

      var row = DBUtils.getWorkflowRow(dataSource, wfid);
      assertNotNull(row);
      assertEquals("PENDING", row.status());
      assertEquals(i, row.recoveryAttempts());
    }

    assertThrows(
        DBOSMaxRecoveryAttemptsExceededException.class,
        () -> sysdb.initWorkflowStatus(status, 5, true, false));
    var row = DBUtils.getWorkflowRow(dataSource, wfid);
    assertNotNull(row);
    assertEquals("MAX_RECOVERY_ATTEMPTS_EXCEEDED", row.status());
    assertEquals(7, row.recoveryAttempts());
  }

  @Test
  public void testDedupeId() throws Exception {
    var wfid = "wfid-1";
    var builder =
        WorkflowStatusInternal.builder(wfid, WorkflowState.PENDING)
            .name("wf-name")
            .inputs("wf-inputs")
            .queueName("queue-name")
            .deduplicationId("dedupe-id");

    var result1 = sysdb.initWorkflowStatus(builder.build(), 5, false, false);
    assertEquals(WorkflowState.PENDING.toString(), result1.status());
    assertEquals(wfid, result1.workflowId());
    assertEquals(0, result1.deadlineEpochMS());

    var before = DBUtils.getWorkflowRow(dataSource, wfid);
    assertThrows(
        DBOSQueueDuplicatedException.class,
        () -> sysdb.initWorkflowStatus(builder.workflowId("wfid-2").build(), 5, false, false));
    var after = DBUtils.getWorkflowRow(dataSource, wfid);

    assertTrue(before.equals(after));
  }

  private static ExportedWorkflow buildExportedWorkflow(String wfId) {
    long now = System.currentTimeMillis();
    var status =
        new WorkflowStatusBuilder(wfId)
            .status(WorkflowState.SUCCESS)
            .name("TestWorkflow")
            .appVersion("1.0.0")
            .recoveryAttempts(0)
            .priority(0)
            .createdAt(now)
            .updatedAt(now)
            .build();

    var steps =
        List.of(
            new StepInfo(0, "step0", "output0", null, null, 1000L, 2000L, null),
            new StepInfo(1, "step1", "output1", null, null, 2000L, 3000L, null));

    var events =
        List.of(
            new WorkflowEvent("event-key-1", "event-val-1", null),
            new WorkflowEvent("event-key-2", "event-val-2", null));

    var eventHistory =
        List.of(
            new WorkflowEventHistory("event-key-1", "history-val-1", 0, null),
            new WorkflowEventHistory("event-key-2", "history-val-2", 1, null));

    var streams =
        List.of(
            new WorkflowStream("stream-key-1", "stream-val-1", 0, 0, null),
            new WorkflowStream("stream-key-1", "stream-val-2", 1, 1, null));

    return new ExportedWorkflow(status, steps, events, eventHistory, streams);
  }

  @Test
  public void testImportWorkflow() throws Exception {
    var wfId = "import-wf-1";
    sysdb.importWorkflow(List.of(buildExportedWorkflow(wfId)));

    var wfRows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(1, wfRows.size());
    assertEquals(wfId, wfRows.get(0).workflowId());
    assertEquals("SUCCESS", wfRows.get(0).status());

    var stepRows = DBUtils.getStepRows(dataSource, wfId);
    assertEquals(2, stepRows.size());

    var eventRows = DBUtils.getWorkflowEvents(dataSource, wfId);
    assertEquals(2, eventRows.size());
    assertTrue(
        eventRows.stream()
            .anyMatch(e -> e.key().equals("event-key-1") && e.value().equals("event-val-1")));
    assertTrue(
        eventRows.stream()
            .anyMatch(e -> e.key().equals("event-key-2") && e.value().equals("event-val-2")));

    var historyRows = DBUtils.getWorkflowEventHistory(dataSource, wfId);
    assertEquals(2, historyRows.size());
  }

  @Test
  public void testImportMultipleWorkflows() throws Exception {
    sysdb.importWorkflow(
        List.of(
            buildExportedWorkflow("import-multi-wf-1"),
            buildExportedWorkflow("import-multi-wf-2"),
            buildExportedWorkflow("import-multi-wf-3")));

    var wfRows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(3, wfRows.size());
    assertTrue(wfRows.stream().anyMatch(r -> r.workflowId().equals("import-multi-wf-1")));
    assertTrue(wfRows.stream().anyMatch(r -> r.workflowId().equals("import-multi-wf-2")));
    assertTrue(wfRows.stream().anyMatch(r -> r.workflowId().equals("import-multi-wf-3")));

    for (var wfId : List.of("import-multi-wf-1", "import-multi-wf-2", "import-multi-wf-3")) {
      assertEquals(2, DBUtils.getStepRows(dataSource, wfId).size());
      assertEquals(2, DBUtils.getWorkflowEvents(dataSource, wfId).size());
    }
  }

  @Test
  public void testExportWorkflow() throws Exception {
    var wfId = "export-wf-1";
    sysdb.importWorkflow(List.of(buildExportedWorkflow(wfId)));

    var exported = sysdb.exportWorkflow(wfId, false);

    assertEquals(1, exported.size());
    var wf = exported.get(0);
    assertEquals(wfId, wf.status().workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wf.status().status());
    assertEquals("TestWorkflow", wf.status().name());

    assertEquals(2, wf.steps().size());
    assertTrue(wf.steps().stream().anyMatch(s -> s.functionName().equals("step0")));
    assertTrue(wf.steps().stream().anyMatch(s -> s.functionName().equals("step1")));

    assertEquals(2, wf.events().size());
    assertTrue(wf.events().stream().anyMatch(e -> e.key().equals("event-key-1")));

    assertEquals(2, wf.eventHistory().size());

    assertEquals(2, wf.streams().size());
    assertTrue(wf.streams().stream().anyMatch(s -> s.key().equals("stream-key-1")));
  }

  private static ExportedWorkflow buildExportedWorkflowWithChildren(
      String wfId, String child1Id, String child2Id) {
    long now = System.currentTimeMillis();
    var status =
        new WorkflowStatusBuilder(wfId)
            .status(WorkflowState.SUCCESS)
            .name("TestWorkflow")
            .appVersion("1.0.0")
            .recoveryAttempts(0)
            .priority(0)
            .createdAt(now)
            .updatedAt(now)
            .build();

    // Steps reference child workflows via childWorkflowId
    var steps =
        List.of(
            new StepInfo(0, "step0", null, null, child1Id, 1000L, 2000L, null),
            new StepInfo(1, "step1", null, null, child2Id, 2000L, 3000L, null));

    return new ExportedWorkflow(status, steps, List.of(), List.of(), List.of());
  }

  @Test
  public void testExportWorkflowWithoutChildren() throws Exception {
    var parentId = "export-no-children-parent";
    var child1Id = "export-no-children-child-1";
    var child2Id = "export-no-children-child-2";

    sysdb.importWorkflow(
        List.of(
            buildExportedWorkflowWithChildren(parentId, child1Id, child2Id),
            buildExportedWorkflow(child1Id),
            buildExportedWorkflow(child2Id)));

    var result = sysdb.exportWorkflow(parentId, false);
    assertEquals(1, result.size());
    assertEquals(parentId, result.get(0).status().workflowId());
  }

  @Test
  public void testExportWorkflowWithChildren() throws Exception {
    var parentId = "export-children-parent";
    var child1Id = "export-children-child-1";
    var child2Id = "export-children-child-2";

    sysdb.importWorkflow(
        List.of(
            buildExportedWorkflowWithChildren(parentId, child1Id, child2Id),
            buildExportedWorkflow(child1Id),
            buildExportedWorkflow(child2Id)));

    var result = sysdb.exportWorkflow(parentId, true);
    assertEquals(3, result.size());
    assertTrue(result.stream().anyMatch(w -> w.status().workflowId().equals(parentId)));
    assertTrue(result.stream().anyMatch(w -> w.status().workflowId().equals(child1Id)));
    assertTrue(result.stream().anyMatch(w -> w.status().workflowId().equals(child2Id)));
  }

  @Test
  public void testImportExportRoundTrip() throws Exception {
    var wfId = "roundtrip-wf-1";
    var original = buildExportedWorkflow(wfId);
    sysdb.importWorkflow(List.of(original));

    var exported = sysdb.exportWorkflow(wfId, false);
    assertEquals(1, exported.size());

    // Delete the original and reimport from the exported data
    sysdb.deleteWorkflows(List.of(wfId), false);
    assertEquals(0, DBUtils.getWorkflowRows(dataSource).size());

    sysdb.importWorkflow(exported);

    var reimported = sysdb.exportWorkflow(wfId, false);
    assertEquals(1, reimported.size());
    var wf = reimported.get(0);
    assertEquals(wfId, wf.status().workflowId());
    assertEquals(2, wf.steps().size());
    assertEquals(2, wf.events().size());
    assertEquals(2, wf.eventHistory().size());
    assertEquals(2, wf.streams().size());
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
    assertEquals("sched-1", s.name());
    assertEquals("myWorkflow", s.workflowName());
    assertEquals("com.example.MyClass", s.className());
    assertEquals("0 * * * *", s.schedule());
    assertEquals(ScheduleStatus.ACTIVE, s.status());
    assertEquals("{}", s.context());
    assertNull(s.lastFiredAt());
    assertFalse(s.automaticBackfill());
    assertNull(s.cronTimezone());
    assertNull(s.queueName());
    assertNotNull(s.scheduleId());
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
    assertEquals("beta-1", paused.get(0).name());

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
    assertTrue(byPrefix.stream().allMatch(s -> s.name().startsWith("alpha-")));

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
    assertEquals("my-id-123", s.scheduleId());
    assertEquals("sched-full", s.name());
    assertEquals("fullWorkflow", s.workflowName());
    assertEquals("com.example.Full", s.className());
    assertEquals("*/5 * * * *", s.schedule());
    assertEquals("{\"key\":\"val\"}", s.context());
    assertEquals(Instant.parse("2026-03-01T00:00:00Z"), s.lastFiredAt());
    assertTrue(s.automaticBackfill());
    assertEquals("America/New_York", s.cronTimezone());
    assertEquals("my-queue", s.queueName());
  }
}
