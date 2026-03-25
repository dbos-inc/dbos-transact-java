package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.VersionInfo;
import dev.dbos.transact.workflow.WorkflowEvent;
import dev.dbos.transact.workflow.WorkflowEventHistory;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStream;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.time.Instant;
import java.util.List;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT)
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

    sysdb.deleteWorkflows("wfid-1", "wfid-3");

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

    var children = sysdb.getWorkflowChildrenInternal("wfid-2");
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
    sysdb.deleteWorkflows(wfId);
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
}
