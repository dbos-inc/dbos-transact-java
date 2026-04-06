package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.migrations.MigrationManager;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.WorkflowStatusBuilder;
import dev.dbos.transact.workflow.ExportedWorkflow;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowEvent;
import dev.dbos.transact.workflow.WorkflowEventHistory;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStream;

import java.util.List;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class ImportExportTest {

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

  private static ExportedWorkflow buildExportedWorkflow(String wfId) {
    long now = System.currentTimeMillis();
    var status =
        new WorkflowStatusBuilder(wfId)
            .status(WorkflowState.SUCCESS)
            .workflowName("TestWorkflow")
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

  private static ExportedWorkflow buildExportedWorkflowWithChildren(
      String wfId, String child1Id, String child2Id) {
    long now = System.currentTimeMillis();
    var status =
        new WorkflowStatusBuilder(wfId)
            .status(WorkflowState.SUCCESS)
            .workflowName("TestWorkflow")
            .appVersion("1.0.0")
            .recoveryAttempts(0)
            .priority(0)
            .createdAt(now)
            .updatedAt(now)
            .build();

    var steps =
        List.of(
            new StepInfo(0, "step0", null, null, child1Id, 1000L, 2000L, null),
            new StepInfo(1, "step1", null, null, child2Id, 2000L, 3000L, null));

    return new ExportedWorkflow(status, steps, List.of(), List.of(), List.of());
  }

  @Test
  public void testImportWorkflow() throws Exception {
    var wfId = "import-wf-1";
    sysdb.importWorkflow(List.of(buildExportedWorkflow(wfId)));

    var wfRows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(1, wfRows.size());
    assertEquals(wfId, wfRows.get(0).workflowId());
    assertEquals(WorkflowState.SUCCESS.name(), wfRows.get(0).status());

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

    var streamRows = DBUtils.getStreamEntries(dataSource, wfId);
    assertEquals(2, streamRows.size());
    assertTrue(streamRows.stream().anyMatch(s -> s.key().equals("stream-key-1") && s.offset() == 0));
    assertTrue(streamRows.stream().anyMatch(s -> s.key().equals("stream-key-1") && s.offset() == 1));
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
      assertEquals(2, DBUtils.getStreamEntries(dataSource, wfId).size());
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
    assertEquals(WorkflowState.SUCCESS, wf.status().status());
    assertEquals("TestWorkflow", wf.status().workflowName());

    assertEquals(2, wf.steps().size());
    assertTrue(wf.steps().stream().anyMatch(s -> s.functionName().equals("step0")));
    assertTrue(wf.steps().stream().anyMatch(s -> s.functionName().equals("step1")));

    assertEquals(2, wf.events().size());
    assertTrue(wf.events().stream().anyMatch(e -> e.key().equals("event-key-1")));

    assertEquals(2, wf.eventHistory().size());

    assertEquals(2, wf.streams().size());
    assertTrue(wf.streams().stream().anyMatch(s -> s.key().equals("stream-key-1")));
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
}
