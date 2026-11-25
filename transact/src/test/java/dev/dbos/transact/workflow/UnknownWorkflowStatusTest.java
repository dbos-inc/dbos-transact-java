package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.utils.WorkflowStatusBuilder;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A status this version does not know, such as one a newer version wrote, reads as {@link
 * WorkflowState#UNKNOWN} with its stored value, instead of failing the read.
 */
class UnknownWorkflowStatusTest {

  private static final String NEWER = "SOMETHING_NEWER";

  public interface SimpleService {
    String work();
  }

  public static class SimpleServiceImpl implements SimpleService {
    @Override
    @Workflow
    public String work() {
      return "done";
    }
  }

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  SimpleService proxy;

  @BeforeEach
  void beforeEach() {
    dataSource = pgContainer.dataSource();
    dbos = new DBOS(pgContainer.dbosConfig());
    proxy = dbos.registerProxy(SimpleService.class, new SimpleServiceImpl());
    dbos.launch();
  }

  /** Runs a workflow, then gives its row a status this version does not know. */
  private String workflowWithNewerStatus() throws Exception {
    var workflowId = "newer-status";
    try (var _o = new WorkflowOptions(workflowId).setContext()) {
      assertEquals("done", proxy.work());
    }
    DBUtils.setWorkflowState(dataSource, workflowId, NEWER);
    return workflowId;
  }

  @Test
  void decodesAnUnknownStatusValue() {
    assertEquals(WorkflowState.SUSPENDED, WorkflowState.fromStatusValue("SUSPENDED"));
    assertEquals(WorkflowState.UNKNOWN, WorkflowState.fromStatusValue(NEWER));
    // UNKNOWN is never written, so a stored "UNKNOWN" is just another status this version lacks.
    assertEquals(WorkflowState.UNKNOWN, WorkflowState.fromStatusValue("UNKNOWN"));
    assertFalse(WorkflowState.UNKNOWN.isActive());
    assertFalse(WorkflowState.UNKNOWN.isTerminal());
    // Every other state is exactly one of the two.
    for (var state : WorkflowState.values()) {
      if (state != WorkflowState.UNKNOWN) {
        assertTrue(state.isActive() != state.isTerminal(), state.name());
      }
    }
    assertFalse(WorkflowState.activeStates().contains(WorkflowState.UNKNOWN));
    assertFalse(WorkflowState.knownNames().contains("UNKNOWN"));
  }

  @Test
  void listsAndReadsAWorkflowWithAnUnknownStatus() throws Exception {
    var workflowId = workflowWithNewerStatus();

    var status = dbos.getWorkflowStatus(workflowId).orElseThrow();
    assertEquals(WorkflowState.UNKNOWN, status.status());
    assertEquals(NEWER, status.statusValue());

    var listed = dbos.listWorkflows(new ListWorkflowsInput().withWorkflowIds(workflowId));
    assertEquals(1, listed.size());
    assertEquals(WorkflowState.UNKNOWN, listed.get(0).status());
    assertEquals(NEWER, listed.get(0).statusValue());

    // A known status carries its own name.
    try (var _o = new WorkflowOptions("known-status").setContext()) {
      proxy.work();
    }
    var known = dbos.getWorkflowStatus("known-status").orElseThrow();
    assertEquals(WorkflowState.SUCCESS, known.status());
    assertEquals(WorkflowState.SUCCESS.name(), known.statusValue());
  }

  @Test
  void filtersByAnUnknownStatus() throws Exception {
    var workflowId = workflowWithNewerStatus();

    assertEquals(
        List.of(workflowId),
        ids(dbos.listWorkflows(new ListWorkflowsInput().withStatus(WorkflowState.UNKNOWN))));
    assertEquals(
        List.of(),
        ids(dbos.listWorkflows(new ListWorkflowsInput().withStatus(WorkflowState.SUCCESS))));
    // UNKNOWN combines with known states: a workflow matching any of them is listed.
    assertEquals(
        List.of(workflowId),
        ids(
            dbos.listWorkflows(
                new ListWorkflowsInput()
                    .withStatus(WorkflowState.ERROR, WorkflowState.UNKNOWN)
                    .withWorkflowIds(workflowId))));
  }

  @Test
  void waitsOnAWorkflowWithAnUnknownStatus() throws Exception {
    var workflowId = workflowWithNewerStatus();

    // The result wait doesn't throw on the status, and doesn't take it as finished: it keeps
    // polling, and returns once the row reads as finished.
    var result =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return dbos.<String, RuntimeException>retrieveWorkflow(workflowId).getResult();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
    assertThrows(TimeoutException.class, () -> result.get(1, TimeUnit.SECONDS));
    DBUtils.setWorkflowState(dataSource, workflowId, WorkflowState.SUCCESS.name());
    assertEquals("done", result.get(10, TimeUnit.SECONDS));
  }

  @Test
  void aQueuesOnlyListingNamingAStatusSkipsTheActiveDefault() throws Exception {
    var workflowId = workflowWithNewerStatus();
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "UPDATE dbos.workflow_status SET queue_name = 'some-queue' WHERE workflow_uuid = ?")) {
      stmt.setString(1, workflowId);
      assertEquals(1, stmt.executeUpdate());
    }

    // Without a status filter, a queues-only listing lists only active workflows.
    assertEquals(List.of(), ids(dbos.listWorkflows(new ListWorkflowsInput().withQueuesOnly(true))));
    assertEquals(
        List.of(workflowId),
        ids(
            dbos.listWorkflows(
                new ListWorkflowsInput().withQueuesOnly(true).withStatus(WorkflowState.UNKNOWN))));
  }

  @Test
  void refusesToRewindAWorkflowWithAnUnknownStatus() throws Exception {
    var workflowId = workflowWithNewerStatus();

    var refused =
        assertThrows(IllegalStateException.class, () -> dbos.rewindWorkflow(workflowId, 0));
    assertTrue(refused.getMessage().contains(workflowId), refused.getMessage());
    assertEquals(NEWER, dbos.getWorkflowStatus(workflowId).orElseThrow().statusValue());
  }

  @Test
  void aStatusAlwaysComesWithAMatchingStatusValue() {
    var builder = new WorkflowStatusBuilder("wf");
    assertEquals("SUCCESS", builder.status(WorkflowState.SUCCESS).build().statusValue());
    assertThrows(
        IllegalArgumentException.class, () -> builder.status(WorkflowState.UNKNOWN).build());
    assertThrows(
        IllegalArgumentException.class,
        () -> builder.status(WorkflowState.SUCCESS).statusValue("ERROR").build());
    assertThrows(
        IllegalArgumentException.class,
        () -> builder.status(WorkflowState.UNKNOWN).statusValue("SUCCESS").build());
    assertEquals(
        NEWER, builder.status(WorkflowState.UNKNOWN).statusValue(NEWER).build().statusValue());

    // A value alone gives the state it names; neither stays neither.
    assertEquals(
        WorkflowState.SUCCESS,
        new WorkflowStatusBuilder("wf").statusValue("SUCCESS").build().status());
    assertEquals(
        WorkflowState.UNKNOWN, new WorkflowStatusBuilder("wf").statusValue(NEWER).build().status());
    var neither = new WorkflowStatusBuilder("wf").build();
    assertNull(neither.status());
    assertNull(neither.statusValue());
  }

  private static List<String> ids(List<WorkflowStatus> statuses) {
    return statuses.stream().map(WorkflowStatus::workflowId).toList();
  }
}
