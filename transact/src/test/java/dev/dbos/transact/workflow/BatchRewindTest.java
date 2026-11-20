package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.WorkflowOptions;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.internal.DBOSEnterpriseEngine;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.spi.EnterpriseEngine;
import dev.dbos.transact.spi.WorkflowRewind;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** Rewinding a batch of workflows through the enterprise contract, with no step factories. */
public class BatchRewindTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;

  private RewindTestServiceImpl impl;
  private RewindTestService proxy;
  private EnterpriseEngine engine;

  @BeforeEach
  void beforeEach() {
    dataSource = pgContainer.dataSource();
    dbos = new DBOS(pgContainer.dbosConfig());
    impl = new RewindTestServiceImpl(dbos);
    proxy = dbos.registerProxy(RewindTestService.class, impl);
    impl.setProxy(proxy);
    dbos.launch();
    engine =
        new DBOSEnterpriseEngine(
            DBOSTestAccess.getDbosExecutor(dbos), DBOSTestAccess.getSystemDatabase(dbos));
  }

  // Runs threeSteps to completion under the given ID.
  private String start(String workflowId, String name) {
    try (var o = new WorkflowOptions(workflowId).setContext()) {
      proxy.threeSteps(name);
    }
    return workflowId;
  }

  private String start(String name) {
    return start(UUID.randomUUID().toString(), name);
  }

  private List<Integer> resultOf(String workflowId) throws Exception {
    return dbos.<List<Integer>, Exception>retrieveWorkflow(workflowId).getResult();
  }

  private WorkflowStatus statusOf(String workflowId) {
    return dbos.retrieveWorkflow(workflowId).getStatus();
  }

  @Test
  public void rerunsEachWorkflowFromItsOwnStartStep() throws Exception {
    dbos.registerQueue("batch_partitioned", new QueueOptions().withPartitionConcurrency(1));
    dbos.registerQueue("batch_plain", new QueueOptions());

    var partitionedId = UUID.randomUUID().toString();
    var plainId = UUID.randomUUID().toString();
    dbos.startWorkflow(
            () -> proxy.threeSteps("p"),
            new StartWorkflowOptions(partitionedId)
                .withQueue("batch_partitioned")
                .withQueuePartitionKey("pk"))
        .getResult();
    dbos.startWorkflow(
            () -> proxy.threeSteps("q"), new StartWorkflowOptions(plainId).withQueue("batch_plain"))
        .getResult();

    engine.rewindWorkflows(
        List.of(new WorkflowRewind(partitionedId, 1), new WorkflowRewind(plainId, 2)),
        null,
        null,
        null);

    // Same IDs, and only the steps from each workflow's own start step ran again.
    assertEquals(List.of(1, 2, 2), resultOf(partitionedId));
    assertEquals(List.of(1, 1, 2), resultOf(plainId));
    assertEquals(
        List.of(0, 1, 2),
        dbos.listWorkflowSteps(partitionedId).stream().map(StepInfo::functionId).toList());

    // With no queue given, each kept its own queue and partition key.
    assertEquals("batch_partitioned", statusOf(partitionedId).queueName());
    assertEquals("pk", statusOf(partitionedId).queuePartitionKey());
    assertEquals("batch_plain", statusOf(plainId).queueName());
    assertNull(statusOf(plainId).queuePartitionKey());
  }

  @ParameterizedTest
  @EnumSource(
      value = WorkflowState.class,
      names = {"PENDING", "ENQUEUED", "DELAYED", "SUSPENDED"})
  public void refusesAnUnfinishedWorkflow(WorkflowState unfinished) throws Exception {
    var finishedId = start("finished-" + unfinished);
    var unfinishedId = start("unfinished-" + unfinished);
    DBUtils.setWorkflowState(dataSource, unfinishedId, unfinished.name());

    var refused =
        assertThrows(
            IllegalStateException.class,
            () ->
                engine.rewindWorkflows(
                    List.of(new WorkflowRewind(finishedId, 0), new WorkflowRewind(unfinishedId, 0)),
                    null,
                    null,
                    null));
    assertTrue(
        refused.getMessage().startsWith("Cannot rewind " + unfinishedId), refused.getMessage());

    // Nothing changed, not even for the finished workflow.
    assertEquals(WorkflowState.SUCCESS, statusOf(finishedId).status());
    assertEquals(3, dbos.listWorkflowSteps(finishedId).size());
    assertEquals(1, impl.runsOf("finished-" + unfinished + ".a"));
    assertEquals(unfinished, statusOf(unfinishedId).status());
    assertEquals(3, dbos.listWorkflowSteps(unfinishedId).size());
  }

  @Test
  public void refusesAMissingWorkflow() throws Exception {
    var finishedId = start("finished-missing");
    var missingId = UUID.randomUUID().toString();

    var refused =
        assertThrows(
            DBOSNonExistentWorkflowException.class,
            () ->
                engine.rewindWorkflows(
                    List.of(new WorkflowRewind(finishedId, 1), new WorkflowRewind(missingId, 1)),
                    null,
                    null,
                    null));
    assertEquals(missingId, refused.workflowId());

    assertEquals(WorkflowState.SUCCESS, statusOf(finishedId).status());
    assertEquals(3, dbos.listWorkflowSteps(finishedId).size());
    assertEquals(1, impl.runsOf("finished-missing.b"));
  }

  @Test
  public void isOneTransaction() throws Exception {
    // On CockroachDB the concurrent change is a serialization error instead, and the retry reads
    // the new status.
    Assumptions.assumeFalse(PgContainer.USE_COCKROACH_DB, "relies on READ COMMITTED");
    // The batch goes in workflow ID order, so the first workflow is rewound before the second's
    // guarded update finds its status changed.
    var firstId = start("a-" + UUID.randomUUID(), "txn-first");
    var secondId = start("z-" + UUID.randomUUID(), "txn-second");

    // As if the second workflow were cancelled between the batch's read and its rewind.
    var testThread = Thread.currentThread();
    DebugTriggers.setDebugTrigger(
        DebugTriggers.DEBUG_TRIGGER_REWIND_AFTER_STATUS_READ,
        new DebugTriggers.DebugAction()
            .setCallback(
                () -> {
                  if (Thread.currentThread() != testThread) return;
                  try {
                    DBUtils.setWorkflowState(dataSource, secondId, WorkflowState.CANCELLED.name());
                  } catch (SQLException e) {
                    throw new RuntimeException(e);
                  }
                }));
    try {
      var refused =
          assertThrows(
              IllegalStateException.class,
              () ->
                  engine.rewindWorkflows(
                      List.of(new WorkflowRewind(firstId, 1), new WorkflowRewind(secondId, 1)),
                      null,
                      null,
                      null));
      assertTrue(
          refused.getMessage().contains("changed status while being rewound"),
          refused.getMessage());
    } finally {
      DebugTriggers.clearDebugTrigger(DebugTriggers.DEBUG_TRIGGER_REWIND_AFTER_STATUS_READ);
    }

    // The first workflow's rewind was rolled back with the rest.
    assertEquals(WorkflowState.SUCCESS, statusOf(firstId).status());
    assertEquals(3, dbos.listWorkflowSteps(firstId).size());
    assertEquals(List.of(1, 1, 1), resultOf(firstId));
    assertEquals(1, impl.runsOf("txn-first.b"));
    assertEquals(WorkflowState.CANCELLED, statusOf(secondId).status());
    assertEquals(3, dbos.listWorkflowSteps(secondId).size());
  }

  @Test
  public void keepsOrOverridesVersionAndQueue() throws Exception {
    dbos.registerQueue("batch_override", new QueueOptions().withPartitionConcurrency(1));
    var elsewhereId = start("elsewhere");
    var hereId = start("here");
    var runningVersion = statusOf(hereId).appVersion();

    // Stamp one workflow with a version nothing runs, so it stays ENQUEUED.
    dbos.rewindWorkflow(
        elsewhereId, 2, new RewindOptions().withApplicationVersion("not-this-deployment"));
    dbos.cancelWorkflow(elsewhereId);

    // Given no version, each keeps its own: only the other one runs again.
    engine.rewindWorkflows(
        List.of(new WorkflowRewind(elsewhereId, 2), new WorkflowRewind(hereId, 2)),
        null,
        null,
        null);
    assertEquals(List.of(1, 1, 2), resultOf(hereId));
    assertEquals("not-this-deployment", statusOf(elsewhereId).appVersion());
    assertEquals(WorkflowState.ENQUEUED, statusOf(elsewhereId).status());
    dbos.cancelWorkflow(elsewhereId);

    // A version and queue given apply to every workflow in the batch.
    engine.rewindWorkflows(
        List.of(new WorkflowRewind(elsewhereId, 2), new WorkflowRewind(hereId, 2)),
        runningVersion,
        "batch_override",
        "repair");
    assertEquals(List.of(1, 1, 2), resultOf(elsewhereId));
    assertEquals(List.of(1, 1, 3), resultOf(hereId));
    for (var workflowId : List.of(elsewhereId, hereId)) {
      var status = statusOf(workflowId);
      assertEquals(runningVersion, status.appVersion());
      assertEquals("batch_override", status.queueName());
      assertEquals("repair", status.queuePartitionKey());
    }
  }

  @Test
  public void validatesAndDedupesItsInputs() throws Exception {
    var workflowId = start("inputs");

    // An empty batch changes nothing.
    engine.rewindWorkflows(List.of(), null, null, null);

    // A negative start step refuses the batch before anything changes.
    var refused =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                engine.rewindWorkflows(
                    List.of(new WorkflowRewind(workflowId, -1)), null, null, null));
    assertTrue(refused.getMessage().contains("must be >= 0"), refused.getMessage());
    assertEquals(WorkflowState.SUCCESS, statusOf(workflowId).status());
    assertEquals(1, impl.runsOf("inputs.a"));

    // A workflow listed twice is rewound once, from its first listing's step.
    engine.rewindWorkflows(
        List.of(new WorkflowRewind(workflowId, 1), new WorkflowRewind(workflowId, 2)),
        null,
        null,
        null);
    assertEquals(List.of(1, 2, 2), resultOf(workflowId));
  }
}
