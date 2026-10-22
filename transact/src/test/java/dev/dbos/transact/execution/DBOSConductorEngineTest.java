package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowState;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface EngineTestWorkflows {
  String echo(String value);
}

class EngineTestWorkflowsImpl implements EngineTestWorkflows {
  private final DBOS dbos;
  // How many times echoStep ran for each value, so a test can tell a rerun step from a copied one.
  final Map<String, AtomicInteger> stepRuns = new ConcurrentHashMap<>();

  EngineTestWorkflowsImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public String echo(String value) {
    return dbos.runStep(
        () -> {
          stepRuns.computeIfAbsent(value, ignored -> new AtomicInteger()).incrementAndGet();
          return value;
        },
        "echoStep");
  }
}

/** The engine DBOS hands the Conductor client serves its calls from the running instance. */
class DBOSConductorEngineTest {
  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;
  EngineTestWorkflowsImpl impl;
  EngineTestWorkflows proxy;
  DBOSExecutor executor;
  DBOSConductorEngine engine;

  @BeforeEach
  void beforeEach() {
    dbos = new DBOS(pgContainer.dbosConfig());
    impl = new EngineTestWorkflowsImpl(dbos);
    proxy = dbos.registerProxy(EngineTestWorkflows.class, impl);
    dbos.launch();
    executor = DBOSTestAccess.getDbosExecutor(dbos);
    engine = new DBOSConductorEngine(executor, executor.getSystemDatabase());
  }

  @Test
  void reportsTheExecutorsIdentity() {
    assertEquals(executor.appName(), engine.appName());
    assertEquals(executor.appVersion(), engine.appVersion());
    assertEquals(executor.executorId(), engine.executorId());
    assertEquals(executor.executorMetadata(), engine.executorMetadata());
  }

  @Test
  void listsAndForksWorkflowsByTheirIds() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    dbos.startWorkflow(() -> proxy.echo("hello"), new StartWorkflowOptions(workflowId)).getResult();

    var statuses = engine.listWorkflows(new ListWorkflowsInput().withWorkflowIds(workflowId));
    assertEquals(List.of(WorkflowState.SUCCESS), statuses.stream().map(s -> s.status()).toList());
    assertEquals(1, engine.listWorkflowSteps(workflowId, true, null, null).size());

    var forkedId = engine.forkWorkflow(workflowId, 0, new ForkOptions());
    assertNotEquals(workflowId, forkedId);
    assertEquals("hello", dbos.<String, Exception>retrieveWorkflow(forkedId).getResult());
  }

  @Test
  void forksWorkflowsInOneBatchEachFromItsOwnStep() throws Exception {
    var first = UUID.randomUUID().toString();
    var second = UUID.randomUUID().toString();
    dbos.startWorkflow(() -> proxy.echo("one"), new StartWorkflowOptions(first)).getResult();
    dbos.startWorkflow(() -> proxy.echo("two"), new StartWorkflowOptions(second)).getResult();

    var forkedIds = engine.forkWorkflows(List.of(first, second), List.of(0, 1), null, null, null);

    assertEquals(2, forkedIds.size());
    assertEquals("one", dbos.<String, Exception>retrieveWorkflow(forkedIds.get(0)).getResult());
    assertEquals("two", dbos.<String, Exception>retrieveWorkflow(forkedIds.get(1)).getResult());
    assertEquals(first, dbos.retrieveWorkflow(forkedIds.get(0)).getStatus().forkedFrom());
    assertEquals(second, dbos.retrieveWorkflow(forkedIds.get(1)).getStatus().forkedFrom());
    // Forked from step 0, the first fork reruns echoStep; forked from step 1, the second copies its
    // original's step 0 instead.
    assertEquals(2, impl.stepRuns.get("one").get());
    assertEquals(1, impl.stepRuns.get("two").get());
  }

  @Test
  void refusesStartStepsThatDoNotMatchTheWorkflows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> engine.forkWorkflows(List.of(), List.of(0), null, null, null));
  }

  @Test
  void forksNoneOfTheBatchIfOneIsMissing() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    dbos.startWorkflow(() -> proxy.echo("one"), new StartWorkflowOptions(workflowId)).getResult();

    var thrown =
        assertThrows(
            DBOSNonExistentWorkflowException.class,
            () ->
                engine.forkWorkflows(
                    List.of(workflowId, "no-such-workflow"), List.of(0, 0), null, null, null));
    assertEquals("no-such-workflow", thrown.workflowId());
    assertEquals(
        List.of(workflowId),
        engine.listWorkflows(new ListWorkflowsInput()).stream().map(s -> s.workflowId()).toList());
  }

  @Test
  void handsOutTheSystemDatabaseForConductorOnlySql() throws Exception {
    var workflowId = UUID.randomUUID().toString();
    dbos.startWorkflow(() -> proxy.echo("hello"), new StartWorkflowOptions(workflowId)).getResult();

    var database = engine.systemDatabase();
    var sql =
        "SELECT status FROM \"%s\".workflow_status WHERE workflow_uuid = ?"
            .formatted(database.schema());
    var status =
        database.retry(
            () -> {
              try (var conn = database.dataSource().getConnection();
                  var stmt = conn.prepareStatement(sql)) {
                stmt.setString(1, workflowId);
                try (var rs = stmt.executeQuery()) {
                  rs.next();
                  return rs.getString("status");
                }
              }
            });
    assertEquals(WorkflowState.SUCCESS.name(), status);
    assertEquals(Set.of(), database.workflowChildren(workflowId));
  }
}
