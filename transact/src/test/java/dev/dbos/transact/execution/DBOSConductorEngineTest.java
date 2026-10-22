package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowState;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface EngineTestWorkflows {
  String echo(String value);
}

class EngineTestWorkflowsImpl implements EngineTestWorkflows {
  private final DBOS dbos;

  EngineTestWorkflowsImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow
  public String echo(String value) {
    return dbos.runStep(() -> value, "echoStep");
  }
}

/** The engine DBOS hands the Conductor client serves its calls from the running instance. */
class DBOSConductorEngineTest {
  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;
  EngineTestWorkflows proxy;
  DBOSExecutor executor;
  DBOSConductorEngine engine;

  @BeforeEach
  void beforeEach() {
    dbos = new DBOS(pgContainer.dbosConfig());
    proxy = dbos.registerProxy(EngineTestWorkflows.class, new EngineTestWorkflowsImpl(dbos));
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
