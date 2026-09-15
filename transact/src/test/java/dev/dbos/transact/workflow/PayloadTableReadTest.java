package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.Result;
import dev.dbos.transact.utils.PgContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Migration 109's payload tables are read by this release but written by the next one. Every read
 * of inputs, output and error prefers workflow_input / workflow_output and falls back to the legacy
 * workflow_status column, which is what lets the release that moves the writes roll out one node at
 * a time. Nothing writes the new shape yet, so these tests put rows there by hand.
 */
class PayloadTableReadTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;

  private PayloadReadService proxy;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
    proxy = dbos.registerProxy(PayloadReadService.class, new PayloadReadServiceImpl());
    dbos.launch();
  }

  @Test
  void legacyColumnsAreReadWhenThePayloadTablesHaveNoRow() throws Exception {
    var handle = dbos.startWorkflow(() -> proxy.echo(1));
    assertEquals(1, handle.getResult());

    assertEveryReadSees(handle.workflowId(), 1);
  }

  @Test
  void thePayloadTableWinsOverTheLegacyColumn() throws Exception {
    var target = dbos.startWorkflow(() -> proxy.echo(1));
    assertEquals(1, target.getResult());
    var donor = dbos.startWorkflow(() -> proxy.echo(2));
    assertEquals(2, donor.getResult());

    givePayloadRowsOf(donor.workflowId(), target.workflowId());

    // The legacy columns still hold 1, so anything reporting 2 read the payload table.
    assertEveryReadSees(target.workflowId(), 2);
  }

  @Test
  void thePayloadTableIsReadWhenTheLegacyColumnIsNull() throws Exception {
    var target = dbos.startWorkflow(() -> proxy.echo(1));
    assertEquals(1, target.getResult());
    var donor = dbos.startWorkflow(() -> proxy.echo(2));
    assertEquals(2, donor.getResult());

    givePayloadRowsOf(donor.workflowId(), target.workflowId());
    // The shape a future release writes, and the one an older row can never have: the payload table
    // is the only copy left.
    clearLegacyColumns(target.workflowId());

    assertEveryReadSees(target.workflowId(), 2);
  }

  @Test
  void theErrorColumnReadsBothShapesToo() throws Exception {
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    var target = dbos.startWorkflow(() -> proxy.fail(1));
    assertThrows(Exception.class, target::getResult);
    var donor = dbos.startWorkflow(() -> proxy.fail(2));
    assertThrows(Exception.class, donor::getResult);

    var targetId = target.workflowId();
    assertEquals("failure 1", systemDatabase.getWorkflowStatus(targetId).error().message());

    givePayloadRowsOf(donor.workflowId(), targetId);
    assertEquals(
        "failure 2",
        systemDatabase.getWorkflowStatus(targetId).error().message(),
        "the payload table's error must win");

    clearLegacyColumns(targetId);
    assertEquals(
        "failure 2",
        systemDatabase.getWorkflowStatus(targetId).error().message(),
        "and still be read once the legacy column is gone");
  }

  /** Every read path that goes through the inputs/output COALESCE helpers. */
  private void assertEveryReadSees(String workflowId, int expected) throws Exception {
    var systemDatabase = DBOSTestAccess.getSystemDatabase(dbos);

    var status = systemDatabase.getWorkflowStatus(workflowId);
    assertEquals(expected, (int) (Integer) status.input()[0], "getWorkflowStatus inputs");
    assertEquals(expected, (int) (Integer) status.output(), "getWorkflowStatus output");

    var listed =
        systemDatabase.listWorkflows(
            new ListWorkflowsInput(List.of(workflowId)).withLoadInput(true).withLoadOutput(true));
    assertEquals(1, listed.size());
    assertEquals(expected, (int) (Integer) listed.get(0).input()[0], "listWorkflows inputs");
    assertEquals(expected, (int) (Integer) listed.get(0).output(), "listWorkflows output");

    Result<Integer> awaited = systemDatabase.awaitWorkflowResult(workflowId, true);
    assertInstanceOf(Result.Success.class, awaited);
    assertEquals(
        expected, (int) ((Result.Success<Integer>) awaited).value(), "awaitWorkflowResult output");

    // The fork reads the original's inputs and writes them onto the new row, which this release
    // still fills from the legacy column: reading the child back shows what the fork saw.
    var forkedId = systemDatabase.forkWorkflow(workflowId, 0, new ForkOptions());
    assertEquals(
        expected,
        (int) (Integer) systemDatabase.getWorkflowStatus(forkedId).input()[0],
        "forkWorkflow inputs");
  }

  /**
   * Gives {@code to} the payload rows of {@code from}, copied straight out of its status row, so
   * the test never has to encode a serialized value itself.
   */
  private void givePayloadRowsOf(String from, String to) throws Exception {
    exec(
        "INSERT INTO dbos.workflow_input(workflow_uuid, inputs, retention_timestamp)"
            + " SELECT ?, inputs, created_at FROM dbos.workflow_status WHERE workflow_uuid = ?",
        to,
        from);
    exec(
        "INSERT INTO dbos.workflow_output(workflow_uuid, output, error, retention_timestamp)"
            + " SELECT ?, output, error, created_at FROM dbos.workflow_status WHERE workflow_uuid = ?",
        to,
        from);
  }

  private void clearLegacyColumns(String workflowId) throws Exception {
    exec(
        "UPDATE dbos.workflow_status SET inputs = NULL, output = NULL, error = NULL"
            + " WHERE workflow_uuid = ?",
        workflowId);
  }

  private void exec(String sql, String... args) throws Exception {
    try (var conn = connection();
        var stmt = conn.prepareStatement(sql)) {
      for (int i = 0; i < args.length; i++) {
        stmt.setString(i + 1, args[i]);
      }
      stmt.executeUpdate();
    }
  }

  private Connection connection() throws Exception {
    return DriverManager.getConnection(
        pgContainer.jdbcUrl(), pgContainer.username(), pgContainer.password());
  }
}

interface PayloadReadService {
  int echo(int x);

  int fail(int x);
}

class PayloadReadServiceImpl implements PayloadReadService {

  @Workflow
  @Override
  public int echo(int x) {
    return x;
  }

  @Workflow
  @Override
  public int fail(int x) {
    throw new IllegalStateException("failure " + x);
  }
}
