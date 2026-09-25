package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.Result;
import dev.dbos.transact.utils.PgContainer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Every read of inputs, output and error prefers migration 109's workflow_input / workflow_output
 * and falls back to the legacy workflow_status column. The SDK writes only the payload tables, but
 * rows written by earlier releases, or by an executor still on one mid-upgrade, carry the legacy
 * columns instead, so these tests rebuild those shapes by hand.
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
    PayloadReadServiceImpl.stepRuns.set(0);
    proxy = dbos.registerProxy(PayloadReadService.class, new PayloadReadServiceImpl(dbos));
    dbos.launch();
  }

  @Test
  void legacyColumnsAreReadWhenThePayloadTablesHaveNoRow() throws Exception {
    var handle = dbos.startWorkflow(() -> proxy.echo(1));
    assertEquals(1, handle.getResult());

    moveToLegacyColumns(handle.workflowId());

    assertEveryReadSees(handle.workflowId(), 1);
  }

  @Test
  void thePayloadTableWinsOverTheLegacyColumn() throws Exception {
    var target = dbos.startWorkflow(() -> proxy.echo(1));
    assertEquals(1, target.getResult());
    var donor = dbos.startWorkflow(() -> proxy.echo(2));
    assertEquals(2, donor.getResult());

    moveToLegacyColumns(target.workflowId());
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

    moveToLegacyColumns(target.workflowId());
    givePayloadRowsOf(donor.workflowId(), target.workflowId());
    // The shape the SDK writes now: the payload table is the only copy.
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
    moveToLegacyColumns(targetId);
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

  @Test
  void aWorkflowAnOlderReleaseLeftMidRunRecoversWithItsLegacyInputs() throws Exception {
    var handle = dbos.startWorkflow(() -> proxy.stepThenEcho(7));
    assertEquals(7, handle.getResult());
    assertEquals(1, PayloadReadServiceImpl.stepRuns.get());
    var workflowId = handle.workflowId();

    // Rewind the row to what a 1.1 executor that crashed after the step leaves behind: PENDING,
    // the step checkpointed, the input only on the status row, and no outcome anywhere.
    moveToLegacyColumns(workflowId);
    exec(
        "UPDATE dbos.workflow_status SET status = 'PENDING', output = NULL, error = NULL,"
            + " completed_at = NULL WHERE workflow_uuid = ?",
        workflowId);

    var executor = DBOSTestAccess.getDbosExecutor(dbos);
    assertEquals(
        List.of(workflowId), executor.recoverPendingWorkflows(List.of(executor.executorId())));

    // Recovery re-enqueues, and the run it starts has only the legacy column to read the input
    // from. The step replays from its checkpoint rather than running again.
    assertEquals(7, dbos.<Integer, Exception>retrieveWorkflow(workflowId).getResult());
    assertEquals(1, PayloadReadServiceImpl.stepRuns.get(), "the step must replay, not re-run");

    // The outcome this release recorded went to workflow_output, beside the legacy input: a row
    // of both shapes, which every read resolves.
    assertEveryReadSees(workflowId, 7);
  }

  @Test
  void aReusedIdDoesNotInheritPayloadsRetentionHasNotSweptYet() throws Exception {
    var donor = dbos.startWorkflow(() -> proxy.echo(2));
    assertEquals(2, donor.getResult());

    // A retention round deletes a workflow's status row before its payload sweep removes the
    // payloads, so for a while they sit under an ID no workflow holds. Plant that state.
    var workflowId = "reused-" + UUID.randomUUID();
    givePayloadRowsOf(donor.workflowId(), workflowId);
    exec(
        "UPDATE dbos.workflow_input SET retention_timestamp = 1000 WHERE workflow_uuid = ?",
        workflowId);
    exec(
        "UPDATE dbos.workflow_output SET retention_timestamp = 1000 WHERE workflow_uuid = ?",
        workflowId);

    // A new workflow under that ID runs with its own input, not the leftover one.
    var handle = dbos.startWorkflow(() -> proxy.echo(5), new StartWorkflowOptions(workflowId));
    assertEquals(5, handle.getResult());
    assertEveryReadSees(workflowId, 5);

    // And its payloads carry its own retention, so the payload sweep that would have removed the
    // leftovers leaves them alone.
    DBOSTestAccess.getSystemDatabase(dbos)
        .garbageCollect(Instant.now().minusSeconds(60), null, 1000);
    assertEveryReadSees(workflowId, 5);
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

    // The fork reads the original's inputs and copies them to the new workflow: reading the child
    // back shows what the fork saw.
    var forkedId = systemDatabase.forkWorkflow(workflowId, 0, new ForkOptions());
    assertEquals(
        expected,
        (int) (Integer) systemDatabase.getWorkflowStatus(forkedId).input()[0],
        "forkWorkflow inputs");
  }

  /**
   * Rewrites a row into the shape a release that predates the payload writes leaves: payloads in
   * the legacy workflow_status columns, no payload-table rows.
   */
  private void moveToLegacyColumns(String workflowId) throws Exception {
    exec(
        "UPDATE dbos.workflow_status ws SET inputs = wi.inputs FROM dbos.workflow_input wi"
            + " WHERE wi.workflow_uuid = ws.workflow_uuid AND ws.workflow_uuid = ?",
        workflowId);
    exec(
        "UPDATE dbos.workflow_status ws SET output = wo.output, error = wo.error"
            + " FROM dbos.workflow_output wo"
            + " WHERE wo.workflow_uuid = ws.workflow_uuid AND ws.workflow_uuid = ?",
        workflowId);
    exec("DELETE FROM dbos.workflow_input WHERE workflow_uuid = ?", workflowId);
    exec("DELETE FROM dbos.workflow_output WHERE workflow_uuid = ?", workflowId);
  }

  /**
   * Gives {@code to} the payload rows of {@code from}, copied straight out of its own, so the test
   * never has to encode a serialized value itself. {@code to} must have none of its own yet.
   */
  private void givePayloadRowsOf(String from, String to) throws Exception {
    exec(
        "INSERT INTO dbos.workflow_input(workflow_uuid, inputs, retention_timestamp)"
            + " SELECT ?, inputs, retention_timestamp FROM dbos.workflow_input"
            + " WHERE workflow_uuid = ?",
        to,
        from);
    exec(
        "INSERT INTO dbos.workflow_output(workflow_uuid, output, error, retention_timestamp)"
            + " SELECT ?, output, error, retention_timestamp FROM dbos.workflow_output"
            + " WHERE workflow_uuid = ?",
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

  int stepThenEcho(int x);

  int fail(int x);
}

class PayloadReadServiceImpl implements PayloadReadService {

  static final AtomicInteger stepRuns = new AtomicInteger();

  private final DBOS dbos;

  PayloadReadServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Workflow
  @Override
  public int echo(int x) {
    return x;
  }

  @Workflow
  @Override
  public int stepThenEcho(int x) {
    return dbos.runStep(
        () -> {
          stepRuns.incrementAndGet();
          return x;
        },
        "recordInput");
  }

  @Workflow
  @Override
  public int fail(int x) {
    throw new IllegalStateException("failure " + x);
  }
}
