package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.utils.DebouncedRows;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The bounce: one statement that extends a debounced DELAYED workflow's delay and replaces its
 * inputs, or reports who holds the pair instead. And the transition that ends the bounce window by
 * clearing the debounce key.
 *
 * <p>Runs against a client's system database rather than a launched DBOS, so no queue runner sweeps
 * the planted rows out from under the assertions.
 */
public class DebounceDelayedWorkflowTest {

  static final String QUEUE = "bounce-queue";
  static final String NAME = "process";
  static final String CLASS = "com.example.ProcessImpl";
  static final String DEDUP = NAME + "-key";

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose HikariDataSource dataSource;
  @AutoClose DBOSClient client;
  SystemDatabase sysdb;

  @BeforeEach
  void beforeEach() {
    // Launching migrates the schema; the client needs nothing else from it.
    try (DBOS dbos = new DBOS(pgContainer.dbosConfig())) {
      dbos.launch();
    }
    dataSource = pgContainer.dataSource();
    client = pgContainer.dbosClient("app-a");
    sysdb = DBOSTestAccess.getSystemDatabase(client);
  }

  private String plant(long delayUntil, Long deadline, String name, String appName)
      throws SQLException {
    return plant(QUEUE, delayUntil, deadline, name, appName);
  }

  private String plant(String queue, long delayUntil, Long deadline, String name, String appName)
      throws SQLException {
    return DebouncedRows.insert(
        dataSource,
        new DebouncedRows.Spec(
            name, CLASS, null, queue, DEDUP, delayUntil, deadline, stale(), null, null, appName));
  }

  private static String stale() {
    return SerializationUtil.serializeArgs(new Object[] {"stale"}, null, null, null)
        .serializedValue();
  }

  private DebounceResult bounce(long delayUntil, String name, String instanceName) {
    return bounce(delayUntil, name, instanceName, null);
  }

  private DebounceResult bounce(
      long delayUntil, String name, String instanceName, String serializationFormat) {
    return (DebounceResult)
        sysdb.debounceDelayedWorkflow(
            name,
            CLASS,
            instanceName,
            QUEUE,
            DEDUP,
            delayUntil,
            new Object[] {"fresh"},
            serializationFormat,
            null);
  }

  // ==================== Extending ====================

  @Test
  void extendsTheDelayAndReplacesTheInputs() throws Exception {
    long now = System.currentTimeMillis();
    var id = plant(now + 1_000, null, NAME, "app-a");

    var result = bounce(now + 5_000, NAME, null);

    assertEquals(new DebounceResult.Bounced(id), result);
    var row = DebouncedRows.read(dataSource, id);
    assertEquals(WorkflowState.DELAYED.name(), row.status());
    assertEquals(DEDUP, row.deduplicationId());
    assertEquals(now + 5_000, row.delayUntilEpochMs());
    var fresh = SerializationUtil.serializeArgs(new Object[] {"fresh"}, null, null, null);
    assertEquals(fresh.serializedValue(), row.inputs());
    assertEquals(fresh.serialization(), row.serialization());
  }

  @Test
  void writesTheInputsInTheRequestedFormat() throws Exception {
    // A portable workflow's row holds portable arguments. A bounce that wrote this SDK's native
    // format over them would leave a row no reader of that workflow can deserialize.
    long now = System.currentTimeMillis();
    var portableStale =
        SerializationUtil.serializeArgs(
            new Object[] {"stale"}, null, SerializationUtil.PORTABLE, null);
    var id =
        DebouncedRows.insert(
            dataSource,
            new DebouncedRows.Spec(
                NAME,
                CLASS,
                null,
                QUEUE,
                DEDUP,
                now + 1_000,
                null,
                portableStale.serializedValue(),
                portableStale.serialization(),
                null,
                "app-a"));

    var result = bounce(now + 5_000, NAME, null, SerializationUtil.PORTABLE);

    assertEquals(new DebounceResult.Bounced(id), result);
    var row = DebouncedRows.read(dataSource, id);
    var fresh =
        SerializationUtil.serializeArgs(
            new Object[] {"fresh"}, null, SerializationUtil.PORTABLE, null);
    assertEquals(fresh.serializedValue(), row.inputs());
    assertEquals(SerializationUtil.PORTABLE, row.serialization());
    // And it really is the portable encoding, not the native one under a portable label.
    assertArrayEquals(
        new Object[] {"fresh"},
        SerializationUtil.deserializePositionalArgs(row.inputs(), row.serialization(), null));
  }

  @Test
  void replacesTheInputsInThePayloadTableToo() throws Exception {
    // Readers prefer workflow_input over workflow_status.inputs, so a bounce that left the payload
    // table alone would run the workflow with the arguments it was meant to replace.
    long now = System.currentTimeMillis();
    var id = plant(now + 1_000, null, NAME, "app-a");
    DebouncedRows.insertInput(dataSource, id, stale());

    assertEquals(new DebounceResult.Bounced(id), bounce(now + 5_000, NAME, null));

    var fresh = SerializationUtil.serializeArgs(new Object[] {"fresh"}, null, null, null);
    assertEquals(fresh.serializedValue(), DebouncedRows.readInput(dataSource, id));
    assertEquals(fresh.serializedValue(), DebouncedRows.read(dataSource, id).inputs());
  }

  @Test
  void doesNotCreateAPayloadRowWhenTheRowHadNone() throws Exception {
    // This version does not write payload rows; such a row reads its inputs from the status row.
    long now = System.currentTimeMillis();
    var id = plant(now + 1_000, null, NAME, "app-a");
    assertNull(DebouncedRows.readInput(dataSource, id));

    bounce(now + 5_000, NAME, null);

    assertNull(DebouncedRows.readInput(dataSource, id));
    var fresh = SerializationUtil.serializeArgs(new Object[] {"fresh"}, null, null, null);
    assertEquals(fresh.serializedValue(), DebouncedRows.read(dataSource, id).inputs());
  }

  @Test
  void leavesThePayloadTableAloneWhenNothingMatched() throws Exception {
    long now = System.currentTimeMillis();
    var id = plant(now + 1_000, null, "other", "app-a");
    DebouncedRows.insertInput(dataSource, id, stale());

    assertInstanceOf(DebounceResult.NotBounced.class, bounce(now + 5_000, NAME, null));

    assertEquals(stale(), DebouncedRows.readInput(dataSource, id));
  }

  @Test
  void capsTheDelayAtTheDebounceDeadline() throws Exception {
    long now = System.currentTimeMillis();
    var id = plant(now + 1_000, now + 2_000, NAME, "app-a");

    var result = bounce(now + 5_000, NAME, null);

    assertEquals(new DebounceResult.Bounced(id), result);
    assertEquals(now + 2_000, DebouncedRows.read(dataSource, id).delayUntilEpochMs());
  }

  @Test
  void extendsAnUnclaimedRowAndClaimsIt() throws Exception {
    long now = System.currentTimeMillis();
    var id = plant(now + 1_000, null, NAME, null);

    assertEquals(new DebounceResult.Bounced(id), bounce(now + 5_000, NAME, null));

    // Claimed for the bouncing application, as its dequeue would claim it.
    assertEquals("app-a", DebouncedRows.read(dataSource, id).applicationName());
  }

  @Test
  void keepsTheOwnerOfAClaimedRow() throws Exception {
    long now = System.currentTimeMillis();
    var id = plant(now + 1_000, null, NAME, "app-a");

    bounce(now + 5_000, NAME, null);

    assertEquals("app-a", DebouncedRows.read(dataSource, id).applicationName());
  }

  // ==================== Reporting the holder ====================

  @Test
  void reportsAnUnheldKey() {
    var result = bounce(System.currentTimeMillis() + 5_000, NAME, null);

    assertEquals(new DebounceResult.NotBounced(null), result);
  }

  @Test
  void doesNotExtendAnotherWorkflowThatCollidesOnTheKey() throws Exception {
    long now = System.currentTimeMillis();
    var id = plant(now + 1_000, null, "other", "app-a");

    var result = bounce(now + 5_000, NAME, null);

    var holder = assertInstanceOf(DebounceResult.NotBounced.class, result).holder();
    assertNotNull(holder);
    assertEquals(id, holder.workflowId());
    assertEquals("other", holder.workflowName());
    assertEquals(CLASS, holder.className());
    assertTrue(holder.isDebounced());
    assertEquals(WorkflowState.DELAYED, holder.status());
    assertFalse(holder.isDebouncedInstanceOf(NAME, CLASS, null));
    assertTrue(holder.isDebouncedInstanceOf("other", CLASS, null));
    // Untouched.
    var row = DebouncedRows.read(dataSource, id);
    assertEquals(now + 1_000, row.delayUntilEpochMs());
    assertEquals(stale(), row.inputs());
  }

  @Test
  void doesNotExtendAnotherInstanceOfTheSameClass() throws Exception {
    long now = System.currentTimeMillis();
    var id = plant(now + 1_000, null, NAME, "app-a");

    var result = bounce(now + 5_000, NAME, "east");

    var holder = assertInstanceOf(DebounceResult.NotBounced.class, result).holder();
    assertEquals(id, holder.workflowId());
    assertEquals(now + 1_000, DebouncedRows.read(dataSource, id).delayUntilEpochMs());
  }

  @Test
  void doesNotExtendAPeerApplicationsRowButReportsIt() throws Exception {
    long now = System.currentTimeMillis();
    var id = plant(now + 1_000, null, NAME, "app-b");

    var result = bounce(now + 5_000, NAME, null);

    var holder = assertInstanceOf(DebounceResult.NotBounced.class, result).holder();
    assertEquals(id, holder.workflowId());
    assertEquals("app-b", holder.applicationName());
    assertTrue(holder.isForeignTo("app-a"));
    assertEquals(now + 1_000, DebouncedRows.read(dataSource, id).delayUntilEpochMs());
  }

  @Test
  void doesNotExtendARowThatIsNoLongerDelayed() throws Exception {
    long now = System.currentTimeMillis();
    var id = plant(now - 1_000, null, NAME, "app-a");
    sysdb.transitionDelayedWorkflows();

    var result = bounce(now + 5_000, NAME, null);

    // The key was cleared by the transition, so nothing holds it any more.
    assertEquals(new DebounceResult.NotBounced(null), result);
    assertEquals(WorkflowState.ENQUEUED.name(), DebouncedRows.read(dataSource, id).status());
  }

  // ==================== The transition ====================

  @Test
  void transitionClearsTheDebounceKeyAndKeepsOtherDeduplicationIds() throws Exception {
    long now = System.currentTimeMillis();
    var debounced = plant(now - 1_000, null, NAME, "app-a");
    var plain = insertPlainDelayed(now - 1_000);
    // The same key on another queue is a different pair; this one has not expired.
    var waiting = plant("other-queue", now + 60_000, null, NAME, "app-a");

    sysdb.transitionDelayedWorkflows();

    var d = DebouncedRows.read(dataSource, debounced);
    assertEquals(WorkflowState.ENQUEUED.name(), d.status());
    assertNull(d.deduplicationId());
    var p = DebouncedRows.read(dataSource, plain);
    assertEquals(WorkflowState.ENQUEUED.name(), p.status());
    assertEquals("plain-dedup", p.deduplicationId());
    var w = DebouncedRows.read(dataSource, waiting);
    assertEquals(WorkflowState.DELAYED.name(), w.status());
    assertEquals(DEDUP, w.deduplicationId());
  }

  /** A DELAYED workflow deduplicated on its own account: not debounced, keeps its id. */
  private String insertPlainDelayed(long delayUntil) throws SQLException {
    var id = java.util.UUID.randomUUID().toString();
    var sql =
        """
          INSERT INTO "dbos".workflow_status
              (workflow_uuid, status, name, class_name, queue_name, deduplication_id,
               delay_until_epoch_ms, inputs, application_name, created_at, updated_at,
               recovery_attempts, priority)
          VALUES (?, 'DELAYED', ?, ?, ?, 'plain-dedup', ?, ?, 'app-a', ?, ?, 0, 0)
        """;
    long now = System.currentTimeMillis();
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, id);
      stmt.setString(2, NAME);
      stmt.setString(3, CLASS);
      stmt.setString(4, "plain-queue");
      stmt.setLong(5, delayUntil);
      stmt.setString(6, stale());
      stmt.setLong(7, now);
      stmt.setLong(8, now);
      stmt.executeUpdate();
    }
    return id;
  }
}
