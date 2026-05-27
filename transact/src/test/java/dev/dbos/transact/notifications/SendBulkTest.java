package dev.dbos.transact.notifications;

import static org.junit.jupiter.api.Assertions.*;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.SendMessage;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface BulkService {
  String block(String topic);
}

class BulkServiceImpl implements BulkService {
  private final DBOS dbos;

  BulkServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Workflow
  @Override
  public String block(String topic) {
    return dbos.<String>recv(topic, Duration.ofSeconds(30)).orElse(null);
  }
}

class SendBulkTest {

  @AutoClose final PgContainer pgContainer = new PgContainer();

  private DBOSConfig dbosConfig;
  @AutoClose private DBOS dbos;
  private BulkService service;

  @BeforeEach
  void setup() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
    service = dbos.registerProxy(BulkService.class, new BulkServiceImpl(dbos));
    dbos.launch();
  }

  private SystemDatabase db() {
    return DBOSTestAccess.getSystemDatabase(dbos);
  }

  /** Starts a blocking workflow that provides a valid workflow_status row. */
  private String startBlockingWorkflow(String id) throws Exception {
    dbos.startWorkflow(() -> service.block(id), new StartWorkflowOptions(id));
    return id;
  }

  // -------------------------------------------------------------------------
  // testSendBulkEmpty
  // -------------------------------------------------------------------------

  @Test
  void testSendBulkEmpty() {
    // Outside workflow: should be a complete no-op
    assertDoesNotThrow(() -> db().sendBulk(List.of(), null, -1, "DBOS.sendBulk", false, null));
  }

  // -------------------------------------------------------------------------
  // testSendBulkBasic
  // -------------------------------------------------------------------------

  @Test
  void testSendBulkBasic() throws Exception {
    String dest1 = startBlockingWorkflow("dest1-" + UUID.randomUUID());
    String dest2 = startBlockingWorkflow("dest2-" + UUID.randomUUID());

    // Two messages to dest1, one message to dest2, all in one call
    db().sendBulk(
        List.of(
            new SendMessage(dest1, "hello-1"),
            new SendMessage(dest1, "hello-2"),
            new SendMessage(dest2, "world")),
        null,
        -1,
        "DBOS.sendBulk",
        false,
        null);

    var notes1 = db().getAllNotifications(dest1);
    assertEquals(2, notes1.size());

    var notes2 = db().getAllNotifications(dest2);
    assertEquals(1, notes2.size());
    assertEquals("world", notes2.get(0).message());
  }

  // -------------------------------------------------------------------------
  // testSendBulkNonExistentDest — entire tx rolls back
  // -------------------------------------------------------------------------

  @Test
  void testSendBulkNonExistentDest() throws Exception {
    String validDest = startBlockingWorkflow("valid-" + UUID.randomUUID());

    assertThrows(
        DBOSNonExistentWorkflowException.class,
        () ->
            db().sendBulk(
                List.of(
                    new SendMessage(validDest, "first"),
                    new SendMessage("nonexistent-" + UUID.randomUUID(), "second")),
                null,
                -1,
                "DBOS.sendBulk",
                false,
                null));

    // Atomicity: valid dest must have received nothing
    assertEquals(0, db().getAllNotifications(validDest).size());
  }

  // -------------------------------------------------------------------------
  // testSendBulkDuplicateKeyWithinBatch — rejected before transaction opens
  // -------------------------------------------------------------------------

  @Test
  void testSendBulkDuplicateKeyWithinBatch() throws Exception {
    String dest = startBlockingWorkflow("dupkey-dest-" + UUID.randomUUID());
    String key = UUID.randomUUID().toString();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            db().sendBulk(
                List.of(
                    new SendMessage(dest, "msg1", null, key),
                    new SendMessage(dest, "msg2", null, key)),
                null,
                -1,
                "DBOS.sendBulk",
                false,
                null));

    // Nothing was delivered
    assertEquals(0, db().getAllNotifications(dest).size());
  }

  // -------------------------------------------------------------------------
  // testSendBulkIdempotencyKey — duplicate across calls is deduped
  // -------------------------------------------------------------------------

  @Test
  void testSendBulkIdempotencyKey() throws Exception {
    String dest = startBlockingWorkflow("idem-dest-" + UUID.randomUUID());
    String key = UUID.randomUUID().toString();

    var messages = List.of(new SendMessage(dest, "hello", null, key));
    db().sendBulk(messages, null, -1, "DBOS.sendBulk", false, null);
    db().sendBulk(messages, null, -1, "DBOS.sendBulk", false, null);

    // Only one notification despite two calls
    assertEquals(1, db().getAllNotifications(dest).size());
  }

  // -------------------------------------------------------------------------
  // testSendBulkFromWorkflow — step is recorded; replay skips re-delivery
  // -------------------------------------------------------------------------

  @Test
  void testSendBulkFromWorkflow() throws Exception {
    // senderWf provides a real workflow_status row to act as the sender context
    String senderWf = startBlockingWorkflow("sender-wf-" + UUID.randomUUID());
    String dest = startBlockingWorkflow("wf-dest-" + UUID.randomUUID());

    var messages = List.of(new SendMessage(dest, "from-workflow"));
    // Use stepId 100 to stay clear of the recv workflow's own step IDs
    db().sendBulk(messages, senderWf, 100, "DBOS.sendBulk", false, null);

    assertEquals(1, db().getAllNotifications(dest).size());

    // The sendBulk step should be recorded in the sender workflow's history
    var steps = dbos.listWorkflowSteps(senderWf);
    assertTrue(steps.stream().anyMatch(s -> "DBOS.sendBulk".equals(s.functionName())));

    // Second call with same workflowId/stepId is a replay — no new delivery
    db().sendBulk(messages, senderWf, 100, "DBOS.sendBulk", false, null);
    assertEquals(1, db().getAllNotifications(dest).size());
  }

  // -------------------------------------------------------------------------
  // testSendBulkSendToForks — fan-out to recursive fork tree
  // -------------------------------------------------------------------------

  @Test
  void testSendBulkSendToForks() throws Exception {
    // Build the tree: parent -> child -> grandchild
    String parentId = "parent-" + UUID.randomUUID();
    String childId = "child-" + UUID.randomUUID();
    String grandchildId = "grandchild-" + UUID.randomUUID();
    String unrelatedId = "unrelated-" + UUID.randomUUID();

    startBlockingWorkflow(parentId);
    startBlockingWorkflow(unrelatedId);
    dbos.forkWorkflow(parentId, 0, new ForkOptions(childId));
    dbos.forkWorkflow(childId, 0, new ForkOptions(grandchildId));

    String key = UUID.randomUUID().toString();
    db().sendBulk(
        List.of(new SendMessage(parentId, "fan-out", null, key)),
        null,
        -1,
        "DBOS.sendBulk",
        true,
        null);

    // All three should have received the message
    assertEquals(1, db().getAllNotifications(parentId).size());
    assertEquals(1, db().getAllNotifications(childId).size());
    assertEquals(1, db().getAllNotifications(grandchildId).size());

    // Unrelated workflow receives nothing
    assertEquals(0, db().getAllNotifications(unrelatedId).size());

    // Message UUIDs follow the {key}::{dest} pattern — sending again is idempotent
    db().sendBulk(
        List.of(new SendMessage(parentId, "fan-out", null, key)),
        null,
        -1,
        "DBOS.sendBulk",
        true,
        null);
    assertEquals(1, db().getAllNotifications(parentId).size());
    assertEquals(1, db().getAllNotifications(childId).size());
    assertEquals(1, db().getAllNotifications(grandchildId).size());
  }
}
