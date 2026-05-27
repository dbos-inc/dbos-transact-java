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
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

interface BulkService {
  String block(String topic);

  void sendBulkWorkflow(List<SendMessage> messages);
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

  @Workflow
  @Override
  public void sendBulkWorkflow(List<SendMessage> messages) {
    dbos.sendBulk(messages);
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

  /** Starts a blocking recv workflow, providing a valid workflow_status row as a destination. */
  private String startDest(String id) throws Exception {
    dbos.startWorkflow(() -> service.block(id), new StartWorkflowOptions(id));
    return id;
  }

  // -------------------------------------------------------------------------
  // testSendBulkEmpty
  // -------------------------------------------------------------------------

  @Test
  void testSendBulkEmpty() {
    // Outside workflow: no-op
    assertDoesNotThrow(() -> dbos.sendBulk(List.of()));

    // Inside workflow: also a no-op, no step recorded
    String wfId = "empty-wf-" + UUID.randomUUID();
    dbos.startWorkflow(() -> service.sendBulkWorkflow(List.of()), new StartWorkflowOptions(wfId))
        .getResult();
    assertEquals(0, dbos.listWorkflowSteps(wfId).size());
  }

  // -------------------------------------------------------------------------
  // testSendBulk
  // -------------------------------------------------------------------------

  @Test
  void testSendBulk() throws Exception {
    String dest1 = startDest("dest1-" + UUID.randomUUID());
    String dest2 = startDest("dest2-" + UUID.randomUUID());

    // Multi-message to one dest + one message to another, all atomic
    dbos.sendBulk(
        List.of(
            new SendMessage(dest1, "hello-1"),
            new SendMessage(dest1, "hello-2"),
            new SendMessage(dest2, "world")));

    assertEquals(2, db().getAllNotifications(dest1).size());
    assertEquals(1, db().getAllNotifications(dest2).size());
    assertEquals("world", db().getAllNotifications(dest2).get(0).message());

    // Non-existent destination rolls back the entire batch
    String validDest = startDest("valid-" + UUID.randomUUID());
    assertThrows(
        DBOSNonExistentWorkflowException.class,
        () ->
            dbos.sendBulk(
                List.of(
                    new SendMessage(validDest, "first"),
                    new SendMessage("nonexistent-" + UUID.randomUUID(), "second"))));
    assertEquals(0, db().getAllNotifications(validDest).size());
  }

  // -------------------------------------------------------------------------
  // testSendBulkFromWorkflow
  // -------------------------------------------------------------------------

  @Test
  void testSendBulkFromWorkflow() throws Exception {
    String dest = startDest("wf-dest-" + UUID.randomUUID());

    // Run the sendBulk workflow and wait for it to complete
    String senderWfId = "sender-wf-" + UUID.randomUUID();
    var handle =
        dbos.startWorkflow(
            () -> service.sendBulkWorkflow(List.of(new SendMessage(dest, "from-workflow"))),
            new StartWorkflowOptions(senderWfId));
    handle.getResult();

    assertEquals(1, db().getAllNotifications(dest).size());

    // The entire bulk send is recorded as exactly one step
    List<StepInfo> steps = dbos.listWorkflowSteps(senderWfId);
    assertEquals(1, steps.size());
    assertEquals("DBOS.sendBulk", steps.get(0).functionName());
  }

  // -------------------------------------------------------------------------
  // testSendBulkIdempotencyKey
  // -------------------------------------------------------------------------

  @Test
  void testSendBulkIdempotencyKey() throws Exception {
    String dest = startDest("idem-dest-" + UUID.randomUUID());
    String key = UUID.randomUUID().toString();

    var messages = List.of(new SendMessage(dest, "hello", null, key));
    dbos.sendBulk(messages);
    dbos.sendBulk(messages);

    // Only one notification despite two calls
    assertEquals(1, db().getAllNotifications(dest).size());
  }

  // -------------------------------------------------------------------------
  // testSendBulkDuplicateKeyWithinBatch
  // -------------------------------------------------------------------------

  @Test
  void testSendBulkDuplicateKeyWithinBatch() throws Exception {
    String dest = startDest("dupkey-dest-" + UUID.randomUUID());
    String key = UUID.randomUUID().toString();

    assertThrows(
        IllegalArgumentException.class,
        () ->
            dbos.sendBulk(
                List.of(
                    new SendMessage(dest, "msg1", null, key),
                    new SendMessage(dest, "msg2", null, key))));

    // Nothing was delivered
    assertEquals(0, db().getAllNotifications(dest).size());
  }

  // -------------------------------------------------------------------------
  // testSendBulkSendToForks
  // -------------------------------------------------------------------------

  @Test
  void testSendBulkSendToForks() throws Exception {
    String parentId = "parent-" + UUID.randomUUID();
    String childId = "child-" + UUID.randomUUID();
    String grandchildId = "grandchild-" + UUID.randomUUID();
    String unrelatedId = "unrelated-" + UUID.randomUUID();

    startDest(parentId);
    startDest(unrelatedId);
    dbos.forkWorkflow(parentId, 0, new ForkOptions(childId));
    dbos.forkWorkflow(childId, 0, new ForkOptions(grandchildId));

    String key = UUID.randomUUID().toString();
    dbos.sendBulk(List.of(new SendMessage(parentId, "fan-out", null, key)), true);

    // Fan-out reaches all descendants
    assertEquals(1, db().getAllNotifications(parentId).size());
    assertEquals(1, db().getAllNotifications(childId).size());
    assertEquals(1, db().getAllNotifications(grandchildId).size());

    // Unrelated workflow untouched
    assertEquals(0, db().getAllNotifications(unrelatedId).size());

    // Re-send with same key is idempotent (ON CONFLICT DO NOTHING via {key}::{dest} UUIDs)
    dbos.sendBulk(List.of(new SendMessage(parentId, "fan-out", null, key)), true);
    assertEquals(1, db().getAllNotifications(parentId).size());
    assertEquals(1, db().getAllNotifications(childId).size());
    assertEquals(1, db().getAllNotifications(grandchildId).size());
  }
}
