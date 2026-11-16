package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.utils.PgContainer;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A workflow another SDK parked as SUSPENDED is in flight here too: listings include it, a caller
 * keeps waiting on it, a rewind refuses it, and a send wakes it. The rows are written by hand,
 * since Java does not park workflows itself.
 */
public class SuspendedStatusTest {

  /** Far enough ahead that only a wake can make a parked receiver due. */
  private static final long PARKED = 9_000_000_000_000L;

  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose HikariDataSource dataSource;
  @AutoClose DBOS dbos;

  @BeforeEach
  void beforeEach() {
    dbos = new DBOS(pgContainer.dbosConfig());
    dbos.launch();
    dataSource = pgContainer.dataSource();
  }

  private void plant(String id, String status, String queue, String suspendKey)
      throws SQLException {
    var sql =
        """
          INSERT INTO "dbos".workflow_status
              (workflow_uuid, status, name, class_name, queue_name, suspend_key,
               delay_until_epoch_ms, executor_id, recovery_attempts, priority)
          VALUES (?, ?, 'parked', 'com.example.Parked', ?, ?, ?, 'other-sdk', 0, 0)
        """;
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, id);
      stmt.setString(2, status);
      stmt.setString(3, queue);
      stmt.setString(4, suspendKey);
      stmt.setLong(5, PARKED);
      stmt.executeUpdate();
    }
  }

  private long delayUntil(String id) throws SQLException {
    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "SELECT delay_until_epoch_ms FROM \"dbos\".workflow_status WHERE workflow_uuid = ?")) {
      stmt.setString(1, id);
      try (var rs = stmt.executeQuery()) {
        assertTrue(rs.next());
        return rs.getLong(1);
      }
    }
  }

  @Test
  void suspendedIsInFlight() {
    assertTrue(WorkflowState.SUSPENDED.isActive());
    assertEquals(WorkflowState.SUSPENDED, WorkflowState.valueOf("SUSPENDED"));
  }

  @Test
  void aSuspendedWorkflowIsInTheDefaultQueueListing() throws Exception {
    plant("suspended", "SUSPENDED", "some-queue", "topic");
    plant("finished", "SUCCESS", "some-queue", null);

    var listed =
        dbos.listWorkflows(new ListWorkflowsInput().withQueuesOnly(true)).stream()
            .map(WorkflowStatus::workflowId)
            .toList();

    assertTrue(listed.contains("suspended"), "a SUSPENDED workflow is in flight: " + listed);
    assertFalse(listed.contains("finished"), "the default listing is of in-flight workflows only");
    assertEquals(
        WorkflowState.SUSPENDED,
        dbos.listWorkflows(new ListWorkflowsInput().withWorkflowIds("suspended")).get(0).status());
  }

  @Test
  void getResultKeepsWaitingOnASuspendedWorkflow() throws Exception {
    plant("waited-on", "SUSPENDED", null, "topic");
    WorkflowHandle<Object, Exception> handle = dbos.retrieveWorkflow("waited-on");

    var result =
        CompletableFuture.supplyAsync(
            () -> {
              try {
                return handle.getResult();
              } catch (Exception e) {
                throw new IllegalStateException(e);
              }
            });
    Thread.sleep(2_000);
    assertFalse(result.isDone(), "getResult returned for a workflow that is still in flight");

    try (var conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "UPDATE \"dbos\".workflow_status SET status = 'CANCELLED' WHERE workflow_uuid = ?")) {
      stmt.setString(1, "waited-on");
      stmt.executeUpdate();
    }
    var thrown = assertThrows(ExecutionException.class, () -> result.get(30, TimeUnit.SECONDS));
    assertInstanceOf(DBOSAwaitedWorkflowCancelledException.class, thrown.getCause().getCause());
  }

  @Test
  void aSuspendedWorkflowCannotBeRewound() throws Exception {
    plant("parked", "SUSPENDED", null, "topic");

    var thrown =
        assertThrows(
            IllegalStateException.class,
            () -> dbos.rewindWorkflow("parked", 1, new RewindOptions()));
    assertTrue(thrown.getMessage().contains("SUSPENDED"), thrown.getMessage());
  }

  @Test
  void aSendWakesOnlyASuspendedReceiverOnItsTopic() throws Exception {
    plant("waiting", "SUSPENDED", null, "topic");
    plant("waiting-null-topic", "SUSPENDED", null, "__null__topic__");
    plant("other-topic", "SUSPENDED", null, "elsewhere");
    plant("not-suspended", "PENDING", null, "topic");
    plant("not-addressed", "SUSPENDED", null, "topic");
    plant("no-key", "SUSPENDED", null, null);

    dbos.sendBulk(
        List.of(
            new SendMessage("waiting", "hello", "topic", null),
            new SendMessage("waiting-null-topic", "hello", null, null),
            new SendMessage("other-topic", "hello", "topic", null),
            new SendMessage("not-suspended", "hello", "topic", null),
            new SendMessage("no-key", "hello", "topic", null)));

    assertEquals(0L, delayUntil("waiting"));
    assertEquals(0L, delayUntil("waiting-null-topic"));
    assertEquals(PARKED, delayUntil("other-topic"));
    assertEquals(PARKED, delayUntil("not-suspended"));
    assertEquals(PARKED, delayUntil("not-addressed"));
    assertEquals(PARKED, delayUntil("no-key"));
  }

  @Test
  void aSendFromTheClientWakesASuspendedReceiver() throws Exception {
    plant("waiting", "SUSPENDED", null, "topic");

    try (var client = pgContainer.dbosClient()) {
      client.send("waiting", "hello", "topic", null);
    }

    assertEquals(0L, delayUntil("waiting"));
  }
}
