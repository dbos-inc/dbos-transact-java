package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class PgSqlClientTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @AutoClose final PgContainer pgContainer = new PgContainer();

  DBOSConfig dbosConfig;
  @AutoClose DBOS dbos;
  @AutoClose HikariDataSource dataSource;
  ClientService service;

  @BeforeEach
  void beforeEach() {
    dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
    dataSource = pgContainer.dataSource();

    dbos.registerQueue(new Queue("testQueue"));
    service = dbos.registerWorkflows(ClientService.class, new ClientServiceImpl(dbos));

    dbos.launch();
  }

  @Test
  public void clientEnqueue() throws Exception {
    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    String workflowId = enqueueWorkflow("enqueueTest", null, null, null, 42, "spam");
    assertNotNull(workflowId);

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertEquals(workflowId, row.workflowId());
    assertEquals(WorkflowState.ENQUEUED.name(), row.status());

    var handle = dbos.retrieveWorkflow(workflowId);

    qs.unpause();

    var result = handle.getResult();
    assertTrue(result instanceof String);
    assertEquals("42-spam", result);

    var stat = handle.getStatus();
    assertEquals(WorkflowState.SUCCESS, stat.status());
  }

  @Test
  public void clientEnqueueDeDupe() throws Exception {
    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    String workflowId = enqueueWorkflow("enqueueTest", "dedupe", null, null, 42, "spam");

    assertNotNull(workflowId);
    var ex =
        assertThrows(
            PSQLException.class,
            () -> enqueueWorkflow("enqueueTest", "dedupe", null, null, 17, "eggs"));
    assertTrue(ex.getMessage().startsWith("ERROR: DBOS queue duplicated"));
    assertTrue(
        ex.getMessage()
            .contains(" with queue testQueue and deduplication ID dedupe already exists"));
  }

  @Test
  public void clientEnqueueTimeout() throws Exception {
    var wfid1 = enqueueWorkflow("sleep", null, Duration.ofSeconds(1), null, 10000);
    var handle1 = dbos.retrieveWorkflow(wfid1);
    assertThrows(
        DBOSAwaitedWorkflowCancelledException.class,
        () -> {
          handle1.getResult();
        });
    assertEquals(WorkflowState.CANCELLED, handle1.getStatus().status());
  }

  @Test
  public void clientEnqueueDeadline() throws Exception {
    var wfid1 = enqueueWorkflow("sleep", null, null, Instant.now().plusMillis(1000), 10000);
    var handle1 = dbos.retrieveWorkflow(wfid1);
    assertThrows(
        DBOSAwaitedWorkflowCancelledException.class,
        () -> {
          handle1.getResult();
        });
    assertEquals(WorkflowState.CANCELLED, handle1.getStatus().status());
  }

  @Test
  public void clientSendWithIdempotencyKey() throws Exception {
    var handle = dbos.startWorkflow(() -> service.sendTest(42));
    var idempotencyKey = UUID.randomUUID().toString();

    sendMessage(handle.workflowId(), "test.message", "test-topic", idempotencyKey);
    assertEquals("42-test.message", handle.getResult());

    var notifications = DBUtils.getNotifications(dataSource, handle.workflowId());
    assertEquals(1, notifications.size());
    var notification = notifications.get(0);
    assertEquals(idempotencyKey, notification.messageUuid());
    assertTrue(notification.consumed());
  }

  @Test
  public void clientSendNoIdempotencyKey() throws Exception {
    var handle = dbos.startWorkflow(() -> service.sendTest(42));

    sendMessage(handle.workflowId(), "test.message", "test-topic", null);
    assertEquals("42-test.message", handle.getResult());

    var notifications = DBUtils.getNotifications(dataSource, handle.workflowId());
    assertEquals(1, notifications.size());
    var notification = notifications.get(0);
    assertNotNull(notification.messageUuid());
    assertDoesNotThrow(() -> UUID.fromString(notification.messageUuid()));
    assertTrue(notification.consumed());
  }

  @Test
  public void clientSendWithIdempotencyKeyTwice() throws Exception {
    var handle = dbos.startWorkflow(() -> service.sendTest(42));
    var idempotencyKey = UUID.randomUUID().toString();

    sendMessage(handle.workflowId(), "test.message", "test-topic", idempotencyKey);
    sendMessage(handle.workflowId(), "test.message", "test-topic", idempotencyKey);

    assertEquals("42-test.message", handle.getResult());

    var notifications = DBUtils.getNotifications(dataSource, handle.workflowId());
    assertEquals(1, notifications.size());
    var notification = notifications.get(0);
    assertEquals(idempotencyKey, notification.messageUuid());
    assertTrue(notification.consumed());
  }

  @Test
  public void invalidSend() throws Exception {
    var invalidWorkflowId = UUID.randomUUID().toString();

    var ex =
        assertThrows(
            PSQLException.class, () -> sendMessage(invalidWorkflowId, "test.message", null, null));
    var expected = "Destination workflow %s does not exist".formatted(invalidWorkflowId);
    assertTrue(ex.getMessage().startsWith("ERROR: DBOS non-existent workflow"));
    assertTrue(ex.getMessage().contains(expected));
  }

  String enqueueWorkflow(
      String workflowName, String dedupId, Duration timeout, Instant deadline, Object... args)
      throws SQLException {
    var jsonArgs =
        Arrays.stream(args)
            .map(
                o -> {
                  try {
                    return MAPPER.writeValueAsString(o);
                  } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                  }
                })
            .toArray(String[]::new);
    var sql =
        """
      SELECT dbos.enqueue_workflow(
        workflow_name => ?,
        class_name => ?,
        queue_name => ?,
        positional_args => ?,
        deduplication_id => ?,
        timeout_ms => ?,
        deadline_epoch_ms => ?)
      """;
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareCall(sql)) {
      Long timeoutMS = timeout == null ? null : timeout.toMillis();
      Long deadlineMS = deadline == null ? null : deadline.toEpochMilli();
      stmt.setString(1, Objects.requireNonNull(workflowName));
      stmt.setString(2, "ClientServiceImpl");
      stmt.setString(3, "testQueue");
      stmt.setString(5, dedupId);
      stmt.setObject(6, timeoutMS, Types.BIGINT);
      stmt.setObject(7, deadlineMS, Types.BIGINT);

      var argsArray = conn.createArrayOf("json", jsonArgs);
      stmt.setObject(4, argsArray);
      try (ResultSet rs = stmt.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      } finally {
        argsArray.free();
      }
    }
  }

  void sendMessage(String destinationId, Object message, String topic, String idempotencyKey)
      throws SQLException, JsonProcessingException {
    String jsonMessage = MAPPER.writeValueAsString(Objects.requireNonNull(message));

    var sql = "SELECT dbos.send_message(?, ?::json, topic => ?, idempotency_key => ?)";
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareCall(sql)) {
      stmt.setString(1, Objects.requireNonNull(destinationId));
      stmt.setString(2, jsonMessage);
      stmt.setString(3, topic);
      stmt.setString(4, idempotencyKey);
      stmt.execute();
    }
  }
}
