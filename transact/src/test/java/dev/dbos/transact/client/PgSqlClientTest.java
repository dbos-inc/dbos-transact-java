package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.execution.ThrowingRunnable;
import dev.dbos.transact.execution.ThrowingSupplier;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Queue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PSQLException;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class PgSqlClientTest {
  private static DBOSConfig dbosConfig;
  private static final String dbUrl = "jdbc:postgresql://localhost:5432/dbos_java_sys";
  private static final String dbUser = "postgres";
  private static final String dbPassword = System.getenv("PGPASSWORD");

  private ClientService service;

  private HikariDataSource dataSource;

  @BeforeAll
  static void onetimeSetup() throws Exception {
    dbosConfig =
        DBOSConfig.defaults("systemdbtest")
            .withDatabaseUrl(dbUrl)
            .withDbUser(dbUser)
            .withDbPassword(dbPassword);
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    DBOSTestAccess.reinitialize(dbosConfig);
    DBOS.registerQueue(new Queue("testQueue"));
    service = DBOS.registerWorkflows(ClientService.class, new ClientServiceImpl());
    DBOS.launch();

    dataSource =
        SystemDatabase.createDataSource(
            dbosConfig.databaseUrl(), dbosConfig.dbUser(), dbosConfig.dbPassword());
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dataSource.close();
    DBOS.shutdown();
  }

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void clientEnqueue() throws Exception {

    var qs = DBOSTestAccess.getQueueService();
    qs.pause();

    ThrowingSupplier<String, Exception> enqueueSupplier =
        () -> {
          var sql = "SELECT dbos.enqueue_workflow(?, ?, ?, class_name => ?)";
          try (var conn = dataSource.getConnection();
              var stmt = conn.prepareCall(sql)) {
            stmt.setString(1, "enqueueTest");
            stmt.setString(2, "testQueue");
            var argsArray =
                conn.createArrayOf(
                    "json",
                    new String[] {
                      MAPPER.writeValueAsString("42"), MAPPER.writeValueAsString("spam")
                    });
            stmt.setObject(3, argsArray);
            stmt.setString(4, "ClientServiceImpl");
            try (ResultSet rs = stmt.executeQuery()) {
              return rs.next() ? rs.getString(1) : null;
            } finally {
              argsArray.free();
            }
          }
        };

    String workflowId = enqueueSupplier.execute();
    assertNotNull(workflowId);

    var rows = DBUtils.getWorkflowRows(dataSource);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertEquals(workflowId, row.workflowId());
    assertEquals("ENQUEUED", row.status());

    var handle = DBOS.retrieveWorkflow(workflowId);

    qs.unpause();

    var result = handle.getResult();
    assertTrue(result instanceof String);
    assertEquals("42-spam", result);

    var stat = handle.getStatus();
    assertEquals("SUCCESS", stat.status());
  }

  @FunctionalInterface
  public interface ThrowingFunction<P, T, E extends Exception> {
    T execute(P param) throws E;
  }

  @Test
  public void clientEnqueueInvalidWorkflowName() throws Exception {
    final var workflowId = UUID.randomUUID().toString();

    ThrowingFunction<String, String, Exception> enqueueSupplier =
        (workflowName) -> {
          var sql = "SELECT dbos.enqueue_workflow(?, ?, ?, workflow_id => ?, class_name => ?)";
          try (var conn = dataSource.getConnection();
              var stmt = conn.prepareCall(sql)) {
            stmt.setString(1, workflowName);
            stmt.setString(2, "testQueue");
            var argsArray =
                conn.createArrayOf(
                    "json",
                    new String[] {
                      MAPPER.writeValueAsString("42"), MAPPER.writeValueAsString("spam")
                    });
            stmt.setObject(3, argsArray);
            stmt.setString(4, workflowId);
            stmt.setString(5, "ClientServiceImpl");
            try (ResultSet rs = stmt.executeQuery()) {
              return rs.next() ? rs.getString(1) : null;
            } finally {
              argsArray.free();
            }
          }
        };

    String retValue = enqueueSupplier.execute("enqueueTest");
    assertEquals(workflowId, retValue);

    var ex = assertThrows(PSQLException.class, () -> enqueueSupplier.execute("wrong name"));
    assertTrue(ex.getMessage().startsWith("ERROR: Conflicting DBOS workflow name"));
    assertTrue(
        ex.getMessage()
            .contains(
                "Workflow %s exists with name %s, but the provided workflow name is: %s"
                    .formatted(workflowId, "enqueueTest", "wrong name")));
  }

  @Test
  public void clientEnqueueDeDupe() throws Exception {
    var qs = DBOSTestAccess.getQueueService();
    qs.pause();

    ThrowingSupplier<String, Exception> enqueueSupplier =
        () -> {
          var sql = "SELECT dbos.enqueue_workflow(?, ?, ?, class_name => ?, deduplication_id => ?)";
          try (var conn = dataSource.getConnection();
              var stmt = conn.prepareCall(sql)) {
            stmt.setString(1, "enqueueTest");
            stmt.setString(2, "testQueue");
            var argsArray =
                conn.createArrayOf(
                    "json",
                    new String[] {
                      MAPPER.writeValueAsString("42"), MAPPER.writeValueAsString("spam")
                    });
            stmt.setObject(3, argsArray);
            stmt.setString(4, "ClientServiceImpl");
            stmt.setString(5, "plugh!");
            try (ResultSet rs = stmt.executeQuery()) {
              return rs.next() ? rs.getString(1) : null;
            } finally {
              argsArray.free();
            }
          }
        };

    String workflowId = enqueueSupplier.execute();

    assertNotNull(workflowId);
    var ex = assertThrows(PSQLException.class, () -> enqueueSupplier.execute());
    assertTrue(ex.getMessage().startsWith("ERROR: DBOS queue duplicated"));
    assertTrue(
        ex.getMessage()
            .contains(" with queue testQueue and deduplication ID plugh! already exists"));
  }

  @Test
  public void clientEnqueueTimeout() throws Exception {

    ThrowingSupplier<String, Exception> enqueueSupplier =
        () -> {
          var sql = "SELECT dbos.enqueue_workflow(?, ?, ?, class_name => ?, timeout_ms => ?)";
          try (var conn = dataSource.getConnection();
              var stmt = conn.prepareCall(sql)) {
            stmt.setString(1, "sleep");
            stmt.setString(2, "testQueue");
            var argsArray =
                conn.createArrayOf("json", new String[] {MAPPER.writeValueAsString(10000)});
            stmt.setObject(3, argsArray);
            stmt.setString(4, "ClientServiceImpl");
            stmt.setLong(5, Duration.ofSeconds(1).toMillis());
            try (ResultSet rs = stmt.executeQuery()) {
              return rs.next() ? rs.getString(1) : null;
            } finally {
              argsArray.free();
            }
          }
        };

    var wfid1 = enqueueSupplier.execute();
    var handle1 = DBOS.retrieveWorkflow(wfid1);
    assertThrows(
        DBOSAwaitedWorkflowCancelledException.class,
        () -> {
          handle1.getResult();
        });
    assertEquals("CANCELLED", handle1.getStatus().status());
  }

  @Test
  public void clientEnqueueDeadline() throws Exception {

    ThrowingSupplier<String, Exception> enqueueSupplier =
        () -> {
          var sql =
              "SELECT dbos.enqueue_workflow(?, ?, ?, class_name => ?, deadline_epoch_ms => ?)";
          try (var conn = dataSource.getConnection();
              var stmt = conn.prepareCall(sql)) {
            stmt.setString(1, "sleep");
            stmt.setString(2, "testQueue");
            var argsArray =
                conn.createArrayOf("json", new String[] {MAPPER.writeValueAsString(10000)});
            stmt.setObject(3, argsArray);
            stmt.setString(4, "ClientServiceImpl");
            stmt.setLong(5, Instant.ofEpochMilli(System.currentTimeMillis() + 1000).toEpochMilli());
            try (ResultSet rs = stmt.executeQuery()) {
              return rs.next() ? rs.getString(1) : null;
            } finally {
              argsArray.free();
            }
          }
        };

    var wfid1 = enqueueSupplier.execute();
    var handle1 = DBOS.retrieveWorkflow(wfid1);
    assertThrows(
        DBOSAwaitedWorkflowCancelledException.class,
        () -> {
          handle1.getResult();
        });
    assertEquals("CANCELLED", handle1.getStatus().status());
  }

  @Test
  public void clientSendWithIdempotencyKey() throws Exception {

    var handle = DBOS.startWorkflow(() -> service.sendTest(42));
    var idempotencyKey = UUID.randomUUID().toString();

    ThrowingRunnable<Exception> sendAction =
        () -> {
          var sql = "SELECT dbos.send_message(?, ?::json, topic => ?, idempotency_key => ?)";
          try (var conn = dataSource.getConnection();
              var stmt = conn.prepareCall(sql)) {
            stmt.setString(1, handle.workflowId());
            stmt.setString(2, MAPPER.writeValueAsString("test.message"));
            stmt.setString(3, "test-topic");
            stmt.setString(4, idempotencyKey);
            stmt.execute();
          }
        };

    sendAction.execute();
    assertEquals("42-test.message", handle.getResult());

    var notifications = DBUtils.getNotifications(dataSource, handle.workflowId());
    assertEquals(1, notifications.size());
    var notification = notifications.get(0);
    assertEquals(idempotencyKey, notification.messageUuid());
    assertTrue(notification.consumed());
  }

  @Test
  public void clientSendNoIdempotencyKey() throws Exception {

    var handle = DBOS.startWorkflow(() -> service.sendTest(42));

    ThrowingRunnable<Exception> sendAction =
        () -> {
          var sql = "SELECT dbos.send_message(?, ?::json, topic => ?)";
          try (var conn = dataSource.getConnection();
              var stmt = conn.prepareCall(sql)) {
            stmt.setString(1, handle.workflowId());
            stmt.setString(2, MAPPER.writeValueAsString("test.message"));
            stmt.setString(3, "test-topic");
            stmt.execute();
          }
        };

    sendAction.execute();
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

    var handle = DBOS.startWorkflow(() -> service.sendTest(42));
    var idempotencyKey = UUID.randomUUID().toString();

    ThrowingRunnable<Exception> sendAction =
        () -> {
          var sql = "SELECT dbos.send_message(?, ?::json, topic => ?, idempotency_key => ?)";
          try (var conn = dataSource.getConnection();
              var stmt = conn.prepareCall(sql)) {
            stmt.setString(1, handle.workflowId());
            stmt.setString(2, MAPPER.writeValueAsString("test.message"));
            stmt.setString(3, "test-topic");
            stmt.setString(4, idempotencyKey);
            stmt.execute();
          }
        };

    sendAction.execute();
    sendAction.execute();

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

    ThrowingRunnable<Exception> sendAction =
        () -> {
          var sql = "SELECT dbos.send_message(?, ?::json)";
          try (var conn = dataSource.getConnection();
              var stmt = conn.prepareCall(sql)) {
            stmt.setString(1, invalidWorkflowId);
            stmt.setString(2, MAPPER.writeValueAsString("test.message"));
            stmt.execute();
          }
        };

    var ex = assertThrows(PSQLException.class, sendAction::execute);
    var expected = "Destination workflow %s does not exist".formatted(invalidWorkflowId);
    assertTrue(ex.getMessage().startsWith("ERROR: DBOS non-existent workflow"));
    assertTrue(ex.getMessage().contains(expected));
  }
}
