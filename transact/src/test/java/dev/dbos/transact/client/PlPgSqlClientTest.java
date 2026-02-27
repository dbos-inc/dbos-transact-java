package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSClient.EnqueueOptions;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Queue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
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
public class PlPgSqlClientTest {
  private static DBOSConfig dbosConfig;
  private static final String dbUrl = "jdbc:postgresql://localhost:5432/dbos_java_sys";
  private static final String dbUser = "postgres";
  private static final String dbPassword = System.getenv("PGPASSWORD");

  @SuppressWarnings("unused")
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
    DBOS.reinitialize(dbosConfig);
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

  String sendHelper(String destinationId, Object message, String topic, String idempotencyKey) throws Exception {
    var jsonMessage = MAPPER.writeValueAsString(message);

    String sql =
        "SELECT dbos.send_message(?, ?::json, ?, ?)";
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareCall(sql)) {

      stmt.setString(1, destinationId);
      stmt.setString(2, jsonMessage);
      stmt.setString(3, topic);
      stmt.setString(4, idempotencyKey);

      try (ResultSet rs = stmt.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      }
    }
  }

  String enqueueHelper(EnqueueOptions options, Object[] args) throws Exception {

    var jsonArgs = new String[args.length];
    for (int i = 0; i < args.length; i++) {
      jsonArgs[i] = MAPPER.writeValueAsString(args[i]);
    }

    String sql =
        "SELECT dbos.enqueue_workflow(?, ?, ?, ?::json, ?, ?, ?, ?, ?::bigint, ?::bigint, ?, ?::integer, ?, ?)";

    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareCall(sql)) {

      var argsArray = conn.createArrayOf("json", jsonArgs);
      var timeout = options.timeout() != null ? options.timeout().toMillis() : null;
      var deadline = options.deadline() != null ? options.deadline().toEpochMilli() : null;

      stmt.setString(1, options.workflowName());
      stmt.setString(2, options.queueName());
      stmt.setObject(3, argsArray);
      stmt.setObject(4, "{}"); // p_named_args
      stmt.setString(5, options.className());
      stmt.setString(6, options.instanceName());

      stmt.setString(7, options.workflowId());
      stmt.setString(8, options.appVersion());
      stmt.setObject(9, timeout);
      stmt.setObject(10, deadline);
      stmt.setString(11, options.deduplicationId());
      stmt.setObject(12, options.priority());
      stmt.setString(13, options.queuePartitionKey());
      stmt.setNull(14, Types.VARCHAR);

      try (ResultSet rs = stmt.executeQuery()) {
        return rs.next() ? rs.getString(1) : null;
      } finally {
        argsArray.free();
      }
    }
  }

  @Test
  public void clientEnqueue() throws Exception {

    var qs = DBOSTestAccess.getQueueService();
    qs.pause();

    var options = new DBOSClient.EnqueueOptions("ClientServiceImpl", "enqueueTest", "testQueue");
    var workflowId = enqueueHelper(options, new Object[] {"42", "spam"});
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

  @Test
  public void clientEnqueueDeDupe() throws Exception {
    var qs = DBOSTestAccess.getQueueService();
    qs.pause();

    var options =
        new DBOSClient.EnqueueOptions("ClientServiceImpl", "enqueueTest", "testQueue")
            .withDeduplicationId("plugh!");

    var workflowId = enqueueHelper(options, new Object[] {"42", "spam"});
    assertNotNull(workflowId);
    assertThrows(PSQLException.class, () -> enqueueHelper(options, new Object[] {"17", "eggs"}));
  }

  @Test
  public void clientEnqueueTimeouts() throws Exception {

    var options = new DBOSClient.EnqueueOptions("ClientServiceImpl", "sleep", "testQueue");

    var wfid1 = enqueueHelper(options.withTimeout(Duration.ofSeconds(1)), new Object[] {10000});
    var handle1 = DBOS.retrieveWorkflow(wfid1);
    assertThrows(
        DBOSAwaitedWorkflowCancelledException.class,
        () -> {
          handle1.getResult();
        });
    assertEquals("CANCELLED", handle1.getStatus().status());

    var wfid2 =
        enqueueHelper(
            options.withDeadline(Instant.ofEpochMilli(System.currentTimeMillis() + 1000)),
            new Object[] {10000});
    var handle2 = DBOS.retrieveWorkflow(wfid2);
    assertThrows(
        DBOSAwaitedWorkflowCancelledException.class,
        () -> {
          handle2.getResult();
        });
    assertEquals("CANCELLED", handle2.getStatus().status());
  }

  @Test
  public void clientSend() throws Exception {

    var handle = DBOS.startWorkflow(() -> service.sendTest(42));
    var idempotencyKey = UUID.randomUUID().toString();

    sendHelper(handle.workflowId(), "test.message", "test-topic", idempotencyKey);

    var workflowId = "%s-%s".formatted(handle.workflowId(), idempotencyKey);
    var sendHandle = DBOS.retrieveWorkflow(workflowId);
    assertNotNull(sendHandle);
    var status = sendHandle.getStatus();
    assertNotNull(status);
    assertEquals("SUCCESS", status.status());

    assertEquals("42-test.message", handle.getResult());
  }

}
