package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Queue;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class ClientTest {
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

  @Test
  public void enqueueOptionsValidation() throws Exception {
    // workflow/class/queue names must not be null
    assertThrows(
        NullPointerException.class,
        () -> new DBOSClient.EnqueueOptions(null, "workflow-name", "queue-name"));
    assertThrows(
        NullPointerException.class,
        () -> new DBOSClient.EnqueueOptions("class-name", null, "queue-name"));
    assertThrows(
        NullPointerException.class,
        () -> new DBOSClient.EnqueueOptions("class-name", "workflow-name", null));

    // workflow/class/queue names must not be empty
    assertThrows(
        IllegalArgumentException.class,
        () -> new DBOSClient.EnqueueOptions("", "workflow-name", "queue-name"));
    assertThrows(
        IllegalArgumentException.class,
        () -> new DBOSClient.EnqueueOptions("class-name", "", "queue-name"));
    assertThrows(
        IllegalArgumentException.class,
        () -> new DBOSClient.EnqueueOptions("class-name", "workflow-name", ""));

    var options = new DBOSClient.EnqueueOptions("class", "workflow", "queue");

    // dedupe ID and partition key must not be empty if set
    assertThrows(IllegalArgumentException.class, () -> options.withDeduplicationId(""));
    assertThrows(IllegalArgumentException.class, () -> options.withQueuePartitionKey(""));

    // timeout can't be negative or zero
    assertThrows(IllegalArgumentException.class, () -> options.withTimeout(Duration.ZERO));
    assertThrows(IllegalArgumentException.class, () -> options.withTimeout(Duration.ofSeconds(-1)));

    // timeout & deadline can't both be set
    assertThrows(
        IllegalArgumentException.class,
        () ->
            options.withDeadline(Instant.now().plusSeconds(1)).withTimeout(Duration.ofSeconds(1)));
  }

  @Test
  public void clientEnqueue() throws Exception {

    var qs = DBOSTestAccess.getQueueService();
    qs.pause();

    try (var client = new DBOSClient(dbUrl, dbUser, dbPassword)) {
      var options =
          new DBOSClient.EnqueueOptions(
              "dev.dbos.transact.client.ClientServiceImpl", "enqueueTest", "testQueue");
      var handle = client.enqueueWorkflow(options, new Object[] {42, "spam"});
      var rows = DBUtils.getWorkflowRows(dataSource);
      assertEquals(1, rows.size());
      var row = rows.get(0);
      assertEquals(handle.workflowId(), row.workflowId());
      assertEquals("ENQUEUED", row.status());

      qs.unpause();

      var result = handle.getResult();
      assertTrue(result instanceof String);
      assertEquals("42-spam", result);

      var stat = client.getWorkflowStatus(handle.workflowId());
      assertEquals(
          "SUCCESS",
          stat.orElseThrow(() -> new AssertionError("Workflow status not found")).status());
    }
  }

  @Test
  public void clientEnqueueDeDupe() throws Exception {
    var qs = DBOSTestAccess.getQueueService();
    qs.pause();

    try (var client = new DBOSClient(dbUrl, dbUser, dbPassword)) {
      var options =
          new DBOSClient.EnqueueOptions(
                  "dev.dbos.transact.client.ClientServiceImpl", "enqueueTest", "testQueue")
              .withDeduplicationId("plugh!");
      var handle = client.enqueueWorkflow(options, new Object[] {42, "spam"});
      assertNotNull(handle);

      assertThrows(
          RuntimeException.class, () -> client.enqueueWorkflow(options, new Object[] {17, "eggs"}));
    }
  }

  @Test
  public void clientSend() throws Exception {

    var handle = DBOS.startWorkflow(() -> service.sendTest(42));

    var idempotencyKey = UUID.randomUUID().toString();

    try (var client = new DBOSClient(dbUrl, dbUser, dbPassword)) {
      client.send(handle.workflowId(), "test.message", "test-topic", idempotencyKey);
    }

    var workflowId = "%s-%s".formatted(handle.workflowId(), idempotencyKey);
    var sendHandle = DBOS.retrieveWorkflow(workflowId);
    assertNotNull(sendHandle);
    var status = sendHandle.getStatus();
    assertNotNull(status);
    assertEquals("SUCCESS", status.status());

    assertEquals("42-test.message", handle.getResult());
  }

  @Test
  public void clientEnqueueTimeouts() throws Exception {
    try (var client = new DBOSClient(dbUrl, dbUser, dbPassword)) {
      var options =
          new DBOSClient.EnqueueOptions(
              "dev.dbos.transact.client.ClientServiceImpl", "sleep", "testQueue");

      var handle1 =
          client.enqueueWorkflow(options.withTimeout(Duration.ofSeconds(1)), new Object[] {10000});
      assertThrows(
          DBOSAwaitedWorkflowCancelledException.class,
          () -> {
            handle1.getResult();
          });
      var stat1 = client.getWorkflowStatus(handle1.workflowId());
      assertEquals(
          "CANCELLED",
          stat1.orElseThrow(() -> new AssertionError("Workflow status not found")).status());

      var handle2 =
          client.enqueueWorkflow(
              options.withDeadline(Instant.ofEpochMilli(System.currentTimeMillis() + 1000)),
              new Object[] {10000});
      assertThrows(
          DBOSAwaitedWorkflowCancelledException.class,
          () -> {
            handle2.getResult();
          });
      var stat2 = client.getWorkflowStatus(handle2.workflowId());
      assertEquals(
          "CANCELLED",
          stat2.orElseThrow(() -> new AssertionError("Workflow status not found")).status());
    }
  }
}
