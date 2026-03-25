package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Queue;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT)
public class ClientTest {

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

    try (var client = pgContainer.dbosClient()) {
      var options = new DBOSClient.EnqueueOptions("ClientServiceImpl", "enqueueTest", "testQueue");
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
    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    try (var client = pgContainer.dbosClient()) {
      var options =
          new DBOSClient.EnqueueOptions("ClientServiceImpl", "enqueueTest", "testQueue")
              .withDeduplicationId("plugh!");
      var handle = client.enqueueWorkflow(options, new Object[] {42, "spam"});
      assertNotNull(handle);

      assertThrows(
          RuntimeException.class, () -> client.enqueueWorkflow(options, new Object[] {17, "eggs"}));
    }
  }

  @Test
  public void clientSend() throws Exception {

    var handle = dbos.startWorkflow(() -> service.sendTest(42));

    var idempotencyKey = UUID.randomUUID().toString();

    try (var client = pgContainer.dbosClient()) {
      client.send(handle.workflowId(), "test.message", "test-topic", idempotencyKey);
    }

    assertEquals("42-test.message", handle.getResult());
  }

  @Test
  public void clientEnqueueTimeouts() throws Exception {
    try (var client = pgContainer.dbosClient()) {
      var options = new DBOSClient.EnqueueOptions("ClientServiceImpl", "sleep", "testQueue");

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

  @Test
  public void invalidSend() throws Exception {
    var invalidWorkflowId = UUID.randomUUID().toString();

    try (var client = pgContainer.dbosClient()) {
      var ex =
          assertThrows(
              DBOSNonExistentWorkflowException.class,
              () -> client.send(invalidWorkflowId, "test.message", null, null));
      assertTrue(ex.getMessage().contains(invalidWorkflowId));
    }
  }

  @Test
  public void clientListApplicationVersions() throws Exception {
    var sysdb = DBOSTestAccess.getSystemDatabase(dbos);
    sysdb.createApplicationVersion("v1.0.0");
    sysdb.createApplicationVersion("v2.0.0");

    try (var client = pgContainer.dbosClient()) {
      var versions = client.listApplicationVersions();
      assertEquals(2, versions.size());
      var names = versions.stream().map(v -> v.versionName()).toList();
      assertTrue(names.contains("v1.0.0"));
      assertTrue(names.contains("v2.0.0"));
    }
  }

  @Test
  public void clientGetLatestApplicationVersion() throws Exception {
    var sysdb = DBOSTestAccess.getSystemDatabase(dbos);
    sysdb.createApplicationVersion("v1.0.0");
    sysdb.createApplicationVersion("v2.0.0");
    sysdb.updateApplicationVersionTimestamp("v1.0.0", java.time.Instant.now().plusSeconds(60));

    try (var client = pgContainer.dbosClient()) {
      var latest = client.getLatestApplicationVersion();
      assertEquals("v1.0.0", latest.versionName());
    }
  }

  @Test
  public void clientSetLatestApplicationVersion() throws Exception {
    var sysdb = DBOSTestAccess.getSystemDatabase(dbos);
    sysdb.createApplicationVersion("v1.0.0");
    sysdb.createApplicationVersion("v2.0.0");

    try (var client = pgContainer.dbosClient()) {
      client.setLatestApplicationVersion("v1.0.0");
      var latest = client.getLatestApplicationVersion();
      assertEquals("v1.0.0", latest.versionName());
    }
  }
}
