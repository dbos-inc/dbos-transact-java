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
import dev.dbos.transact.exceptions.DBOSException;
import dev.dbos.transact.utils.DBUtils;

import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;

@Timeout(value = 2, unit = TimeUnit.MINUTES)
public class ClientTest {
  private static DBOSConfig dbosConfig;
  private static final String dbUrl = "jdbc:postgresql://localhost:5432/dbos_java_sys";
  private static final String dbUser = "postgres";
  private static final String dbPassword = System.getenv("PGPASSWORD");

  private DBOS dbos;
  private ClientService service;
  private HikariDataSource dataSource;

  @BeforeAll
  static void onetimeSetup() throws Exception {
    dbosConfig =
        new DBOSConfig.Builder()
            .name("systemdbtest")
            .dbHost("localhost")
            .dbPort(5432)
            .dbUser(dbUser)
            .sysDbName("dbos_java_sys")
            .maximumPoolSize(2)
            .build();
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    dbos = DBOS.initialize(dbosConfig);
    dbos.Queue("testQueue").build();
    service =
        dbos.<ClientService>Workflow()
            .interfaceClass(ClientService.class)
            .implementation(new ClientServiceImpl())
            .build();
    dbos.launch();

    dataSource = SystemDatabase.createDataSource(dbosConfig, null);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dataSource.close();
    dbos.shutdown();
  }

  @Test
  public void clientEnqueue() throws Exception {

    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    try (var client = new DBOSClient(dbUrl, dbUser, dbPassword)) {
      var options =
          new DBOSClient.EnqueueOptions("enqueueTest", "testQueue")
              .withClassName("dev.dbos.transact.client.ClientServiceImpl");
      var handle = client.enqueueWorkflow(options, new Object[] {42, "spam"});
      var rows = DBUtils.getWorkflowRows(dataSource);
      assertEquals(1, rows.size());
      var row = rows.get(0);
      assertEquals(handle.getWorkflowId(), row.workflowId());
      assertEquals("ENQUEUED", row.status());

      qs.unpause();

      var result = handle.getResult();
      assertTrue(result instanceof String);
      assertEquals("42-spam", result);

      var stat = client.getWorkflowStatus(handle.getWorkflowId());
      assertEquals("SUCCESS", stat.get().status());
    }
  }

  @Test
  public void clientEnqueueDeDupe() throws Exception {
    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    try (var client = new DBOSClient(dbUrl, dbUser, dbPassword)) {
      var options =
          new DBOSClient.EnqueueOptions("enqueueTest", "testQueue")
              .withClassName("dev.dbos.transact.client.ClientServiceImpl")
              .withDeduplicationId("plugh!");
      var handle = client.enqueueWorkflow(options, new Object[] {42, "spam"});
      assertNotNull(handle);

      assertThrows(
          DBOSException.class, () -> client.enqueueWorkflow(options, new Object[] {17, "eggs"}));
    }
  }

  @Test
  public void clientSend() throws Exception {

    var handle = dbos.startWorkflow(() -> service.sendTest(42));

    var idempotencyKey = UUID.randomUUID().toString();

    try (var client = new DBOSClient(dbUrl, dbUser, dbPassword)) {
      client.send(handle.getWorkflowId(), "test.message", "test-topic", idempotencyKey);
    }

    var workflowId = "%s-%s".formatted(handle.getWorkflowId(), idempotencyKey);
    var sendHandle = dbos.retrieveWorkflow(workflowId);
    assertEquals("SUCCESS", sendHandle.getStatus().status());

    assertEquals("42-test.message", handle.getResult());
  }
}
