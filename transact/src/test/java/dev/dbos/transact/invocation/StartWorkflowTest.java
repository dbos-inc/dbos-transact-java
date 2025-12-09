package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils;
import dev.dbos.transact.workflow.Queue;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = TimeUnit.MINUTES)
public class StartWorkflowTest {
  private static DBOSConfig dbosConfig;
  private HawkService proxy;
  private HikariDataSource dataSource;
  private String localDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);

  @BeforeAll
  static void onetimeSetup() throws Exception {

    dbosConfig =
        DBOSConfig.defaultsFromEnv("systemdbtest")
            .withDatabaseUrl("jdbc:postgresql://localhost:5432/dbos_java_sys")
            .withMaximumPoolSize(2);
  }

  @BeforeEach
  void beforeEachTest() throws SQLException {
    DBUtils.recreateDB(dbosConfig);
    DBOS.reinitialize(dbosConfig);
    var impl = new HawkServiceImpl();
    proxy = DBOS.registerWorkflows(HawkService.class, impl);
    impl.setProxy(proxy);

    DBOS.registerQueue(new Queue("queue"));
    DBOS.registerQueue(new Queue("partitioned-queue").withPartitionedEnabled(true));

    DBOS.launch();

    dataSource = SystemDatabase.createDataSource(dbosConfig);
  }

  @AfterEach
  void afterEachTest() throws Exception {
    dataSource.close();
    DBOS.shutdown();
  }

  @Test
  public void startWorkflowOptionsValidation() throws Exception {

    var options = new StartWorkflowOptions().withQueue("queue-name");

    // dedupe ID and partition key must not be empty if set
    assertThrows(IllegalArgumentException.class, () -> options.withDeduplicationId(""));
    assertThrows(IllegalArgumentException.class, () -> options.withQueuePartitionKey(""));

    // dedupe ID and partition key must not both be set
    assertThrows(
        IllegalArgumentException.class,
        () -> options.withDeduplicationId("dedupe-id").withQueuePartitionKey("partion-key"));

    // queue name must be set if partition key is set
    assertThrows(
        IllegalArgumentException.class,
        () -> new StartWorkflowOptions().withQueuePartitionKey("partion-key"));

    // negative timeout
    assertThrows(IllegalArgumentException.class, () -> options.withTimeout(Duration.ofSeconds(-1)));

    // timeout & deadline both set
    assertThrows(
        IllegalArgumentException.class,
        () ->
            options.withDeadline(Instant.now().plusSeconds(1)).withTimeout(Duration.ofSeconds(1)));
  }

  @Test
  void startWorkflow() throws Exception {
    var handle =
        DBOS.startWorkflow(
            () -> {
              return proxy.simpleWorkflow();
            });
    var result = handle.getResult();
    assertEquals(localDate, result);

    var rows = DBOS.listWorkflows(null);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertEquals(handle.workflowId(), row.workflowId());
    assertEquals("SUCCESS", row.status());
  }

  @Test
  void startWorkflowWithWorkflowId() throws Exception {

    String workflowId = "startWorkflowWithWorkflowId";
    var options = new StartWorkflowOptions(workflowId);
    var handle = DBOS.startWorkflow(() -> proxy.simpleWorkflow(), options);
    assertEquals(workflowId, handle.workflowId());
    var result = handle.getResult();
    assertEquals(localDate, result);

    var row = handle.getStatus();
    assertNotNull(row);
    assertEquals(workflowId, row.workflowId());
    assertEquals("SUCCESS", row.status());
    assertNull(row.getTimeout());
    assertNull(row.getDeadline());
  }

  @Test
  void startWorkflowWithTimeout() throws Exception {

    String workflowId = "startWorkflowWithTimeout";
    var options = new StartWorkflowOptions(workflowId).withTimeout(1, TimeUnit.SECONDS);
    var handle = DBOS.startWorkflow(() -> proxy.simpleWorkflow(), options);
    assertEquals(workflowId, handle.workflowId());
    var result = handle.getResult();
    assertEquals(localDate, result);

    var row = DBOS.retrieveWorkflow(workflowId);
    assertNotNull(row);
    assertEquals(workflowId, row.workflowId());
    assertEquals("SUCCESS", row.getStatus().status());
    assertEquals(1000, row.getStatus().timeoutMs());
    assertNotNull(row.getStatus().deadlineEpochMs());
  }

  @Test
  void invalidQueue() throws Exception {
    var options = new StartWorkflowOptions().withQueue("invalid-queue-name");
    assertThrows(IllegalArgumentException.class, () -> DBOS.startWorkflow(() -> proxy.simpleWorkflow(), options));
  }

  @Test
  void missingPartitionKey() throws Exception {
    var options = new StartWorkflowOptions().withQueue("partitioned-queue");
    assertThrows(IllegalArgumentException.class, () -> DBOS.startWorkflow(() -> proxy.simpleWorkflow(), options));
  }

  @Test
  void invalidPartitionKey() throws Exception {
    var options = new StartWorkflowOptions().withQueue("queue").withQueuePartitionKey("partiton-key");
    assertThrows(IllegalArgumentException.class, () -> DBOS.startWorkflow(() -> proxy.simpleWorkflow(), options));
  }
}
