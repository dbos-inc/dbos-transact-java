package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.utils.DBUtils.DBSettings;
import dev.dbos.transact.workflow.Queue;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class StartWorkflowTest {
  private HawkService proxy;
  private String localDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);

  private static final DBSettings db = DBSettings.get();
  private DBOSConfig dbosConfig;
  private HikariDataSource dataSource;

  @BeforeEach
  void beforeEachTest() throws SQLException {
    db.recreate();

    dataSource = db.dataSource();
    dbosConfig = DBOSConfig.defaults("systemdbtest").withDataSource(dataSource);

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
    assertNull(row.timeout());
    assertNull(row.deadline());
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
    assertThrows(
        IllegalArgumentException.class,
        () -> DBOS.startWorkflow(() -> proxy.simpleWorkflow(), options));
  }

  @Test
  void missingPartitionKey() throws Exception {
    var options = new StartWorkflowOptions().withQueue("partitioned-queue");
    assertThrows(
        IllegalArgumentException.class,
        () -> DBOS.startWorkflow(() -> proxy.simpleWorkflow(), options));
  }

  @Test
  void invalidPartitionKey() throws Exception {
    var options =
        new StartWorkflowOptions().withQueue("queue").withQueuePartitionKey("partition-key");
    assertThrows(
        IllegalArgumentException.class,
        () -> DBOS.startWorkflow(() -> proxy.simpleWorkflow(), options));
  }
}
