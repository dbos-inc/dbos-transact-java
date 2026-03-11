package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Queue;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
@org.junit.jupiter.api.parallel.Execution(org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT)
public class StartWorkflowTest {
  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS.Instance dbos;
  private HawkService proxy;
  private String localDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);

  @BeforeEach
  void beforeEachTest() {
    var dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS.Instance(dbosConfig);
    var impl = new HawkServiceImpl(dbos);
    proxy = dbos.registerWorkflows(HawkService.class, impl);
    impl.setProxy(proxy);

    dbos.registerQueues(
        new Queue("queue"), new Queue("partitioned-queue").withPartitionedEnabled(true));

    dbos.launch();
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
        dbos.startWorkflow(
            () -> {
              return proxy.simpleWorkflow();
            });
    var result = handle.getResult();
    assertEquals(localDate, result);

    var rows = dbos.listWorkflows(null);
    assertEquals(1, rows.size());
    var row = rows.get(0);
    assertEquals(handle.workflowId(), row.workflowId());
    assertEquals("SUCCESS", row.status());
  }

  @Test
  void startWorkflowWithWorkflowId() throws Exception {

    String workflowId = "startWorkflowWithWorkflowId";
    var options = new StartWorkflowOptions(workflowId);
    var handle = dbos.startWorkflow(() -> proxy.simpleWorkflow(), options);
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
    var handle = dbos.startWorkflow(() -> proxy.simpleWorkflow(), options);
    assertEquals(workflowId, handle.workflowId());
    var result = handle.getResult();
    assertEquals(localDate, result);

    var row = dbos.retrieveWorkflow(workflowId);
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
        () -> dbos.startWorkflow(() -> proxy.simpleWorkflow(), options));
  }

  @Test
  void missingPartitionKey() throws Exception {
    var options = new StartWorkflowOptions().withQueue("partitioned-queue");
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.startWorkflow(() -> proxy.simpleWorkflow(), options));
  }

  @Test
  void invalidPartitionKey() throws Exception {
    var options =
        new StartWorkflowOptions().withQueue("queue").withQueuePartitionKey("partition-key");
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.startWorkflow(() -> proxy.simpleWorkflow(), options));
  }
}
