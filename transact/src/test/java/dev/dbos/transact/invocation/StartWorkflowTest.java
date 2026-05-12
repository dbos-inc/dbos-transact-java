package dev.dbos.transact.invocation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.DBOSTestAccess;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.utils.PgContainer;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.WorkflowState;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AutoClose;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StartWorkflowTest {
  @AutoClose final PgContainer pgContainer = new PgContainer();
  @AutoClose DBOS dbos;
  private HawkService proxy;
  private String localDate = LocalDate.now().format(DateTimeFormatter.ISO_DATE);

  @BeforeEach
  void beforeEachTest() {
    var dbosConfig = pgContainer.dbosConfig();
    dbos = new DBOS(dbosConfig);
    var impl = new HawkServiceImpl(dbos);
    proxy = dbos.registerProxy(HawkService.class, impl);
    impl.setProxy(proxy);

    dbos.registerQueues(
        new Queue("queue"), new Queue("partitioned-queue").withPartitioningEnabled(true));

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
    assertEquals(WorkflowState.SUCCESS, row.status());
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
    assertEquals(WorkflowState.SUCCESS, row.status());
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
    assertEquals(WorkflowState.SUCCESS, row.getStatus().status());
    assertEquals(Duration.ofMillis(1000), row.getStatus().timeout());
    assertNotNull(row.getStatus().deadline());
  }

  @Test
  void timeoutAndDurationSetThrows() throws Exception {
    var options =
        new StartWorkflowOptions()
            .withTimeout(Duration.ofSeconds(10))
            .withDeadline(Instant.now().plus(Duration.ofDays(1)));
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.startWorkflow(() -> proxy.simpleWorkflow(), options));
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

  @Test
  void deduplicationIdWithoutQueue() {
    var options = new StartWorkflowOptions().withDeduplicationId("dedupe-id");
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.startWorkflow(() -> proxy.simpleWorkflow(), options));
  }

  @Test
  void priorityWithoutQueue() {
    var options = new StartWorkflowOptions().withPriority(5);
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.startWorkflow(() -> proxy.simpleWorkflow(), options));
  }

  @Test
  void queuePartitionKeyWithoutQueue() {
    var options = new StartWorkflowOptions().withQueuePartitionKey("partition-key");
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.startWorkflow(() -> proxy.simpleWorkflow(), options));
  }

  @Test
  void delayWithoutQueue() {
    var options = new StartWorkflowOptions().withDelay(Duration.ofSeconds(5));
    assertThrows(
        IllegalArgumentException.class,
        () -> dbos.startWorkflow(() -> proxy.simpleWorkflow(), options));
  }

  @Test
  void multipleQueueOptionsWithoutQueue() {
    var options =
        new StartWorkflowOptions()
            .withDeduplicationId("dedupe-id")
            .withPriority(1)
            .withDelay(Duration.ofSeconds(5));
    var ex =
        assertThrows(
            IllegalArgumentException.class,
            () -> dbos.startWorkflow(() -> proxy.simpleWorkflow(), options));
    assertTrue(ex.getMessage().contains("deduplicationId"));
    assertTrue(ex.getMessage().contains("priority"));
    assertTrue(ex.getMessage().contains("delay"));
  }

  @Test
  void startWorkflowWithDelayManualTransition() throws Exception {
    var qs = DBOSTestAccess.getQueueService(dbos);
    qs.pause();

    var delay = Duration.ofMillis(500);
    var handle =
        dbos.startWorkflow(
            () -> proxy.simpleWorkflow(),
            new StartWorkflowOptions().withQueue("queue").withDelay(delay));

    // Workflow should be DELAYED before the delay expires
    assertEquals(WorkflowState.DELAYED, handle.getStatus().status());

    // Wait until the delay has passed
    Thread.sleep(delay.toMillis() + 100);

    // Manually call transitionDelayedWorkflows, simulating what the paused QueueService would do
    var sysdb = DBOSTestAccess.getSystemDatabase(dbos);
    sysdb.transitionDelayedWorkflows();

    // Workflow should now be ENQUEUED, waiting for the QueueService to pick it up
    assertEquals(WorkflowState.ENQUEUED, handle.getStatus().status());

    // Unpause and let the workflow run
    qs.unpause();
    assertEquals(localDate, handle.getResult());
  }

  @Test
  void startWorkflowWithDelayRealPolling() throws Exception {
    long start = System.currentTimeMillis();
    var delay = Duration.ofSeconds(5);

    var handle =
        dbos.startWorkflow(
            () -> proxy.simpleWorkflow(),
            new StartWorkflowOptions().withQueue("queue").withDelay(delay));

    assertEquals(localDate, handle.getResult());

    long elapsed = System.currentTimeMillis() - start;
    assertTrue(
        elapsed >= delay.toMillis(),
        "Expected at least 5s delay but elapsed was " + elapsed + "ms");
  }
}
