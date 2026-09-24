package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.EnqueueOptions;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.QueueName;
import dev.dbos.transact.workflow.Timeout;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

public class EnqueueOptionsTest {
  @Test
  public void enqueueOptionsValidation() throws Exception {
    // empty strings not allowed
    assertThrows(
        IllegalArgumentException.class, () -> new EnqueueOptions("", QueueName.of("queue-name")));
    assertThrows(
        IllegalArgumentException.class, () -> new EnqueueOptions("wf-name", QueueName.of("")));
    assertThrows(
        IllegalArgumentException.class,
        () -> new EnqueueOptions("wf-name", "", QueueName.of("q-name")));
    assertThrows(
        IllegalArgumentException.class,
        () -> new EnqueueOptions("wf-name", "cls", "", QueueName.of("q-name")));
    assertThrows(
        IllegalArgumentException.class,
        () -> new EnqueueOptions("wf-name", QueueName.of("q-name")).withWorkflowId(""));
    assertThrows(
        IllegalArgumentException.class,
        () -> new EnqueueOptions("wf-name", QueueName.of("q-name")).withAppVersion(""));
    assertThrows(
        IllegalArgumentException.class,
        () -> new EnqueueOptions("wf-name", QueueName.of("q-name")).withDeduplicationId(""));
    assertThrows(
        IllegalArgumentException.class,
        () -> new EnqueueOptions("wf-name", QueueName.of("q-name")).withQueuePartitionKey(""));

    // zero or negative durations not allowed
    assertThrows(
        IllegalArgumentException.class,
        () -> new EnqueueOptions("wf-name", QueueName.of("q-name")).withTimeout(Duration.ZERO));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EnqueueOptions("wf-name", QueueName.of("q-name"))
                .withTimeout(Duration.ofSeconds(-1)));
    assertThrows(
        IllegalArgumentException.class,
        () -> new EnqueueOptions("wf-name", QueueName.of("q-name")).withDelay(Duration.ZERO));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EnqueueOptions("wf-name", QueueName.of("q-name"))
                .withDelay(Duration.ofSeconds(-1)));

    // 0 is the default priority; below it is refused
    assertThrows(
        IllegalArgumentException.class,
        () -> new EnqueueOptions("wf-name", QueueName.of("q-name")).withPriority(-1));
    assertEquals(
        0, new EnqueueOptions("wf-name", QueueName.of("q-name")).withPriority(0).priority());
  }

  /**
   * Each shorthand puts every argument in its own field; the queue can only go where it belongs.
   */
  @Test
  public void constructorsPlaceEachArgument() {
    var two = new EnqueueOptions("wf", QueueName.of("q"));
    assertEquals("wf", two.workflowName());
    assertNull(two.className());
    assertNull(two.instanceName());
    assertEquals("q", two.queueName());

    var three = new EnqueueOptions("wf", "cls", QueueName.of("q"));
    assertEquals("wf", three.workflowName());
    assertEquals("cls", three.className());
    assertNull(three.instanceName());
    assertEquals("q", three.queueName());

    var four = new EnqueueOptions("wf", "cls", "inst", QueueName.of("q"));
    assertEquals("wf", four.workflowName());
    assertEquals("cls", four.className());
    assertEquals("inst", four.instanceName());
    assertEquals("q", four.queueName());

    assertThrows(NullPointerException.class, () -> new EnqueueOptions("wf", (QueueName) null));
  }

  /**
   * Both names are required, so a null is refused at construction -- the runtime path would
   * otherwise write an ENQUEUED row with no workflow name, which nothing could ever run.
   */
  @Test
  public void requiredNamesMustNotBeNull() {
    assertThrows(NullPointerException.class, () -> new EnqueueOptions(null, QueueName.of("q")));
    assertThrows(
        NullPointerException.class,
        () ->
            new EnqueueOptions(
                "wf", null, null, null, null, null, null, null, null, null, null, null, null, null,
                null, null, null, null));
  }

  @Test
  @SuppressWarnings("removal") // builds a Queue value by hand; only the accessor is under test
  public void aQueueSuppliesItsNameAsAQueueName() {
    var queue = new Queue("orders");
    assertEquals(QueueName.of("orders"), queue.queueName());
    assertEquals("orders", new EnqueueOptions("wf", queue.queueName()).queueName());
  }

  /** A blank name is refused whichever constructor is used, as QueueName refuses one. */
  @Test
  public void blankNamesAreRefused() {
    assertThrows(IllegalArgumentException.class, () -> new EnqueueOptions("  ", QueueName.of("q")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new EnqueueOptions(
                "wf", null, null, "  ", null, null, null, null, null, null, null, null, null, null,
                null, null, null, null));
  }

  /**
   * Only an explicit timeout contradicts a deadline, and the record refuses it wherever it's built.
   */
  @Test
  public void anExplicitTimeoutAndADeadlineAreRefused() {
    var options = new EnqueueOptions("wf", QueueName.of("q")).withDeadline(Instant.now());
    assertThrows(IllegalArgumentException.class, () -> options.withTimeout(Duration.ofSeconds(1)));
    assertEquals(Timeout.none(), options.withNoTimeout().timeout());
    assertEquals(Timeout.inherit(), options.withTimeout(Timeout.inherit()).timeout());
  }
}
