package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.workflow.SerializationStrategy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class EnqueueOptionsTest {
  @Test
  public void enqueueOptionsValidation() throws Exception {
    // empty strings not allowed
    assertThrows(
        IllegalArgumentException.class, () -> new DBOSClient.EnqueueOptions("", "queue-name"));
    assertThrows(
        IllegalArgumentException.class, () -> new DBOSClient.EnqueueOptions("wf-name", ""));
    assertThrows(
        IllegalArgumentException.class,
        () -> new DBOSClient.EnqueueOptions("wf-name", "q-name").withClassName(""));
    assertThrows(
        IllegalArgumentException.class,
        () -> new DBOSClient.EnqueueOptions("wf-name", "q-name").withInstanceName(""));
    assertThrows(
        IllegalArgumentException.class,
        () -> new DBOSClient.EnqueueOptions("wf-name", "q-name").withWorkflowId(""));
    assertThrows(
        IllegalArgumentException.class,
        () -> new DBOSClient.EnqueueOptions("wf-name", "q-name").withAppVersion(""));
    assertThrows(
        IllegalArgumentException.class,
        () -> new DBOSClient.EnqueueOptions("wf-name", "q-name").withDeduplicationId(""));
    assertThrows(
        IllegalArgumentException.class,
        () -> new DBOSClient.EnqueueOptions("wf-name", "q-name").withQueuePartitionKey(""));

    // zero or negative durations not allowed
    assertThrows(
        IllegalArgumentException.class,
        () -> new DBOSClient.EnqueueOptions("wf-name", "q-name").withTimeout(Duration.ZERO));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            new DBOSClient.EnqueueOptions("wf-name", "q-name").withTimeout(Duration.ofSeconds(-1)));
    assertThrows(
        IllegalArgumentException.class,
        () -> new DBOSClient.EnqueueOptions("wf-name", "q-name").withDelay(Duration.ZERO));
    assertThrows(
        IllegalArgumentException.class,
        () -> new DBOSClient.EnqueueOptions("wf-name", "q-name").withDelay(Duration.ofSeconds(-1)));
  }

  /**
   * The constructor omitting the application name exists so callers that predate system database
   * sharing keep compiling. It delegates positionally, so it is compared against the canonical
   * constructor to catch a mis-ordered delegation, which would otherwise compile cleanly.
   */
  @Test
  public void constructorWithoutAnApplicationNameEnqueuesForTheEnqueueingApplication() {
    var attributes = Map.<String, Object>of("key", "value");
    var roles = List.of("role");

    var actual =
        new DBOSClient.EnqueueOptions(
            "workflow-name",
            "class-name",
            "instance-name",
            "queue-name",
            "workflow-id",
            "app-version",
            Duration.ofSeconds(1),
            Instant.ofEpochSecond(2),
            "deduplication-id",
            3,
            "queue-partition-key",
            Duration.ofSeconds(4),
            SerializationStrategy.PORTABLE,
            "authenticated-user",
            "assumed-role",
            roles,
            attributes);

    var expected =
        new DBOSClient.EnqueueOptions(
            "workflow-name",
            "class-name",
            "instance-name",
            "queue-name",
            "workflow-id",
            "app-version",
            Duration.ofSeconds(1),
            Instant.ofEpochSecond(2),
            "deduplication-id",
            3,
            "queue-partition-key",
            Duration.ofSeconds(4),
            SerializationStrategy.PORTABLE,
            "authenticated-user",
            "assumed-role",
            roles,
            attributes,
            null);

    assertNull(actual.applicationName());
    assertEquals(expected, actual);
  }
}
