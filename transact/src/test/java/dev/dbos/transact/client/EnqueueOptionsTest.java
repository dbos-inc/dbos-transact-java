package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOSClient;

import java.time.Duration;

import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
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
}
