package dev.dbos.transact.client;

import static org.junit.jupiter.api.Assertions.assertThrows;

import dev.dbos.transact.DBOSClient;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

@org.junit.jupiter.api.Timeout(value = 2, unit = java.util.concurrent.TimeUnit.MINUTES)
public class EnqueueOptionsTest {
  @Test
  public void enqueueOptionsValidation() throws Exception {
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
}
