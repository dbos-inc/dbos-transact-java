package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

/**
 * The set/and builders survive until 2.0 only as fronts for the plain-value with builders, so each
 * must build exactly what its replacement does.
 */
@SuppressWarnings("removal")
class DeprecatedQueueOptionsTest {

  private static final QueueOptions BASE = QueueOptions.empty().withPollingInterval(Duration.ZERO);
  private static final Duration PERIOD = Duration.ofSeconds(60);

  @Test
  void staticFactoriesMatchWithBuilders() {
    var empty = QueueOptions.empty();
    assertEquals(empty.withConcurrency(3), QueueOptions.setConcurrency(3));
    assertEquals(empty.withWorkerConcurrency(4), QueueOptions.setWorkerConcurrency(4));
    assertEquals(empty.withRateLimit(5, PERIOD), QueueOptions.setRateLimit(5, PERIOD));
    assertEquals(
        empty.withRateLimit(5, 60, TimeUnit.SECONDS),
        QueueOptions.setRateLimit(5, 60, TimeUnit.SECONDS));
    assertEquals(empty.withPartitionConcurrency(6), QueueOptions.setPartitionConcurrency(6));
    assertEquals(
        empty.withPartitionWorkerConcurrency(7), QueueOptions.setPartitionWorkerConcurrency(7));
    assertEquals(
        empty.withPartitionRateLimit(8, PERIOD), QueueOptions.setPartitionRateLimit(8, PERIOD));
    assertEquals(
        empty.withPartitionRateLimit(8, 60, TimeUnit.SECONDS),
        QueueOptions.setPartitionRateLimit(8, 60, TimeUnit.SECONDS));
    assertEquals(empty.withPollingInterval(PERIOD), QueueOptions.setPollingInterval(PERIOD));
  }

  @Test
  void chainingMethodsMatchWithBuilders() {
    assertEquals(BASE.withConcurrency(3), BASE.andConcurrency(3));
    assertEquals(BASE.withWorkerConcurrency(4), BASE.andWorkerConcurrency(4));
    assertEquals(BASE.withRateLimit(5, PERIOD), BASE.andRateLimit(5, PERIOD));
    assertEquals(
        BASE.withRateLimit(5, 60, TimeUnit.SECONDS), BASE.andRateLimit(5, 60, TimeUnit.SECONDS));
    assertEquals(BASE.withPartitionConcurrency(6), BASE.andPartitionConcurrency(6));
    assertEquals(BASE.withPartitionWorkerConcurrency(7), BASE.andPartitionWorkerConcurrency(7));
    assertEquals(BASE.withPartitionRateLimit(8, PERIOD), BASE.andPartitionRateLimit(8, PERIOD));
    assertEquals(
        BASE.withPartitionRateLimit(8, 60, TimeUnit.SECONDS),
        BASE.andPartitionRateLimit(8, 60, TimeUnit.SECONDS));
    assertEquals(BASE.withPollingInterval(PERIOD), BASE.andPollingInterval(PERIOD));
  }

  @Test
  void nullClearsTheColumnInBothForms() {
    assertEquals(Field.of(null), QueueOptions.setConcurrency(null).concurrency());
    assertEquals(BASE.withConcurrency((Integer) null), BASE.andConcurrency(null));
    assertEquals(BASE.withRateLimit(null, null), BASE.andRateLimit(null, null));
  }
}
