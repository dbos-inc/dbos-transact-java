package dev.dbos.transact.workflow;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

/**
 * empty(), the set/and builders and the Field/Optional with overloads survive until their removal
 * only as fronts for the no-arg constructor and the plain-value with builders, so each must build
 * exactly what its replacement does.
 */
@SuppressWarnings("removal")
class DeprecatedQueueOptionsTest {

  private static final QueueOptions BASE = new QueueOptions().withPollingInterval(Duration.ZERO);
  private static final Duration PERIOD = Duration.ofSeconds(60);

  @Test
  void emptyMatchesTheNoArgConstructor() {
    assertEquals(new QueueOptions(), QueueOptions.empty());
    assertTrue(QueueOptions.empty().isEmpty());
    assertSame(QueueOptions.empty(), QueueOptions.empty());
  }

  @Test
  void staticFactoriesMatchWithBuilders() {
    var empty = new QueueOptions();
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
  void fieldAndOptionalBuildersMatchPlainValueBuilders() {
    assertEquals(BASE.withConcurrency(3), BASE.withConcurrency(Field.of(3)));
    assertEquals(BASE.withWorkerConcurrency(4), BASE.withWorkerConcurrency(Field.of(4)));
    assertEquals(BASE.withPartitionConcurrency(6), BASE.withPartitionConcurrency(Field.of(6)));
    assertEquals(
        BASE.withPartitionWorkerConcurrency(7), BASE.withPartitionWorkerConcurrency(Field.of(7)));
    assertEquals(BASE.withPollingInterval(PERIOD), BASE.withPollingInterval(Optional.of(PERIOD)));
    assertEquals(BASE.withRateLimitMax(5), BASE.withRateLimitMax(Field.of(5)));
    assertEquals(BASE.withRateLimitPeriod(PERIOD), BASE.withRateLimitPeriod(Field.of(PERIOD)));
    assertEquals(BASE.withPartitionRateLimitMax(8), BASE.withPartitionRateLimitMax(Field.of(8)));
    assertEquals(
        BASE.withPartitionRateLimitPeriod(PERIOD),
        BASE.withPartitionRateLimitPeriod(Field.of(PERIOD)));
    assertEquals(BASE.withConcurrency((Integer) null), BASE.withConcurrency(Field.of(null)));
  }

  @Test
  void nullClearsTheColumnInBothForms() {
    assertEquals(Field.of(null), QueueOptions.setConcurrency(null).concurrency());
    assertEquals(BASE.withConcurrency((Integer) null), BASE.andConcurrency(null));
    assertEquals(BASE.withRateLimit(null, null), BASE.andRateLimit(null, null));
  }
}
