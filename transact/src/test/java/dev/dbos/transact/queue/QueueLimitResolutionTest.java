package dev.dbos.transact.queue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.workflow.Field;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.Queue.RateLimit;
import dev.dbos.transact.workflow.QueueOptions;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The queue limit surface on its own: which limits a queue carries, whether it partitions, and at
 * what scope each limit is enforced. No database and no dequeue — those arrive with the later
 * slices of #507.
 *
 * <p>Checked against Go's {@code ResolveLimits} / {@code IsPartitioned} / {@code
 * IsLegacyPartitioned} (dbos/internal/models/queue.go) and the validation rules Go, Python and
 * TypeScript share.
 */
public class QueueLimitResolutionTest {

  private static final Duration POLL = Duration.ofSeconds(1);
  private static final RateLimit LIMIT = new RateLimit(10, Duration.ofSeconds(60));
  private static final RateLimit PARTITION_LIMIT = new RateLimit(3, Duration.ofSeconds(60));

  private static Queue queue(
      Integer concurrency,
      Integer workerConcurrency,
      boolean partitioningEnabled,
      RateLimit rateLimit,
      Integer partitionConcurrency,
      Integer partitionWorkerConcurrency,
      RateLimit partitionRateLimit) {
    return new Queue(
        "q",
        concurrency,
        workerConcurrency,
        false,
        partitioningEnabled,
        rateLimit,
        partitionConcurrency,
        partitionWorkerConcurrency,
        partitionRateLimit,
        POLL,
        null);
  }

  private static Queue plain(Integer concurrency, Integer workerConcurrency, RateLimit rateLimit) {
    return queue(concurrency, workerConcurrency, false, rateLimit, null, null, null);
  }

  @Nested
  @DisplayName("what makes a queue partitioned")
  class Partitioning {

    @Test
    void anUnlimitedQueueIsNotPartitioned() {
      var q = plain(null, null, null);
      assertFalse(q.hasPartitionLimits());
      assertFalse(q.isPartitioned());
      assertFalse(q.isLegacyPartitioned());
    }

    @Test
    void queueWideLimitsAloneDoNotPartition() {
      var q = plain(10, 5, LIMIT);
      assertFalse(q.hasPartitionLimits());
      assertFalse(q.isPartitioned());
      assertFalse(q.isLegacyPartitioned());
    }

    @Test
    void anyPerPartitionLimitPartitionsTheQueue() {
      // Each on its own: there is no separate switch, so each limit must carry the decision.
      for (var q :
          new Queue[] {
            queue(null, null, false, null, 2, null, null),
            queue(null, null, false, null, null, 2, null),
            queue(null, null, false, null, null, null, PARTITION_LIMIT),
          }) {
        assertTrue(q.hasPartitionLimits());
        assertTrue(q.isPartitioned());
        assertFalse(q.isLegacyPartitioned(), "a queue with real partition limits is not legacy");
      }
    }

    @Test
    void theDeprecatedFlagAlonePartitionsAndIsLegacy() {
      var q = queue(10, 5, true, LIMIT, null, null, null);
      assertFalse(q.hasPartitionLimits());
      assertTrue(q.isPartitioned());
      assertTrue(q.isLegacyPartitioned());
    }

    @Test
    void theFlagIsNotLegacyOncePartitionLimitsAreSet() {
      // Both set: the per-partition limits win and the flag adds nothing, so the queue-wide
      // limits keep their own scope rather than being reinterpreted.
      var q = queue(10, 5, true, LIMIT, 2, null, null);
      assertTrue(q.isPartitioned());
      assertFalse(q.isLegacyPartitioned());
    }
  }

  @Nested
  @DisplayName("resolving each limit to the scope it is enforced at")
  class Resolution {

    @Test
    void anUnpartitionedQueueResolvesToItself() {
      var resolved = plain(10, 5, LIMIT).resolveLimits();
      assertEquals(10, resolved.concurrency());
      assertEquals(5, resolved.workerConcurrency());
      assertEquals(LIMIT, resolved.rateLimit());
      assertNull(resolved.partitionConcurrency());
      assertNull(resolved.partitionWorkerConcurrency());
      assertNull(resolved.partitionRateLimit());
    }

    @Test
    void bothScopesSurviveTogether() {
      var resolved = queue(10, 5, false, LIMIT, 4, 2, PARTITION_LIMIT).resolveLimits();
      assertEquals(10, resolved.concurrency());
      assertEquals(5, resolved.workerConcurrency());
      assertEquals(LIMIT, resolved.rateLimit());
      assertEquals(4, resolved.partitionConcurrency());
      assertEquals(2, resolved.partitionWorkerConcurrency());
      assertEquals(PARTITION_LIMIT, resolved.partitionRateLimit());
    }

    @Test
    void aLegacyQueueMovesItsQueueWideLimitsToThePartitionScope() {
      // This is the whole reason resolveLimits exists: the row stores these in the queue-wide
      // columns, but a legacy partitioned queue enforces them per partition key.
      var resolved = queue(10, 5, true, LIMIT, null, null, null).resolveLimits();
      assertNull(resolved.concurrency(), "nothing is enforced queue-wide in the legacy mode");
      assertNull(resolved.workerConcurrency());
      assertNull(resolved.rateLimit());
      assertEquals(10, resolved.partitionConcurrency());
      assertEquals(5, resolved.partitionWorkerConcurrency());
      assertEquals(LIMIT, resolved.partitionRateLimit());
    }

    @Test
    void theFlagDoesNotRemapOncePartitionLimitsAreSet() {
      var resolved = queue(10, 5, true, LIMIT, 4, null, null).resolveLimits();
      assertEquals(10, resolved.concurrency(), "the queue-wide limits keep their own scope");
      assertEquals(5, resolved.workerConcurrency());
      assertEquals(LIMIT, resolved.rateLimit());
      assertEquals(4, resolved.partitionConcurrency());
    }

    @Test
    void anUnlimitedQueueResolvesToNothing() {
      var resolved = plain(null, null, null).resolveLimits();
      assertNull(resolved.concurrency());
      assertNull(resolved.workerConcurrency());
      assertNull(resolved.rateLimit());
      assertNull(resolved.partitionConcurrency());
      assertNull(resolved.partitionWorkerConcurrency());
      assertNull(resolved.partitionRateLimit());
    }
  }

  @Nested
  @DisplayName("QueueOptions carries the per-partition limits")
  class Options {

    @Test
    void emptyStaysEmptyWithTheNewFields() {
      assertTrue(new QueueOptions().isEmpty());
    }

    @Test
    void eachPerPartitionFactorySetsOnlyItsOwnField() {
      var c = new QueueOptions().withPartitionConcurrency(4);
      assertFalse(c.isEmpty());
      assertEquals(4, c.partitionConcurrency().get());
      assertFalse(c.partitionWorkerConcurrency().isPresent());
      assertFalse(c.concurrency().isPresent());

      var w = new QueueOptions().withPartitionWorkerConcurrency(2);
      assertEquals(2, w.partitionWorkerConcurrency().get());
      assertFalse(w.partitionConcurrency().isPresent());

      var r = new QueueOptions().withPartitionRateLimit(5, Duration.ofSeconds(30));
      assertEquals(5, r.partitionRateLimitMax().get());
      assertEquals(Duration.ofSeconds(30), r.partitionRateLimitPeriod().get());
      assertFalse(r.rateLimitMax().isPresent(), "the queue-wide limit is untouched");
    }

    @Test
    void aPerPartitionFieldAloneMakesTheOptionsNonEmpty() {
      // isEmpty() gates updateQueue's early return, so a field it does not know about would
      // make an update that carries only that field silently do nothing.
      assertFalse(new QueueOptions().withPartitionConcurrency(1).isEmpty());
      assertFalse(new QueueOptions().withPartitionWorkerConcurrency(1).isEmpty());
      assertFalse(new QueueOptions().withPartitionRateLimit(1, Duration.ofSeconds(1)).isEmpty());
    }

    @Test
    void aFieldCanBeClearedAsDistinctFromLeftAlone() {
      var cleared = new QueueOptions().withPartitionConcurrency((Integer) null);
      assertTrue(cleared.partitionConcurrency().isPresent(), "present, holding null");
      assertNull(cleared.partitionConcurrency().get());
      assertFalse(
          new QueueOptions().partitionConcurrency().isPresent(), "absent, meaning unchanged");
    }

    @Test
    void theDeprecatedConstructorLeavesThePerPartitionFieldsAbsent() {
      @SuppressWarnings("removal")
      var o =
          new QueueOptions(
              Field.of(4),
              Field.absent(),
              Field.absent(),
              Field.absent(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());
      assertEquals(4, o.concurrency().get());
      assertFalse(o.partitionConcurrency().isPresent());
      assertFalse(o.partitionRateLimitMax().isPresent());
    }
  }

  @Nested
  @DisplayName("validation on write")
  class Validation {

    private static String rejects(Supplier<Queue> construct) {
      var queue = construct.get();
      return assertThrows(IllegalArgumentException.class, queue::validateForRegistration)
          .getMessage();
    }

    @Test
    void theLegacyFlagMayNotBeSetAlongsideAPartitionLimit() {
      for (Supplier<Queue> flagAndLimit :
          List.<Supplier<Queue>>of(
              () -> queue(10, null, true, null, 2, null, null),
              () -> queue(10, null, true, null, null, 2, null),
              () -> queue(10, null, true, null, null, null, PARTITION_LIMIT))) {
        assertTrue(rejects(flagAndLimit).contains("set only one of them"));
      }
      // Either on its own may be written.
      queue(10, 5, true, LIMIT, null, null, null).validateForRegistration();
      queue(10, 5, false, LIMIT, 2, 1, PARTITION_LIMIT).validateForRegistration();
    }

    @Test
    void aQueueWideConcurrencyMayBeZeroButNotNegative() {
      // Zero pauses the queue.
      plain(0, 0, null).validateForRegistration();
      assertTrue(rejects(() -> plain(-1, null, null)).contains("concurrency must not be negative"));
      assertTrue(
          rejects(() -> plain(null, -1, null)).contains("workerConcurrency must not be negative"));
    }

    @Test
    void aPartitionConcurrencyMustBePositive() {
      assertTrue(
          rejects(() -> queue(null, null, false, null, 0, null, null))
              .contains("partitionConcurrency"));
      assertTrue(
          rejects(() -> queue(null, null, false, null, null, 0, null))
              .contains("partitionWorkerConcurrency"));
    }

    @Test
    void aNarrowerScopeMayNotExceedAWiderOne() {
      // partitionWorkerConcurrency <= partitionConcurrency
      assertTrue(
          rejects(() -> queue(null, null, false, null, 2, 3, null))
              .contains("partitionConcurrency must be greater than or equal"));
      // partitionWorkerConcurrency <= workerConcurrency
      assertTrue(
          rejects(() -> queue(null, 2, false, null, null, 3, null))
              .contains("workerConcurrency must be greater than or equal"));
      // partitionConcurrency <= concurrency
      assertTrue(
          rejects(() -> queue(2, null, false, null, 3, null, null))
              .contains("greater than or equal to partitionConcurrency"));
      // partitionWorkerConcurrency <= concurrency
      assertTrue(
          rejects(() -> queue(2, null, false, null, null, 3, null))
              .contains("greater than or equal to partitionWorkerConcurrency"));
      // workerConcurrency <= concurrency
      assertTrue(
          rejects(() -> plain(2, 5, null))
              .contains("concurrency must be greater than or equal to workerConcurrency"));
      // A paused queue still bounds the narrower limits: pausing one that has them means
      // pausing those too.
      assertTrue(
          rejects(() -> plain(0, 1, null))
              .contains("concurrency must be greater than or equal to workerConcurrency"));
      assertTrue(
          rejects(() -> queue(0, null, false, null, 1, null, null))
              .contains("greater than or equal to partitionConcurrency"));
    }

    @Test
    void theConstructorAcceptsWhatAWriteWouldRefuseBecauseItIsAlsoTheReadPath() {
      // QueuesDAO.queueFromResultSet loads every row through this constructor, so a rule enforced
      // here would make a stored row that breaks it unreadable -- and one unreadable row takes
      // listQueues, and dynamic queue discovery with it, down too.
      var q = plain(0, 0, new RateLimit(0, Duration.ofSeconds(1)));
      assertEquals(0, q.concurrency());
      assertEquals(0, q.workerConcurrency());
      assertEquals(0, q.rateLimit().limit());
      var crossed = queue(2, 5, false, null, 3, 6, new RateLimit(1, Duration.ZERO));
      assertEquals(3, crossed.partitionConcurrency());
      assertEquals(6, crossed.partitionWorkerConcurrency());
    }

    @Test
    void equalLimitsAtAdjacentScopesAreAllowed() {
      var q = queue(4, 4, false, null, 4, 4, null);
      q.validateForRegistration();
      assertEquals(4, q.concurrency());
      assertEquals(4, q.partitionWorkerConcurrency());
    }

    @Test
    void everyRateLimitMustBeWhole() {
      assertTrue(
          rejects(
                  () ->
                      queue(
                          null,
                          null,
                          false,
                          null,
                          null,
                          null,
                          new RateLimit(0, Duration.ofSeconds(1))))
              .contains("partitionRateLimit limit"));
      assertTrue(
          rejects(() -> queue(null, null, false, null, null, null, new RateLimit(1, Duration.ZERO)))
              .contains("partitionRateLimit period"));
      assertTrue(
          rejects(() -> plain(null, null, new RateLimit(0, Duration.ofSeconds(1))))
              .contains("rateLimit limit"));
      assertTrue(
          rejects(() -> plain(null, null, new RateLimit(1, Duration.ZERO)))
              .contains("rateLimit period"));
    }

    @Test
    void aDurationMustBeAtLeastTheMillisecondItIsStoredAt() {
      // Stored in whole milliseconds, so anything shorter would be stored as zero.
      var subMilli = Duration.ofNanos(500_000);
      assertTrue(
          rejects(() -> plain(null, null, new RateLimit(1, subMilli)))
              .contains("rateLimit period must be at least one millisecond"));
      assertTrue(
          rejects(() -> queue(null, null, false, null, null, null, new RateLimit(1, subMilli)))
              .contains("partitionRateLimit period must be at least one millisecond"));
      assertTrue(
          rejects(() -> polledEvery(subMilli))
              .contains("pollingInterval must be at least one millisecond"));

      plain(null, null, new RateLimit(1, Duration.ofMillis(1))).validateForRegistration();
      queue(null, null, false, null, null, null, new RateLimit(1, Duration.ofMillis(1)))
          .validateForRegistration();
      polledEvery(Duration.ofMillis(1)).validateForRegistration();
    }

    private static Queue polledEvery(Duration pollingInterval) {
      return new Queue(
          "q", null, null, false, false, null, null, null, null, pollingInterval, null);
    }
  }

  @Test
  @DisplayName(
      "a batched partition dequeue needs partition concurrency 1 and no other shared limit")
  public void batchedPartitionDequeueEligibility() {
    assertTrue(queue(null, null, false, null, 1, null, null).canBatchPartitionedDequeue());
    // A per-partition worker limit cannot exceed partition concurrency 1, so it does not matter.
    assertTrue(queue(null, 4, false, null, 1, 1, null).canBatchPartitionedDequeue());
    // The legacy flag enforces its queue-wide limits per partition, so concurrency 1 qualifies.
    assertTrue(queue(1, null, true, null, null, null, null).canBatchPartitionedDequeue());

    // The batched claim reads no worker limit, so a stored one that leaves no room must take the
    // per-partition path, which enforces it.
    assertFalse(queue(null, null, false, null, 1, 0, null).canBatchPartitionedDequeue());
    assertFalse(queue(null, null, false, null, 1, -1, null).canBatchPartitionedDequeue());

    assertFalse(plain(1, null, null).canBatchPartitionedDequeue());
    assertFalse(queue(null, null, true, null, null, null, null).canBatchPartitionedDequeue());
    assertFalse(queue(null, null, false, null, 2, null, null).canBatchPartitionedDequeue());
    assertFalse(queue(5, null, false, null, 1, null, null).canBatchPartitionedDequeue());
    assertFalse(queue(null, null, false, LIMIT, 1, null, null).canBatchPartitionedDequeue());
    assertFalse(
        queue(null, null, false, null, 1, null, PARTITION_LIMIT).canBatchPartitionedDequeue());
  }
}
