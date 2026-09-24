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
import java.util.Optional;

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
      assertTrue(QueueOptions.empty().isEmpty());
    }

    @Test
    void eachPerPartitionFactorySetsOnlyItsOwnField() {
      var c = QueueOptions.setPartitionConcurrency(4);
      assertFalse(c.isEmpty());
      assertEquals(4, c.partitionConcurrency().get());
      assertFalse(c.partitionWorkerConcurrency().isPresent());
      assertFalse(c.concurrency().isPresent());

      var w = QueueOptions.setPartitionWorkerConcurrency(2);
      assertEquals(2, w.partitionWorkerConcurrency().get());
      assertFalse(w.partitionConcurrency().isPresent());

      var r = QueueOptions.setPartitionRateLimit(5, Duration.ofSeconds(30));
      assertEquals(5, r.partitionRateLimitMax().get());
      assertEquals(Duration.ofSeconds(30), r.partitionRateLimitPeriod().get());
      assertFalse(r.rateLimitMax().isPresent(), "the queue-wide limit is untouched");
    }

    @Test
    void aPerPartitionFieldAloneMakesTheOptionsNonEmpty() {
      // isEmpty() gates updateQueue's early return, so a field it does not know about would
      // make an update that carries only that field silently do nothing.
      assertFalse(QueueOptions.setPartitionConcurrency(1).isEmpty());
      assertFalse(QueueOptions.setPartitionWorkerConcurrency(1).isEmpty());
      assertFalse(QueueOptions.setPartitionRateLimit(1, Duration.ofSeconds(1)).isEmpty());
    }

    @Test
    void aFieldCanBeClearedAsDistinctFromLeftAlone() {
      var cleared = QueueOptions.setPartitionConcurrency(null);
      assertTrue(cleared.partitionConcurrency().isPresent(), "present, holding null");
      assertNull(cleared.partitionConcurrency().get());
      assertFalse(
          QueueOptions.empty().partitionConcurrency().isPresent(), "absent, meaning unchanged");
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
  @DisplayName("validation, in the order Go, Python and TypeScript apply it")
  class Validation {

    private static String rejects(Runnable construct) {
      return assertThrows(IllegalArgumentException.class, construct::run).getMessage();
    }

    @Test
    void everyLimitMustBePositive() {
      assertTrue(rejects(() -> plain(0, null, null)).contains("concurrency"));
      assertTrue(rejects(() -> plain(null, 0, null)).contains("workerConcurrency"));
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
    }

    @Test
    void theTwoQueueWideRulesAreDeferredBecauseThisIsAlsoTheReadPath() {
      // Go, Python and TypeScript reject both of these. Java cannot yet: this constructor is how
      // QueuesDAO.queueFromResultSet loads a row, so a rule added here rejects rows already in
      // the database -- and one unreadable row takes listQueues, and dynamic queue discovery
      // with it, down too. Both land with the write-side guard in the persistence slice.
      var q = plain(2, 5, null);
      assertEquals(2, q.concurrency());
      assertEquals(5, q.workerConcurrency());
      assertEquals(
          0, plain(null, null, new RateLimit(0, Duration.ofSeconds(1))).rateLimit().limit());
    }

    @Test
    void equalLimitsAtAdjacentScopesAreAllowed() {
      var q = queue(4, 4, false, null, 4, 4, null);
      assertEquals(4, q.concurrency());
      assertEquals(4, q.partitionWorkerConcurrency());
    }

    @Test
    void aPerPartitionRateLimitMustBeWhole() {
      // Safe to validate: a per-partition column cannot appear in a row written before it
      // existed, so this rule has nothing already stored to reject. The queue-wide pair is
      // deliberately unvalidated here -- see the deferral test above.
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
    }
  }
}
