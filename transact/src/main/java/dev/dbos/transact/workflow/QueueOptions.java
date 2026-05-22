package dev.dbos.transact.workflow;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;

/**
 * Configuration options for a DBOS workflow queue. Used for both registration of database-backed
 * queues and partial updates to queue configuration.
 *
 * <p>When used for registration, absent and null fields both result in the column being null (no
 * limit / use default). When used for updates, absent fields are left unchanged in the database
 * while null-valued fields clear the column.
 *
 * <p>The non-nullable queue properties ({@code priorityEnabled}, {@code partitionQueue}, {@code
 * pollingInterval}) use {@link Optional} — {@link Optional#empty()} means use the default on
 * creation or leave unchanged on update; a present value sets the column.
 */
public record QueueOptions(
    Field<Integer> concurrency,
    Field<Integer> workerConcurrency,
    Field<Integer> rateLimitMax,
    Field<Duration> rateLimitPeriod,
    Optional<Boolean> priorityEnabled,
    Optional<Boolean> partitionQueue,
    Optional<Duration> pollingInterval) {

  private static final QueueOptions EMPTY =
      new QueueOptions(
          Field.absent(),
          Field.absent(),
          Field.absent(),
          Field.absent(),
          Optional.empty(),
          Optional.empty(),
          Optional.empty());

  public static QueueOptions empty() {
    return EMPTY;
  }

  public boolean isEmpty() {
    return !concurrency.isPresent()
        && !workerConcurrency.isPresent()
        && !rateLimitMax.isPresent()
        && !rateLimitPeriod.isPresent()
        && priorityEnabled.isEmpty()
        && partitionQueue.isEmpty()
        && pollingInterval.isEmpty();
  }

  // ── Static factories ──────────────────────────────────────────────────────

  public static QueueOptions setConcurrency(@Nullable Integer value) {
    return EMPTY.withConcurrency(Field.of(value));
  }

  public static QueueOptions setWorkerConcurrency(@Nullable Integer value) {
    return EMPTY.withWorkerConcurrency(Field.of(value));
  }

  public static QueueOptions setRateLimit(@Nullable Integer max, @Nullable Duration period) {
    return EMPTY.withRateLimitMax(Field.of(max)).withRateLimitPeriod(Field.of(period));
  }

  public static QueueOptions setRateLimit(int limit, long period, TimeUnit unit) {
    return setRateLimit(limit, Duration.of(period, unit.toChronoUnit()));
  }

  public static QueueOptions setPriorityEnabled(boolean value) {
    return EMPTY.withPriorityEnabled(Optional.of(value));
  }

  public static QueueOptions setPartitionQueue(boolean value) {
    return EMPTY.withPartitionQueue(Optional.of(value));
  }

  public static QueueOptions setPollingInterval(Duration value) {
    return EMPTY.withPollingInterval(Optional.of(value));
  }

  // ── Builders for chaining ─────────────────────────────────────────────────

  public QueueOptions withConcurrency(Field<Integer> concurrency) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  public QueueOptions withWorkerConcurrency(Field<Integer> workerConcurrency) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  public QueueOptions withRateLimitMax(Field<Integer> rateLimitMax) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  public QueueOptions withRateLimitPeriod(Field<Duration> rateLimitPeriod) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  public QueueOptions withPriorityEnabled(Optional<Boolean> priorityEnabled) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  public QueueOptions withPartitionQueue(Optional<Boolean> partitionQueue) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  public QueueOptions withPollingInterval(Optional<Duration> pollingInterval) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  // ── Convenience chaining methods ──────────────────────────────────────────

  public QueueOptions andConcurrency(@Nullable Integer value) {
    return withConcurrency(Field.of(value));
  }

  public QueueOptions andWorkerConcurrency(@Nullable Integer value) {
    return withWorkerConcurrency(Field.of(value));
  }

  public QueueOptions andRateLimit(@Nullable Integer max, @Nullable Duration period) {
    return withRateLimitMax(Field.of(max)).withRateLimitPeriod(Field.of(period));
  }

  public QueueOptions andRateLimit(int max, long period, TimeUnit unit) {
    return andRateLimit(max, Duration.of(period, unit.toChronoUnit()));
  }

  public QueueOptions andPriorityEnabled(boolean value) {
    return withPriorityEnabled(Optional.of(value));
  }

  public QueueOptions andPartitionQueue(boolean value) {
    return withPartitionQueue(Optional.of(value));
  }

  public QueueOptions andPollingInterval(Duration value) {
    return withPollingInterval(Optional.of(value));
  }
}
