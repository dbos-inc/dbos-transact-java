package dev.dbos.transact.workflow;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;

/**
 * Configuration options for a DBOS workflow queue. Used for both registration of database-backed
 * queues and partial updates to queue configuration.
 *
 * <p>When used for registration, absent and null fields both result in the column being null (no
 * limit / use default). When used for updates, absent fields are left unchanged in the database
 * while null-valued fields clear the column.
 */
public record QueueOptions(
    Field<Integer> concurrency,
    Field<Integer> workerConcurrency,
    Field<Integer> rateLimitMax,
    Field<Duration> rateLimitPeriod,
    Field<Boolean> priorityEnabled,
    Field<Boolean> partitionQueue,
    Field<Duration> pollingInterval) {

  private static final QueueOptions EMPTY =
      new QueueOptions(
          Field.absent(),
          Field.absent(),
          Field.absent(),
          Field.absent(),
          Field.absent(),
          Field.absent(),
          Field.absent());

  public static QueueOptions empty() {
    return EMPTY;
  }

  public boolean isEmpty() {
    return !concurrency.isPresent()
        && !workerConcurrency.isPresent()
        && !rateLimitMax.isPresent()
        && !rateLimitPeriod.isPresent()
        && !priorityEnabled.isPresent()
        && !partitionQueue.isPresent()
        && !pollingInterval.isPresent();
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
    return EMPTY.withPriorityEnabled(Field.of(value));
  }

  public static QueueOptions setPartitionQueue(boolean value) {
    return EMPTY.withPartitionQueue(Field.of(value));
  }

  public static QueueOptions setPollingInterval(@Nullable Duration value) {
    return EMPTY.withPollingInterval(Field.of(value));
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

  public QueueOptions withPriorityEnabled(Field<Boolean> priorityEnabled) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  public QueueOptions withPartitionQueue(Field<Boolean> partitionQueue) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  public QueueOptions withPollingInterval(Field<Duration> pollingInterval) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  // ── Convenience chaining methods (take raw values, wrap in Field.of) ────

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
    return withPriorityEnabled(Field.of(value));
  }

  public QueueOptions andPartitionQueue(boolean value) {
    return withPartitionQueue(Field.of(value));
  }

  public QueueOptions andPollingInterval(@Nullable Duration value) {
    return withPollingInterval(Field.of(value));
  }
}
