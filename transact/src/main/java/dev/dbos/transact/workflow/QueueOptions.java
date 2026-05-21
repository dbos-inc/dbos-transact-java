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

  public static QueueOptions setRateLimit(int limit, Duration period) {
    return setRateLimit(limit, (Duration) period);
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

  // ── Conversion for DB registration ───────────────────────────────────────

  /**
   * Converts to a {@link Queue} record for DB upsert. Absent and null fields both produce null
   * column values (no limit / use default).
   */
  public Queue toQueue(String name) {
    Integer concurrencyVal = concurrency.isPresent() ? concurrency.get() : null;
    Integer workerConcurrencyVal = workerConcurrency.isPresent() ? workerConcurrency.get() : null;
    Boolean priorityEnabledVal = priorityEnabled.isPresent() ? priorityEnabled.get() : null;
    Boolean partitionQueueVal = partitionQueue.isPresent() ? partitionQueue.get() : null;

    Queue.RateLimit rateLimit = null;
    if (rateLimitMax.isPresent()
        && rateLimitPeriod.isPresent()
        && rateLimitMax.get() != null
        && rateLimitPeriod.get() != null) {
      rateLimit = new Queue.RateLimit(rateLimitMax.get(), rateLimitPeriod.get());
    }

    Duration pollingIntervalVal = Queue.DEFAULT_POLLING_INTERVAL;
    if (pollingInterval.isPresent() && pollingInterval.get() != null) {
      pollingIntervalVal = pollingInterval.get();
    }

    return new Queue(
        name,
        concurrencyVal,
        workerConcurrencyVal,
        Boolean.TRUE.equals(priorityEnabledVal),
        Boolean.TRUE.equals(partitionQueueVal),
        rateLimit,
        pollingIntervalVal);
  }
}
