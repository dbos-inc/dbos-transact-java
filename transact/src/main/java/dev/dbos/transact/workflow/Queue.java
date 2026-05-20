package dev.dbos.transact.workflow;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Property definition for a DBOS workflow queue. Provides options for a name, concurrency and rate
 * limits, prioritization behavior and partitioned behavior
 */
public record Queue(
    String name,
    Integer concurrency,
    Integer workerConcurrency,
    boolean priorityEnabled,
    boolean partitioningEnabled,
    RateLimit rateLimit,
    Duration pollingInterval) {

  /** Rate limit parameter structure for DBOS workflow queues */
  public static record RateLimit(int limit, Duration period) {}

  public Queue {
    Objects.requireNonNull(name, "Queue name must not be null");
    if (concurrency != null && concurrency <= 0)
      throw new IllegalArgumentException(
          "If specified, queue concurrency must be greater than zero");
    if (workerConcurrency != null && workerConcurrency <= 0)
      throw new IllegalArgumentException(
          "If specified, queue workerConcurrency must be greater than zero");
    if (pollingInterval != null && (pollingInterval.isNegative() || pollingInterval.isZero()))
      throw new IllegalArgumentException(
          "If specified, queue pollingInterval must be greater than zero");
  }

  /** Construct a queue with a given name */
  public Queue(String name) {
    this(name, null, null, false, false, null, null);
  }

  /**
   * @return true if the Queue has rate-limiting enforced
   */
  public boolean hasLimiter() {
    return rateLimit != null;
  }

  /** Produces a new Queue with the assigned name. */
  public Queue withName(String name) {
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval);
  }

  /**
   * Produces a new Queue with the assigned global concurrency. `null` may be specified to remove
   * the concurrency limit.
   */
  public Queue withConcurrency(Integer concurrency) {
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval);
  }

  /**
   * Produces a new Queue with the assigned per-worker concurrency. `null` may be specified to
   * remove the concurrency limit.
   */
  public Queue withWorkerConcurrency(Integer workerConcurrency) {
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval);
  }

  /** Produces a new Queue with the prioritization enabled/disabled. */
  public Queue withPriorityEnabled(boolean priorityEnabled) {
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval);
  }

  /** Produces a new Queue with the partitioned enabled/disabled. */
  public Queue withPartitioningEnabled(boolean partitioningEnabled) {
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval);
  }

  /**
   * Produces a new Queue with the assigned rate limit. `null` may be specified to remove the rate
   * limit.
   */
  public Queue withRateLimit(RateLimit rateLimit) {
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval);
  }

  /**
   * Produces a new Queue with the assigned rate limit, expressed in workflows per period duration.
   */
  public Queue withRateLimit(int limit, Duration period) {
    return withRateLimit(new RateLimit(limit, period));
  }

  /** Produces a new Queue with the assigned rate limit, expressed in workflows per period. */
  public Queue withRateLimit(int limit, long period, TimeUnit unit) {
    return withRateLimit(new RateLimit(limit, Duration.of(period, unit.toChronoUnit())));
  }

  /**
   * Produces a new Queue with the assigned polling interval. `null` may be specified to use the
   * executor default (1 second).
   */
  public Queue withPollingInterval(Duration pollingInterval) {
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval);
  }
}
