package dev.dbos.transact.workflow;

import java.util.Objects;

/**
 * Property definition for a DBOS workflow queue. Provides options for a name, concurrency and rate
 * limits, and prioritization behavior
 */
public record Queue(
    String name,
    Integer concurrency,
    Integer workerConcurrency,
    boolean priorityEnabled,
    RateLimit rateLimit) {

  public Queue {
    Objects.requireNonNull(name, "Queue name must not be null");
    if (concurrency != null && concurrency <= 0)
      throw new IllegalArgumentException(
          "If specified, queue concurrency must be greater than zero");
    if (workerConcurrency != null && workerConcurrency <= 0)
      throw new IllegalArgumentException(
          "If specified, queue workerConcurrency must be greater than zero");
  }

  /** Construct a queue with a given name */
  public Queue(String name) {
    this(name, null, null, false, null);
  }

  /**
   * @return true if the Queue has rate-limiting enforced
   */
  public boolean hasLimiter() {
    return rateLimit != null;
  }

  /** Rate limit parameter structure for DBOS workflow queues */
  public static record RateLimit(int limit, double period) {}

  /** Produces a new Queue with the assigned name. */
  public Queue withName(String name) {
    return new Queue(name, concurrency, workerConcurrency, priorityEnabled, rateLimit);
  }

  /**
   * Produces a new Queue with the assigned global concurrency. `null` may be specified to remove
   * the concurrency limit.
   */
  public Queue withConcurrency(Integer concurrency) {
    return new Queue(name, concurrency, workerConcurrency, priorityEnabled, rateLimit);
  }

  /**
   * Produces a new Queue with the assigned per-worker concurrency. `null` may be specified to
   * remove the concurrency limit.
   */
  public Queue withWorkerConcurrency(Integer workerConcurrency) {
    return new Queue(name, concurrency, workerConcurrency, priorityEnabled, rateLimit);
  }

  /** Produces a new Queue with the prioritization enabled/disabled. */
  public Queue withPriorityEnabled(boolean priorityEnabled) {
    return new Queue(name, concurrency, workerConcurrency, priorityEnabled, rateLimit);
  }

  /**
   * Produces a new Queue with the assigned rate limit. `null` may be specified to remove the rate
   * limit.
   */
  public Queue withRateLimit(RateLimit rateLimit) {
    return new Queue(name, concurrency, workerConcurrency, priorityEnabled, rateLimit);
  }

  /**
   * Produces a new Queue with the assigned rate limit, expressed in workflows per period (seconds).
   */
  public Queue withRateLimit(int limit, double period) {
    return withRateLimit(new RateLimit(limit, period));
  }
}
