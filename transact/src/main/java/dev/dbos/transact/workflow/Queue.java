package dev.dbos.transact.workflow;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Property definition for a DBOS workflow queue. Provides options for a name, concurrency and rate
 * limits, prioritization behavior and partitioned behavior
 */
public record Queue(
    @NonNull String name,
    @Nullable Integer concurrency,
    @Nullable Integer workerConcurrency,
    boolean priorityEnabled,
    boolean partitioningEnabled,
    @Nullable RateLimit rateLimit,
    @NonNull Duration pollingInterval,
    /**
     * The application that owns this queue and polls it, as recorded in the system database, or
     * null if the queue is unclaimed and so belongs to every application sharing it. Set by the
     * database when a queue is read back; ignored when registering one, which always records the
     * registering application.
     */
    @Nullable String applicationName) {

  public static final Duration DEFAULT_POLLING_INTERVAL = Duration.ofSeconds(1);

  /** Rate limit parameter structure for DBOS workflow queues */
  public record RateLimit(int limit, Duration period) {}

  public Queue {
    Objects.requireNonNull(name, "Queue name must not be null");
    Objects.requireNonNull(pollingInterval, "Queue pollingInterval must not be null");
    if (concurrency != null && concurrency <= 0)
      throw new IllegalArgumentException(
          "If specified, queue concurrency must be greater than zero");
    if (workerConcurrency != null && workerConcurrency <= 0)
      throw new IllegalArgumentException(
          "If specified, queue workerConcurrency must be greater than zero");
    if (pollingInterval.isNegative() || pollingInterval.isZero())
      throw new IllegalArgumentException("Queue pollingInterval must be greater than zero");
  }

  /**
   * Constructs a queue with no explicit owning application, which records the registering
   * application as the owner.
   *
   * @deprecated A {@code Queue} is what {@link dev.dbos.transact.DBOS#findQueue(String)} returns,
   *     not something to build. Register with {@link dev.dbos.transact.DBOS#registerQueue(String,
   *     QueueOptions)}.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue(
      @NonNull String name,
      @Nullable Integer concurrency,
      @Nullable Integer workerConcurrency,
      boolean priorityEnabled,
      boolean partitioningEnabled,
      @Nullable RateLimit rateLimit,
      @NonNull Duration pollingInterval) {
    this(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval,
        null);
  }

  /**
   * Construct a queue with a given name
   *
   * @deprecated See {@link #Queue(String, Integer, Integer, boolean, boolean, RateLimit,
   *     Duration)}.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue(@NonNull String name) {
    this(name, null, null, false, false, null, DEFAULT_POLLING_INTERVAL, null);
  }

  /**
   * @return true if the Queue has rate-limiting enforced
   */
  public boolean hasLimiter() {
    return rateLimit != null;
  }

  /**
   * Produces a new Queue with the assigned name.
   *
   * @deprecated Configure a queue with {@link QueueOptions} at registration.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withName(@NonNull String name) {
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval,
        applicationName);
  }

  /**
   * Produces a new Queue with the assigned global concurrency. `null` may be specified to remove
   * the concurrency limit.
   *
   * @deprecated Configure a queue with {@link QueueOptions} at registration.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withConcurrency(@Nullable Integer concurrency) {
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval,
        applicationName);
  }

  /**
   * Produces a new Queue with the assigned per-worker concurrency. `null` may be specified to
   * remove the concurrency limit.
   *
   * @deprecated Configure a queue with {@link QueueOptions} at registration.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withWorkerConcurrency(@Nullable Integer workerConcurrency) {
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval,
        applicationName);
  }

  /**
   * Produces a new Queue with the prioritization enabled/disabled.
   *
   * @deprecated Configure a queue with {@link QueueOptions} at registration.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withPriorityEnabled(boolean priorityEnabled) {
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval,
        applicationName);
  }

  /**
   * Produces a new Queue with the partitioned enabled/disabled.
   *
   * @deprecated Configure a queue with {@link QueueOptions} at registration.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withPartitioningEnabled(boolean partitioningEnabled) {
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval,
        applicationName);
  }

  /**
   * Produces a new Queue with the assigned rate limit. `null` may be specified to remove the rate
   * limit.
   *
   * @deprecated Configure a queue with {@link QueueOptions} at registration.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withRateLimit(@Nullable RateLimit rateLimit) {
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval,
        applicationName);
  }

  /**
   * Produces a new Queue with the assigned rate limit, expressed in workflows per period duration.
   *
   * @deprecated Configure a queue with {@link QueueOptions} at registration.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withRateLimit(int limit, Duration period) {
    return withRateLimit(new RateLimit(limit, period));
  }

  /**
   * Produces a new Queue with the assigned rate limit, expressed in workflows per period.
   *
   * @deprecated Configure a queue with {@link QueueOptions} at registration.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withRateLimit(int limit, long period, TimeUnit unit) {
    return withRateLimit(new RateLimit(limit, Duration.of(period, unit.toChronoUnit())));
  }

  /**
   * Produces a new Queue with the assigned polling interval.
   *
   * @deprecated Configure a queue with {@link QueueOptions} at registration.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withPollingInterval(@NonNull Duration pollingInterval) {
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval,
        applicationName);
  }
}
