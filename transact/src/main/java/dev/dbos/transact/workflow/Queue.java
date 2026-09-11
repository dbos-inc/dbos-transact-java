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
   * @deprecated Authoring a {@code Queue} by hand only ever fed {@link
   *     dev.dbos.transact.DBOS#registerQueue(Queue)}, which is itself deprecated for removal. Every
   *     other API that accepts one reads {@link #name()} and discards the rest. Register a queue
   *     with {@link dev.dbos.transact.DBOS#registerQueue(String,
   *     dev.dbos.transact.workflow.QueueOptions)} and read one back with {@link
   *     dev.dbos.transact.DBOS#findQueue(String)}; this type is what you receive, not what you
   *     build. Python draws the same line and enforces it at runtime — its {@code Queue.__init__}
   *     refuses direct construction, directing callers to {@code register_queue} and {@code
   *     retrieve_queue}.
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
   *     Duration)}. Pass the name itself where a queue is wanted.
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
   * @deprecated Queue configuration belongs to {@link QueueOptions}, which is what the
   *     database-backed registration accepts. The fields themselves stay live — the dequeue path
   *     reads them off the {@code Queue} the DAO materializes from the {@code queues} row — but
   *     once {@link dev.dbos.transact.DBOS#registerQueue(Queue)} is gone there is no public sink
   *     for a {@code Queue} you configured yourself.
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
   * @deprecated Queue configuration belongs to {@link QueueOptions}, which is what the
   *     database-backed registration accepts. The fields themselves stay live — the dequeue path
   *     reads them off the {@code Queue} the DAO materializes from the {@code queues} row — but
   *     once {@link dev.dbos.transact.DBOS#registerQueue(Queue)} is gone there is no public sink
   *     for a {@code Queue} you configured yourself.
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
   * @deprecated Queue configuration belongs to {@link QueueOptions}, which is what the
   *     database-backed registration accepts. The fields themselves stay live — the dequeue path
   *     reads them off the {@code Queue} the DAO materializes from the {@code queues} row — but
   *     once {@link dev.dbos.transact.DBOS#registerQueue(Queue)} is gone there is no public sink
   *     for a {@code Queue} you configured yourself.
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
   * @deprecated Queue configuration belongs to {@link QueueOptions}, which is what the
   *     database-backed registration accepts. The fields themselves stay live — the dequeue path
   *     reads them off the {@code Queue} the DAO materializes from the {@code queues} row — but
   *     once {@link dev.dbos.transact.DBOS#registerQueue(Queue)} is gone there is no public sink
   *     for a {@code Queue} you configured yourself.
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
   * @deprecated Queue configuration belongs to {@link QueueOptions}, which is what the
   *     database-backed registration accepts. The fields themselves stay live — the dequeue path
   *     reads them off the {@code Queue} the DAO materializes from the {@code queues} row — but
   *     once {@link dev.dbos.transact.DBOS#registerQueue(Queue)} is gone there is no public sink
   *     for a {@code Queue} you configured yourself.
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
   * @deprecated Queue configuration belongs to {@link QueueOptions}, which is what the
   *     database-backed registration accepts. The fields themselves stay live — the dequeue path
   *     reads them off the {@code Queue} the DAO materializes from the {@code queues} row — but
   *     once {@link dev.dbos.transact.DBOS#registerQueue(Queue)} is gone there is no public sink
   *     for a {@code Queue} you configured yourself.
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
   * @deprecated Queue configuration belongs to {@link QueueOptions}, which is what the
   *     database-backed registration accepts. The fields themselves stay live — the dequeue path
   *     reads them off the {@code Queue} the DAO materializes from the {@code queues} row — but
   *     once {@link dev.dbos.transact.DBOS#registerQueue(Queue)} is gone there is no public sink
   *     for a {@code Queue} you configured yourself.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withRateLimit(int limit, Duration period) {
    return withRateLimit(new RateLimit(limit, period));
  }

  /**
   * Produces a new Queue with the assigned rate limit, expressed in workflows per period.
   *
   * @deprecated Queue configuration belongs to {@link QueueOptions}, which is what the
   *     database-backed registration accepts. The fields themselves stay live — the dequeue path
   *     reads them off the {@code Queue} the DAO materializes from the {@code queues} row — but
   *     once {@link dev.dbos.transact.DBOS#registerQueue(Queue)} is gone there is no public sink
   *     for a {@code Queue} you configured yourself.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withRateLimit(int limit, long period, TimeUnit unit) {
    return withRateLimit(new RateLimit(limit, Duration.of(period, unit.toChronoUnit())));
  }

  /**
   * Produces a new Queue with the assigned polling interval.
   *
   * @deprecated Queue configuration belongs to {@link QueueOptions}, which is what the
   *     database-backed registration accepts. The fields themselves stay live — the dequeue path
   *     reads them off the {@code Queue} the DAO materializes from the {@code queues} row — but
   *     once {@link dev.dbos.transact.DBOS#registerQueue(Queue)} is gone there is no public sink
   *     for a {@code Queue} you configured yourself.
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
