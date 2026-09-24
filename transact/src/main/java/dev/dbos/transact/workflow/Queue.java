package dev.dbos.transact.workflow;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Property definition for a DBOS workflow queue. Provides options for a name, concurrency and rate
 * limits, and partitioned behavior. Every queue dispatches in priority order.
 *
 * <p>A queue carries flow control at two scopes at once. The queue-wide limits ({@code
 * concurrency}, {@code workerConcurrency}, {@code rateLimit}) bound the queue as a whole, while the
 * per-partition limits ({@code partitionConcurrency}, {@code partitionWorkerConcurrency}, {@code
 * partitionRateLimit}) bound each partition key independently. Setting any per-partition limit
 * partitions the queue; there is no separate switch for it.
 */
public record Queue(
    @NonNull String name,
    @Nullable Integer concurrency,
    @Nullable Integer workerConcurrency,
    boolean priorityEnabled,
    boolean partitioningEnabled,
    @Nullable RateLimit rateLimit,
    @Nullable Integer partitionConcurrency,
    @Nullable Integer partitionWorkerConcurrency,
    @Nullable RateLimit partitionRateLimit,
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

  /**
   * Every limit on a queue, mapped to the scope it is actually enforced at.
   *
   * <p>This is what the dequeue reads, rather than the raw fields: a legacy {@code
   * partitioningEnabled} queue records its limits in the queue-wide columns but enforces them per
   * partition, so the two differ for exactly that case.
   */
  public record ResolvedLimits(
      @Nullable Integer concurrency,
      @Nullable Integer workerConcurrency,
      @Nullable RateLimit rateLimit,
      @Nullable Integer partitionConcurrency,
      @Nullable Integer partitionWorkerConcurrency,
      @Nullable RateLimit partitionRateLimit) {}

  public Queue {
    Objects.requireNonNull(name, "Queue name must not be null");
    // Every queue dispatches in priority order, so the flag carries no information. Normalizing it
    // here also covers rows written as false by earlier versions.
    priorityEnabled = true;
    Objects.requireNonNull(pollingInterval, "Queue pollingInterval must not be null");
    if (concurrency != null && concurrency <= 0)
      throw new IllegalArgumentException(
          "If specified, queue concurrency must be greater than zero");
    if (workerConcurrency != null && workerConcurrency <= 0)
      throw new IllegalArgumentException(
          "If specified, queue workerConcurrency must be greater than zero");
    if (partitionConcurrency != null && partitionConcurrency <= 0)
      throw new IllegalArgumentException(
          "If specified, queue partitionConcurrency must be greater than zero");
    if (partitionWorkerConcurrency != null && partitionWorkerConcurrency <= 0)
      throw new IllegalArgumentException(
          "If specified, queue partitionWorkerConcurrency must be greater than zero");
    // A limit enforced at a narrower scope can never usefully exceed one enforced at a wider
    // scope: the wider limit would bind first and the narrower one would never be reached.
    if (partitionWorkerConcurrency != null
        && partitionConcurrency != null
        && partitionWorkerConcurrency > partitionConcurrency)
      throw new IllegalArgumentException(
          "Queue partitionConcurrency must be greater than or equal to partitionWorkerConcurrency");
    if (partitionWorkerConcurrency != null
        && workerConcurrency != null
        && partitionWorkerConcurrency > workerConcurrency)
      throw new IllegalArgumentException(
          "Queue workerConcurrency must be greater than or equal to partitionWorkerConcurrency");
    if (partitionConcurrency != null && concurrency != null && partitionConcurrency > concurrency)
      throw new IllegalArgumentException(
          "Queue concurrency must be greater than or equal to partitionConcurrency");
    if (partitionWorkerConcurrency != null
        && concurrency != null
        && partitionWorkerConcurrency > concurrency)
      throw new IllegalArgumentException(
          "Queue concurrency must be greater than or equal to partitionWorkerConcurrency");
    // Only the per-partition rate limit is validated here. The queue-wide one is not, and
    // neither is concurrency >= workerConcurrency, though Go, Python and TypeScript check both:
    // this constructor is also the read path (QueuesDAO.queueFromResultSet builds through it),
    // so a rule added here rejects rows already in the database, and one unreadable row takes
    // listQueues -- and with it dynamic queue discovery -- down with it. Those two rules are
    // enforced on write instead, by validateForRegistration. The per-partition rate limit can stay
    // here: its columns are new, and every SDK that writes them validates them, so no stored row
    // should fail this check.
    validateRateLimit("partitionRateLimit", partitionRateLimit);
    if (pollingInterval.isNegative() || pollingInterval.isZero())
      throw new IllegalArgumentException("Queue pollingInterval must be greater than zero");
  }

  private static void validateRateLimit(String name, @Nullable RateLimit rateLimit) {
    if (rateLimit == null) return;
    if (rateLimit.limit() <= 0)
      throw new IllegalArgumentException(
          "Queue %s limit must be greater than zero".formatted(name));
    if (rateLimit.period() == null
        || rateLimit.period().isNegative()
        || rateLimit.period().isZero())
      throw new IllegalArgumentException(
          "Queue %s period must be greater than zero".formatted(name));
  }

  /**
   * Constructs a queue with no per-partition limits.
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
      @NonNull Duration pollingInterval,
      @Nullable String applicationName) {
    this(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        null,
        null,
        null,
        pollingInterval,
        applicationName);
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
   * @deprecated A {@code Queue} is what {@link dev.dbos.transact.DBOS#findQueue(String)} returns,
   *     not something to build. Register with {@link dev.dbos.transact.DBOS#registerQueue(String,
   *     QueueOptions)}.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue(@NonNull String name) {
    this(name, null, null, false, false, null, DEFAULT_POLLING_INTERVAL, null);
  }

  /**
   * This queue's name as a {@link QueueName}, for the APIs that address a queue by one.
   *
   * @return the queue's name
   */
  public @NonNull QueueName queueName() {
    return QueueName.of(name);
  }

  /**
   * Always {@code true}: every queue dispatches in priority order.
   *
   * @deprecated Priority ordering is no longer optional, so there is nothing to ask.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public boolean priorityEnabled() {
    return priorityEnabled;
  }

  /**
   * Whether the deprecated partitioning flag is set on this queue, as stored. It says that the
   * queue partitions, but not why: once the stored column is derived from the per-partition limits,
   * a queue that partitions because of a limit will read back with the flag set too.
   *
   * @deprecated Use {@link #isPartitioned()} to ask whether the queue dequeues per partition, or
   *     {@link #isLegacyPartitioned()} to ask whether its queue-wide limits are enforced per
   *     partition.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public boolean partitioningEnabled() {
    return partitioningEnabled;
  }

  /**
   * @return true if the Queue has queue-wide rate-limiting enforced
   */
  public boolean hasLimiter() {
    return rateLimit != null;
  }

  /**
   * @return true if any per-partition limit is set
   */
  public boolean hasPartitionLimits() {
    return partitionConcurrency != null
        || partitionWorkerConcurrency != null
        || partitionRateLimit != null;
  }

  /**
   * @return true if the queue dequeues one partition key at a time
   */
  public boolean isPartitioned() {
    return partitioningEnabled || hasPartitionLimits();
  }

  /**
   * @return true if this is the deprecated mode in which the queue-wide limits are enforced per
   *     partition rather than across the queue
   */
  public boolean isLegacyPartitioned() {
    return partitioningEnabled && !hasPartitionLimits();
  }

  /**
   * Checks the rules that can only be applied to a queue being written, not to one being read.
   *
   * <p>Go, Python and TypeScript enforce these in the constructor. Java cannot, because its
   * constructor is also the read path -- {@code QueuesDAO.queueFromResultSet} builds a {@code
   * Queue} out of every row it loads -- and Java accepted {@code workerConcurrency > concurrency}
   * for long enough that such rows exist. Enforcing them on read would make those rows unloadable,
   * and one unloadable row stops queue discovery for the whole process, silently.
   *
   * <p>So they are enforced where a row is created or changed instead, which is where the other
   * three SDKs enforce them too. A row already stored in violation keeps loading, and is only
   * rejected if something tries to write it again.
   *
   * @throws IllegalArgumentException if this queue may not be written
   */
  public void validateForRegistration() {
    if (workerConcurrency != null && concurrency != null && workerConcurrency > concurrency)
      throw new IllegalArgumentException(
          "Queue concurrency must be greater than or equal to workerConcurrency");
    validateRateLimit("rateLimit", rateLimit);
  }

  /** Maps each of the queue's limits to the scope it is enforced at. */
  public @NonNull ResolvedLimits resolveLimits() {
    if (isLegacyPartitioned()) {
      return new ResolvedLimits(null, null, null, concurrency, workerConcurrency, rateLimit);
    }
    return new ResolvedLimits(
        concurrency,
        workerConcurrency,
        rateLimit,
        partitionConcurrency,
        partitionWorkerConcurrency,
        partitionRateLimit);
  }

  /**
   * Produces a new Queue with the assigned name.
   *
   * @deprecated Configure a queue with {@link QueueOptions} at registration.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withName(@NonNull String name) {
    return copyWith(
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
   *
   * @deprecated Configure a queue with {@link QueueOptions} at registration.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withConcurrency(@Nullable Integer concurrency) {
    return copyWith(
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
   *
   * @deprecated Configure a queue with {@link QueueOptions} at registration.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withWorkerConcurrency(@Nullable Integer workerConcurrency) {
    return copyWith(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval);
  }

  /**
   * Returns this queue unchanged: every queue dispatches in priority order.
   *
   * @deprecated Priority ordering is no longer optional; remove the call.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withPriorityEnabled(boolean priorityEnabled) {
    return copyWith(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval);
  }

  /**
   * Produces a new Queue with partitioning enabled/disabled.
   *
   * @deprecated Set a per-partition limit instead, which partitions the queue on its own.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withPartitioningEnabled(boolean partitioningEnabled) {
    return copyWith(
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
   *
   * @deprecated Configure a queue with {@link QueueOptions} at registration.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Queue withRateLimit(@Nullable RateLimit rateLimit) {
    return copyWith(
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
    return copyWith(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval);
  }

  /** Rebuilds the queue from the fields the deprecated withers can change, carrying the rest. */
  private Queue copyWith(
      String name,
      @Nullable Integer concurrency,
      @Nullable Integer workerConcurrency,
      boolean priorityEnabled,
      boolean partitioningEnabled,
      @Nullable RateLimit rateLimit,
      Duration pollingInterval) {
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        partitionConcurrency,
        partitionWorkerConcurrency,
        partitionRateLimit,
        pollingInterval,
        applicationName);
  }
}
