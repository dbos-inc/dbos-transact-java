package dev.dbos.transact.workflow;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Configuration options for a DBOS workflow queue. Used for both registration of database-backed
 * queues and partial updates to queue configuration.
 *
 * <p>When used for registration, absent and null fields both result in the column being null (no
 * limit / use default). When used for updates, absent fields are left unchanged in the database
 * while null-valued fields clear the column.
 *
 * <p>The non-nullable queue properties ({@code partitionQueue}, {@code pollingInterval}) use {@link
 * Optional} — {@link Optional#empty()} means use the default on creation or leave unchanged on
 * update; a present value sets the column.
 *
 * <p>A queue can carry flow control at two scopes at once: the queue-wide limits bound the queue as
 * a whole, and the per-partition limits bound each partition key independently. Setting any
 * per-partition limit is what partitions the queue, so {@code partitionQueue} is redundant
 * alongside one: registration accepts it and it adds nothing, while an update refuses it on a queue
 * already partitioned by its per-partition limits. A queue registered with {@code partitionQueue}
 * alone keeps its limits frozen; re-register it with per-partition limits to move it across.
 *
 * <p>A rate limit's max and period are set and cleared together, at registration and on update
 * alike; supplying only one of them is refused.
 *
 * @param concurrency max concurrent executions of this queue across all workers; {@link
 *     Field#absent()} means no limit on creation or leave unchanged on update; {@code
 *     Field.of(null)} clears the column
 * @param workerConcurrency max concurrent executions of this queue per worker process; {@link
 *     Field#absent()} means no limit on creation or leave unchanged on update; {@code
 *     Field.of(null)} clears the column
 * @param rateLimitMax maximum number of starts allowed in each rate-limit window; must be paired
 *     with {@code rateLimitPeriod}; {@link Field#absent()} means no rate limit or leave unchanged
 * @param rateLimitPeriod duration of the rolling rate-limit window; must be paired with {@code
 *     rateLimitMax}; {@link Field#absent()} means no rate limit or leave unchanged
 * @param partitionConcurrency max concurrent executions of any one partition of this queue across
 *     all workers; setting it partitions the queue
 * @param partitionWorkerConcurrency max concurrent executions of any one partition of this queue
 *     per worker process; setting it partitions the queue
 * @param partitionRateLimitMax maximum number of starts allowed per partition in each rate-limit
 *     window; must be paired with {@code partitionRateLimitPeriod}; setting it partitions the queue
 * @param partitionRateLimitPeriod duration of the rolling per-partition rate-limit window; must be
 *     paired with {@code partitionRateLimitMax}
 * @param priorityEnabled ignored: every queue dispatches in priority order
 * @param partitionQueue whether to partition queue entries so each partition key gets its own
 *     concurrency slot, with the queue-wide limits applied per partition; {@link Optional#empty()}
 *     means use the default or leave unchanged
 * @param pollingInterval how often workers poll the database for new queue entries; {@link
 *     Optional#empty()} means use the default or leave unchanged
 */
public record QueueOptions(
    @NonNull Field<Integer> concurrency,
    @NonNull Field<Integer> workerConcurrency,
    @NonNull Field<Integer> rateLimitMax,
    @NonNull Field<Duration> rateLimitPeriod,
    @NonNull Field<Integer> partitionConcurrency,
    @NonNull Field<Integer> partitionWorkerConcurrency,
    @NonNull Field<Integer> partitionRateLimitMax,
    @NonNull Field<Duration> partitionRateLimitPeriod,
    @NonNull Optional<Boolean> priorityEnabled,
    @NonNull Optional<Boolean> partitionQueue,
    @NonNull Optional<Duration> pollingInterval) {

  public QueueOptions {
    // Every queue dispatches in priority order, so the flag carries no information. Dropping it
    // here keeps options that set only it equal to, and as empty as, new QueueOptions().
    priorityEnabled = Optional.empty();
  }

  /**
   * Constructs options with every property absent; no queue property will be set or changed. Chain
   * the {@code with} builders from here to set properties, e.g. {@code new
   * QueueOptions().withConcurrency(10).withRateLimit(100, Duration.ofSeconds(60))}.
   */
  public QueueOptions() {
    this(
        Field.absent(),
        Field.absent(),
        Field.absent(),
        Field.absent(),
        Field.absent(),
        Field.absent(),
        Field.absent(),
        Field.absent(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  /**
   * Constructs options with no per-partition limits.
   *
   * @deprecated Retained for source compatibility with the pre-per-partition-limit shape. Use the
   *     canonical constructor, or the {@code with} builders.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public QueueOptions(
      @NonNull Field<Integer> concurrency,
      @NonNull Field<Integer> workerConcurrency,
      @NonNull Field<Integer> rateLimitMax,
      @NonNull Field<Duration> rateLimitPeriod,
      @NonNull Optional<Boolean> priorityEnabled,
      @NonNull Optional<Boolean> partitionQueue,
      @NonNull Optional<Duration> pollingInterval) {
    this(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        Field.absent(),
        Field.absent(),
        Field.absent(),
        Field.absent(),
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  /**
   * The deprecated partitioning flag, as set on these options.
   *
   * @deprecated Superseded by the per-partition limits, any of which partitions the queue on its
   *     own: {@link #partitionConcurrency()}, {@link #partitionWorkerConcurrency()}, {@link
   *     #partitionRateLimitMax()}.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  @Override
  public @NonNull Optional<Boolean> partitionQueue() {
    return partitionQueue;
  }

  /**
   * Always empty: the deprecated priority flag is dropped when the options are built, since every
   * queue dispatches in priority order.
   *
   * @deprecated Priority ordering is no longer optional.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  @Override
  public @NonNull Optional<Boolean> priorityEnabled() {
    return priorityEnabled;
  }

  /**
   * Returns options with every property absent.
   *
   * @deprecated Use {@link #QueueOptions()}, as with the other options types.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public static @NonNull QueueOptions empty() {
    return new QueueOptions();
  }

  /** Returns {@code true} if all fields are absent — no property will be set or changed. */
  public boolean isEmpty() {
    return !concurrency.isPresent()
        && !workerConcurrency.isPresent()
        && !rateLimitMax.isPresent()
        && !rateLimitPeriod.isPresent()
        && !partitionConcurrency.isPresent()
        && !partitionWorkerConcurrency.isPresent()
        && !partitionRateLimitMax.isPresent()
        && !partitionRateLimitPeriod.isPresent()
        && partitionQueue.isEmpty()
        && pollingInterval.isEmpty();
  }

  // ── Deprecated static factories ───────────────────────────────────────────

  /**
   * Creates options that set only {@code concurrency}; all other fields are absent.
   *
   * @param value the concurrency limit, or {@code null} to clear the column
   * @deprecated Use {@code new QueueOptions().withConcurrency(value)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public static @NonNull QueueOptions setConcurrency(@Nullable Integer value) {
    return new QueueOptions().withConcurrency(value);
  }

  /**
   * Creates options that set only {@code workerConcurrency}; all other fields are absent.
   *
   * @param value the per-worker concurrency limit, or {@code null} to clear the column
   * @deprecated Use {@code new QueueOptions().withWorkerConcurrency(value)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public static @NonNull QueueOptions setWorkerConcurrency(@Nullable Integer value) {
    return new QueueOptions().withWorkerConcurrency(value);
  }

  /**
   * Creates options that set only the rate limit; all other fields are absent.
   *
   * @param max max starts per window, or {@code null} to clear
   * @param period length of the rolling window, or {@code null} to clear
   * @deprecated Use {@code new QueueOptions().withRateLimit(max, period)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public static @NonNull QueueOptions setRateLimit(
      @Nullable Integer max, @Nullable Duration period) {
    return new QueueOptions().withRateLimit(max, period);
  }

  /**
   * Creates options that set only the rate limit; all other fields are absent.
   *
   * @param limit max starts per window
   * @param period length of the rolling window
   * @param unit time unit for {@code period}
   * @deprecated Use {@code new QueueOptions().withRateLimit(limit, period, unit)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public static @NonNull QueueOptions setRateLimit(int limit, long period, @NonNull TimeUnit unit) {
    return new QueueOptions().withRateLimit(limit, period, unit);
  }

  /**
   * Creates options that set only {@code partitionConcurrency}; all other fields are absent.
   * Setting it is what partitions the queue.
   *
   * @param value the per-partition concurrency limit, or {@code null} to clear the column
   * @deprecated Use {@code new QueueOptions().withPartitionConcurrency(value)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public static @NonNull QueueOptions setPartitionConcurrency(@Nullable Integer value) {
    return new QueueOptions().withPartitionConcurrency(value);
  }

  /**
   * Creates options that set only {@code partitionWorkerConcurrency}; all other fields are absent.
   * Setting it is what partitions the queue.
   *
   * @param value the per-partition, per-worker concurrency limit, or {@code null} to clear
   * @deprecated Use {@code new QueueOptions().withPartitionWorkerConcurrency(value)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public static @NonNull QueueOptions setPartitionWorkerConcurrency(@Nullable Integer value) {
    return new QueueOptions().withPartitionWorkerConcurrency(value);
  }

  /**
   * Creates options that set only the per-partition rate limit; all other fields are absent.
   * Setting it is what partitions the queue.
   *
   * @param max max starts per window per partition, or {@code null} to clear
   * @param period length of the rolling window, or {@code null} to clear
   * @deprecated Use {@code new QueueOptions().withPartitionRateLimit(max, period)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public static @NonNull QueueOptions setPartitionRateLimit(
      @Nullable Integer max, @Nullable Duration period) {
    return new QueueOptions().withPartitionRateLimit(max, period);
  }

  /**
   * Creates options that set only the per-partition rate limit; all other fields are absent.
   *
   * @param limit max starts per window per partition
   * @param period length of the rolling window
   * @param unit time unit for {@code period}
   * @deprecated Use {@code new QueueOptions().withPartitionRateLimit(limit, period, unit)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public static @NonNull QueueOptions setPartitionRateLimit(
      int limit, long period, @NonNull TimeUnit unit) {
    return new QueueOptions().withPartitionRateLimit(limit, period, unit);
  }

  /**
   * Creates options that set only {@code priorityEnabled}; all other fields are absent.
   *
   * @param value ignored
   * @deprecated Every queue dispatches in priority order; remove the call.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public static @NonNull QueueOptions setPriorityEnabled(boolean value) {
    return new QueueOptions().withPriorityEnabled(Optional.of(value));
  }

  /**
   * Creates options that set only {@code partitionQueue}; all other fields are absent.
   *
   * @param value {@code true} to enable queue partitioning, {@code false} to disable
   * @deprecated Set {@link #withPartitionConcurrency(Integer)}, {@link
   *     #withPartitionWorkerConcurrency(Integer)} or {@link #withPartitionRateLimit(Integer,
   *     Duration)} instead, any of which partitions the queue.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public static @NonNull QueueOptions setPartitionQueue(boolean value) {
    return new QueueOptions().withPartitionQueue(Optional.of(value));
  }

  /**
   * Creates options that set only {@code pollingInterval}; all other fields are absent.
   *
   * @param value the interval at which workers poll for new queue entries
   * @deprecated Use {@code new QueueOptions().withPollingInterval(value)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public static @NonNull QueueOptions setPollingInterval(@NonNull Duration value) {
    return new QueueOptions().withPollingInterval(value);
  }

  // ── Deprecated Field and Optional builders ────────────────────────────────

  /**
   * Returns a copy of these options with {@code concurrency} replaced.
   *
   * @param concurrency the new value; use {@link Field#absent()} to leave unchanged, or {@code
   *     Field.of(null)} to clear the column
   * @deprecated Use {@link #withConcurrency(Integer)}; {@code null} clears the column.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions withConcurrency(@NonNull Field<Integer> concurrency) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        partitionConcurrency,
        partitionWorkerConcurrency,
        partitionRateLimitMax,
        partitionRateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  /**
   * Returns a copy of these options with {@code workerConcurrency} replaced.
   *
   * @param workerConcurrency the new value; use {@link Field#absent()} to leave unchanged, or
   *     {@code Field.of(null)} to clear the column
   * @deprecated Use {@link #withWorkerConcurrency(Integer)}; {@code null} clears the column.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions withWorkerConcurrency(@NonNull Field<Integer> workerConcurrency) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        partitionConcurrency,
        partitionWorkerConcurrency,
        partitionRateLimitMax,
        partitionRateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  /**
   * Returns a copy of these options with {@code rateLimitMax} replaced.
   *
   * @param rateLimitMax the new value; use {@link Field#absent()} to leave unchanged, or {@code
   *     Field.of(null)} to clear the column
   * @deprecated Use {@link #withRateLimitMax(Integer)}; {@code null} clears the column.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions withRateLimitMax(@NonNull Field<Integer> rateLimitMax) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        partitionConcurrency,
        partitionWorkerConcurrency,
        partitionRateLimitMax,
        partitionRateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  /**
   * Returns a copy of these options with {@code rateLimitPeriod} replaced.
   *
   * @param rateLimitPeriod the new value; use {@link Field#absent()} to leave unchanged, or {@code
   *     Field.of(null)} to clear the column
   * @deprecated Use {@link #withRateLimitPeriod(Duration)}; {@code null} clears the column.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions withRateLimitPeriod(@NonNull Field<Duration> rateLimitPeriod) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        partitionConcurrency,
        partitionWorkerConcurrency,
        partitionRateLimitMax,
        partitionRateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  /**
   * Returns a copy of these options with {@code partitionConcurrency} replaced.
   *
   * @param partitionConcurrency the new value; use {@link Field#absent()} to leave unchanged, or
   *     {@code Field.of(null)} to clear the column
   * @deprecated Use {@link #withPartitionConcurrency(Integer)}; {@code null} clears the column.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions withPartitionConcurrency(
      @NonNull Field<Integer> partitionConcurrency) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        partitionConcurrency,
        partitionWorkerConcurrency,
        partitionRateLimitMax,
        partitionRateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  /**
   * Returns a copy of these options with {@code partitionWorkerConcurrency} replaced.
   *
   * @param partitionWorkerConcurrency the new value; use {@link Field#absent()} to leave unchanged,
   *     or {@code Field.of(null)} to clear the column
   * @deprecated Use {@link #withPartitionWorkerConcurrency(Integer)}; {@code null} clears the
   *     column.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions withPartitionWorkerConcurrency(
      @NonNull Field<Integer> partitionWorkerConcurrency) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        partitionConcurrency,
        partitionWorkerConcurrency,
        partitionRateLimitMax,
        partitionRateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  /**
   * Returns a copy of these options with {@code partitionRateLimitMax} replaced.
   *
   * @param partitionRateLimitMax the new value; use {@link Field#absent()} to leave unchanged, or
   *     {@code Field.of(null)} to clear the column
   * @deprecated Use {@link #withPartitionRateLimitMax(Integer)}; {@code null} clears the column.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions withPartitionRateLimitMax(
      @NonNull Field<Integer> partitionRateLimitMax) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        partitionConcurrency,
        partitionWorkerConcurrency,
        partitionRateLimitMax,
        partitionRateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  /**
   * Returns a copy of these options with {@code partitionRateLimitPeriod} replaced.
   *
   * @param partitionRateLimitPeriod the new value; use {@link Field#absent()} to leave unchanged,
   *     or {@code Field.of(null)} to clear the column
   * @deprecated Use {@link #withPartitionRateLimitPeriod(Duration)}; {@code null} clears the
   *     column.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions withPartitionRateLimitPeriod(
      @NonNull Field<Duration> partitionRateLimitPeriod) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        partitionConcurrency,
        partitionWorkerConcurrency,
        partitionRateLimitMax,
        partitionRateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  /**
   * Returns a copy of these options with {@code priorityEnabled} replaced.
   *
   * @param priorityEnabled ignored
   * @deprecated Every queue dispatches in priority order; remove the call.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public @NonNull QueueOptions withPriorityEnabled(@NonNull Optional<Boolean> priorityEnabled) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        partitionConcurrency,
        partitionWorkerConcurrency,
        partitionRateLimitMax,
        partitionRateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  /**
   * Returns a copy of these options with {@code partitionQueue} replaced.
   *
   * @param partitionQueue the new value; use {@link Optional#empty()} to leave unchanged
   * @deprecated Set {@link #withPartitionConcurrency(Integer)}, {@link
   *     #withPartitionWorkerConcurrency(Integer)} or {@link #withPartitionRateLimit(Integer,
   *     Duration)} instead, any of which partitions the queue.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public @NonNull QueueOptions withPartitionQueue(@NonNull Optional<Boolean> partitionQueue) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        partitionConcurrency,
        partitionWorkerConcurrency,
        partitionRateLimitMax,
        partitionRateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  /**
   * Returns a copy of these options with {@code pollingInterval} replaced.
   *
   * @param pollingInterval the new value; use {@link Optional#empty()} to leave unchanged
   * @deprecated Use {@link #withPollingInterval(Duration)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions withPollingInterval(@NonNull Optional<Duration> pollingInterval) {
    return new QueueOptions(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriod,
        partitionConcurrency,
        partitionWorkerConcurrency,
        partitionRateLimitMax,
        partitionRateLimitPeriod,
        priorityEnabled,
        partitionQueue,
        pollingInterval);
  }

  // ── Plain-value builders ──────────────────────────────────────────────────
  //
  // Until the deprecated Field overloads above are removed, a bare null literal is ambiguous
  // between them and these; clearing a column is written withConcurrency((Integer) null).

  /**
   * Returns a copy of these options with {@code concurrency} set to the given value.
   *
   * @param value the concurrency limit, or {@code null} to clear the column
   */
  public @NonNull QueueOptions withConcurrency(@Nullable Integer value) {
    return withConcurrency(Field.of(value));
  }

  /**
   * Returns a copy of these options with {@code workerConcurrency} set to the given value.
   *
   * @param value the per-worker concurrency limit, or {@code null} to clear the column
   */
  public @NonNull QueueOptions withWorkerConcurrency(@Nullable Integer value) {
    return withWorkerConcurrency(Field.of(value));
  }

  /**
   * Returns a copy of these options with the rate limit set. The max and period are set and cleared
   * together.
   *
   * @param max max starts per window, or {@code null} to clear
   * @param period length of the rolling window, or {@code null} to clear
   */
  public @NonNull QueueOptions withRateLimit(@Nullable Integer max, @Nullable Duration period) {
    return withRateLimitMax(Field.of(max)).withRateLimitPeriod(Field.of(period));
  }

  /**
   * Returns a copy of these options with the rate limit set.
   *
   * @param max max starts per window
   * @param period length of the rolling window
   * @param unit time unit for {@code period}
   */
  public @NonNull QueueOptions withRateLimit(int max, long period, @NonNull TimeUnit unit) {
    return withRateLimit(max, Duration.of(period, unit.toChronoUnit()));
  }

  /**
   * Returns a copy of these options with only the rate limit's max set. On {@code updateQueue} the
   * period carries over from the stored limit; where there is none, and at registration, a max
   * without a period is refused.
   *
   * @param max max starts per window, or {@code null} to clear (which must clear the period too)
   */
  public @NonNull QueueOptions withRateLimitMax(@Nullable Integer max) {
    return withRateLimitMax(Field.of(max));
  }

  /**
   * Returns a copy of these options with only the rate limit's period set. On {@code updateQueue}
   * the max carries over from the stored limit; where there is none, and at registration, a period
   * without a max is refused.
   *
   * @param period length of the rolling window, or {@code null} to clear (which must clear the max
   *     too)
   */
  public @NonNull QueueOptions withRateLimitPeriod(@Nullable Duration period) {
    return withRateLimitPeriod(Field.of(period));
  }

  /**
   * Returns a copy of these options with {@code partitionConcurrency} set to the given value.
   * Setting it is what partitions the queue.
   *
   * @param value the per-partition concurrency limit, or {@code null} to clear the column
   */
  public @NonNull QueueOptions withPartitionConcurrency(@Nullable Integer value) {
    return withPartitionConcurrency(Field.of(value));
  }

  /**
   * Returns a copy of these options with {@code partitionWorkerConcurrency} set to the given value.
   * Setting it is what partitions the queue.
   *
   * @param value the per-partition, per-worker concurrency limit, or {@code null} to clear
   */
  public @NonNull QueueOptions withPartitionWorkerConcurrency(@Nullable Integer value) {
    return withPartitionWorkerConcurrency(Field.of(value));
  }

  /**
   * Returns a copy of these options with the per-partition rate limit set. Setting it partitions
   * the queue. The max and period are set and cleared together.
   *
   * @param max max starts per window per partition, or {@code null} to clear
   * @param period length of the rolling window, or {@code null} to clear
   */
  public @NonNull QueueOptions withPartitionRateLimit(
      @Nullable Integer max, @Nullable Duration period) {
    return withPartitionRateLimitMax(Field.of(max)).withPartitionRateLimitPeriod(Field.of(period));
  }

  /**
   * Returns a copy of these options with the per-partition rate limit set.
   *
   * @param max max starts per window per partition
   * @param period length of the rolling window
   * @param unit time unit for {@code period}
   */
  public @NonNull QueueOptions withPartitionRateLimit(
      int max, long period, @NonNull TimeUnit unit) {
    return withPartitionRateLimit(max, Duration.of(period, unit.toChronoUnit()));
  }

  /**
   * Returns a copy of these options with only the per-partition rate limit's max set. On {@code
   * updateQueue} the period carries over from the stored limit; where there is none, and at
   * registration, a max without a period is refused.
   *
   * @param max max starts per window per partition, or {@code null} to clear (which must clear the
   *     period too)
   */
  public @NonNull QueueOptions withPartitionRateLimitMax(@Nullable Integer max) {
    return withPartitionRateLimitMax(Field.of(max));
  }

  /**
   * Returns a copy of these options with only the per-partition rate limit's period set. On {@code
   * updateQueue} the max carries over from the stored limit; where there is none, and at
   * registration, a period without a max is refused.
   *
   * @param period length of the rolling window, or {@code null} to clear (which must clear the max
   *     too)
   */
  public @NonNull QueueOptions withPartitionRateLimitPeriod(@Nullable Duration period) {
    return withPartitionRateLimitPeriod(Field.of(period));
  }

  /**
   * Returns a copy of these options with {@code pollingInterval} set to the given value.
   *
   * @param value the interval at which workers poll for new queue entries
   */
  public @NonNull QueueOptions withPollingInterval(@NonNull Duration value) {
    return withPollingInterval(Optional.of(value));
  }

  // ── Deprecated chaining methods ───────────────────────────────────────────

  /**
   * Returns a copy of these options with {@code concurrency} set to the given value.
   *
   * @param value the concurrency limit, or {@code null} to clear the column
   * @deprecated Use {@link #withConcurrency(Integer)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions andConcurrency(@Nullable Integer value) {
    return withConcurrency(value);
  }

  /**
   * Returns a copy of these options with {@code workerConcurrency} set to the given value.
   *
   * @param value the per-worker concurrency limit, or {@code null} to clear the column
   * @deprecated Use {@link #withWorkerConcurrency(Integer)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions andWorkerConcurrency(@Nullable Integer value) {
    return withWorkerConcurrency(value);
  }

  /**
   * Returns a copy of these options with the rate limit set.
   *
   * @param max max starts per window, or {@code null} to clear
   * @param period length of the rolling window, or {@code null} to clear
   * @deprecated Use {@link #withRateLimit(Integer, Duration)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions andRateLimit(@Nullable Integer max, @Nullable Duration period) {
    return withRateLimit(max, period);
  }

  /**
   * Returns a copy of these options with the rate limit set.
   *
   * @param max max starts per window
   * @param period length of the rolling window
   * @param unit time unit for {@code period}
   * @deprecated Use {@link #withRateLimit(int, long, TimeUnit)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions andRateLimit(int max, long period, @NonNull TimeUnit unit) {
    return withRateLimit(max, period, unit);
  }

  /**
   * Returns a copy of these options with {@code partitionConcurrency} set to the given value.
   * Setting it is what partitions the queue.
   *
   * @param value the per-partition concurrency limit, or {@code null} to clear the column
   * @deprecated Use {@link #withPartitionConcurrency(Integer)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions andPartitionConcurrency(@Nullable Integer value) {
    return withPartitionConcurrency(value);
  }

  /**
   * Returns a copy of these options with {@code partitionWorkerConcurrency} set to the given value.
   * Setting it is what partitions the queue.
   *
   * @param value the per-partition, per-worker concurrency limit, or {@code null} to clear
   * @deprecated Use {@link #withPartitionWorkerConcurrency(Integer)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions andPartitionWorkerConcurrency(@Nullable Integer value) {
    return withPartitionWorkerConcurrency(value);
  }

  /**
   * Returns a copy of these options with the per-partition rate limit set. Setting it partitions
   * the queue.
   *
   * @param max max starts per window per partition, or {@code null} to clear
   * @param period length of the rolling window, or {@code null} to clear
   * @deprecated Use {@link #withPartitionRateLimit(Integer, Duration)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions andPartitionRateLimit(
      @Nullable Integer max, @Nullable Duration period) {
    return withPartitionRateLimit(max, period);
  }

  /**
   * Returns a copy of these options with the per-partition rate limit set.
   *
   * @param max max starts per window per partition
   * @param period length of the rolling window
   * @param unit time unit for {@code period}
   * @deprecated Use {@link #withPartitionRateLimit(int, long, TimeUnit)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions andPartitionRateLimit(int max, long period, @NonNull TimeUnit unit) {
    return withPartitionRateLimit(max, period, unit);
  }

  /**
   * Returns a copy of these options with {@code priorityEnabled} set to the given value.
   *
   * @param value ignored
   * @deprecated Every queue dispatches in priority order; remove the call.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public @NonNull QueueOptions andPriorityEnabled(boolean value) {
    return withPriorityEnabled(Optional.of(value));
  }

  /**
   * Returns a copy of these options with {@code partitionQueue} set to the given value.
   *
   * @param value {@code true} to enable queue partitioning, {@code false} to disable
   * @deprecated Set {@link #withPartitionConcurrency(Integer)}, {@link
   *     #withPartitionWorkerConcurrency(Integer)} or {@link #withPartitionRateLimit(Integer,
   *     Duration)} instead, any of which partitions the queue.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public @NonNull QueueOptions andPartitionQueue(boolean value) {
    return withPartitionQueue(Optional.of(value));
  }

  /**
   * Returns a copy of these options with {@code pollingInterval} set to the given value.
   *
   * @param value the interval at which workers poll for new queue entries
   * @deprecated Use {@link #withPollingInterval(Duration)}.
   */
  @Deprecated(since = "1.2", forRemoval = true)
  public @NonNull QueueOptions andPollingInterval(@NonNull Duration value) {
    return withPollingInterval(value);
  }
}
