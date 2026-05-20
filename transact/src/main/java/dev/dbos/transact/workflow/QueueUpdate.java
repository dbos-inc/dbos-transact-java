package dev.dbos.transact.workflow;

import org.jspecify.annotations.Nullable;

/** Partial update specification for a queue row. Only present fields are written to the DB. */
public record QueueUpdate(
    Field<Integer> concurrency,
    Field<Integer> workerConcurrency,
    Field<Integer> rateLimitMax,
    Field<Double> rateLimitPeriodSec,
    Field<Boolean> priorityEnabled,
    Field<Boolean> partitionQueue,
    Field<Double> pollingIntervalSec) {

  private static final QueueUpdate EMPTY =
      new QueueUpdate(
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
        && !rateLimitPeriodSec.isPresent()
        && !priorityEnabled.isPresent()
        && !partitionQueue.isPresent()
        && !pollingIntervalSec.isPresent();
  }

  public static QueueUpdate setConcurrency(@Nullable Integer value) {
    return EMPTY.withConcurrency(Field.of(value));
  }

  public static QueueUpdate setWorkerConcurrency(@Nullable Integer value) {
    return EMPTY.withWorkerConcurrency(Field.of(value));
  }

  public static QueueUpdate setRateLimit(@Nullable Integer max, @Nullable Double periodSec) {
    return EMPTY.withRateLimitMax(Field.of(max)).withRateLimitPeriodSec(Field.of(periodSec));
  }

  public static QueueUpdate setPriorityEnabled(boolean value) {
    return EMPTY.withPriorityEnabled(Field.of(value));
  }

  public static QueueUpdate setPartitionQueue(boolean value) {
    return EMPTY.withPartitionQueue(Field.of(value));
  }

  public static QueueUpdate setPollingIntervalSec(@Nullable Double value) {
    return EMPTY.withPollingIntervalSec(Field.of(value));
  }

  // --- with* builders for chaining ---

  public QueueUpdate withConcurrency(Field<Integer> concurrency) {
    return new QueueUpdate(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriodSec,
        priorityEnabled,
        partitionQueue,
        pollingIntervalSec);
  }

  public QueueUpdate withWorkerConcurrency(Field<Integer> workerConcurrency) {
    return new QueueUpdate(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriodSec,
        priorityEnabled,
        partitionQueue,
        pollingIntervalSec);
  }

  public QueueUpdate withRateLimitMax(Field<Integer> rateLimitMax) {
    return new QueueUpdate(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriodSec,
        priorityEnabled,
        partitionQueue,
        pollingIntervalSec);
  }

  public QueueUpdate withRateLimitPeriodSec(Field<Double> rateLimitPeriodSec) {
    return new QueueUpdate(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriodSec,
        priorityEnabled,
        partitionQueue,
        pollingIntervalSec);
  }

  public QueueUpdate withPriorityEnabled(Field<Boolean> priorityEnabled) {
    return new QueueUpdate(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriodSec,
        priorityEnabled,
        partitionQueue,
        pollingIntervalSec);
  }

  public QueueUpdate withPartitionQueue(Field<Boolean> partitionQueue) {
    return new QueueUpdate(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriodSec,
        priorityEnabled,
        partitionQueue,
        pollingIntervalSec);
  }

  public QueueUpdate withPollingIntervalSec(Field<Double> pollingIntervalSec) {
    return new QueueUpdate(
        concurrency,
        workerConcurrency,
        rateLimitMax,
        rateLimitPeriodSec,
        priorityEnabled,
        partitionQueue,
        pollingIntervalSec);
  }
}
