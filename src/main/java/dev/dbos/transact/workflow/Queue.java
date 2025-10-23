package dev.dbos.transact.workflow;

import java.util.Objects;

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

  public Queue(String name) {
    this(name, null, null, false, null);
  }

  public boolean hasLimiter() {
    return rateLimit != null;
  }

  public static record RateLimit(int limit, double period) {}

  public Queue withName(String name) {
    return new Queue(name, concurrency, workerConcurrency, priorityEnabled, rateLimit);
  }

  public Queue withConcurrency(Integer concurrency) {
    return new Queue(name, concurrency, workerConcurrency, priorityEnabled, rateLimit);
  }

  public Queue withWorkerConcurrency(Integer workerConcurrency) {
    return new Queue(name, concurrency, workerConcurrency, priorityEnabled, rateLimit);
  }

  public Queue withPriorityEnabled(boolean priorityEnabled) {
    return new Queue(name, concurrency, workerConcurrency, priorityEnabled, rateLimit);
  }

  public Queue withRateLimit(RateLimit rateLimit) {
    return new Queue(name, concurrency, workerConcurrency, priorityEnabled, rateLimit);
  }

  public Queue withRateLimit(int limit, double period) {
    return withRateLimit(new RateLimit(limit, period));
  }
}
