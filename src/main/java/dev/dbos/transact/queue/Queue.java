package dev.dbos.transact.queue;

import java.util.Objects;

public record Queue(
    String name,
    int concurrency,
    int workerConcurrency,
    boolean priorityEnabled,
    RateLimit rateLimit) {

  public Queue {
    Objects.requireNonNull(name, "Queue name must not be null");
  }

  public Queue(String name) {
    this(name, -1, -1, false, null);
  }

  public boolean hasLimiter() {
    return rateLimit != null;
  }

  public static record RateLimit(int limit, double period) {}

  public Queue withName(String name) {
    return new Queue(name, concurrency, workerConcurrency, priorityEnabled, rateLimit);
  }

  public Queue withConcurrency(int concurrency) {
    return new Queue(name, concurrency, workerConcurrency, priorityEnabled, rateLimit);
  }

  public Queue withWorkerConcurrency(int workerConcurrency) {
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
