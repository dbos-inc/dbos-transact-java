package dev.dbos.transact.queue;

import java.util.Objects;

public record Queue(
    String name,
    int concurrency,
    int workerConcurrency,
    boolean priorityEnabled,
    RateLimit rateLimit) {

  public Queue {
    Objects.requireNonNull(name);
    if (workerConcurrency > concurrency) {
      throw new IllegalArgumentException(
          String.format(
              "workerConcurrency must be less than or equal to concurrency for queue %s", name));
    }
  }

  public boolean hasLimiter() {
    return rateLimit != null;
  }

  public record RateLimit(int limit, double period) {}
}
