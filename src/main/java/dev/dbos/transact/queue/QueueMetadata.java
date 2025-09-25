package dev.dbos.transact.queue;

public record QueueMetadata(
    String name,
    int concurrency,
    int workerConcurrency,
    RateLimit rateLimit,
    boolean priorityEnabled) {

  public QueueMetadata(Queue queue) {
    this(
        queue.name(),
        queue.concurrency(),
        queue.workerConcurrency(),
        RateLimit.of(queue),
        queue.priorityEnabled());
  }

  public record RateLimit(Integer limit, Double period) {
    public static RateLimit of(Queue queue) {
      return queue.rateLimit() == null
          ? null
          : new RateLimit(queue.rateLimit().getLimit(), queue.rateLimit().getPeriod());
    }
  }
}
