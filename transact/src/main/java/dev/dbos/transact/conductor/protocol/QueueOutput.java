package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.Queue;

public record QueueOutput(
    String name,
    Integer concurrency,
    Integer worker_concurrency,
    Integer rate_limit_max,
    Double rate_limit_period_sec,
    boolean priority_enabled,
    boolean partition_queue,
    double polling_interval_sec) {

  public static QueueOutput from(Queue q) {
    Queue.RateLimit rl = q.rateLimit();
    return new QueueOutput(
        q.name(),
        q.concurrency(),
        q.workerConcurrency(),
        rl != null ? rl.limit() : null,
        rl != null ? rl.period().toMillis() / 1000.0 : null,
        q.priorityEnabled(),
        q.partitioningEnabled(),
        q.pollingInterval().toMillis() / 1000.0);
  }
}
