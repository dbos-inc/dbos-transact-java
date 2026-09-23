package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.Queue;

public record QueueOutput(
    String name,
    Integer concurrency,
    Integer worker_concurrency,
    Integer rate_limit_max,
    Double rate_limit_period_sec,
    Integer partition_concurrency,
    Integer partition_worker_concurrency,
    Integer partition_rate_limit_max,
    Double partition_rate_limit_period_sec,
    boolean priority_enabled,
    boolean partition_queue,
    double polling_interval_sec,
    String application_name) {

  public static QueueOutput from(Queue q) {
    Queue.RateLimit rl = q.rateLimit();
    Queue.RateLimit prl = q.partitionRateLimit();
    return new QueueOutput(
        q.name(),
        q.concurrency(),
        q.workerConcurrency(),
        rl != null ? rl.limit() : null,
        rl != null ? rl.period().toMillis() / 1000.0 : null,
        q.partitionConcurrency(),
        q.partitionWorkerConcurrency(),
        prl != null ? prl.limit() : null,
        prl != null ? prl.period().toMillis() / 1000.0 : null,
        true, // every queue dispatches in priority order
        // Conductor reads partition_queue to tell whether a queue dequeues per partition, which
        // per-partition limits decide just as much as the legacy flag does.
        q.isPartitioned(),
        q.pollingInterval().toMillis() / 1000.0,
        q.applicationName());
  }
}
