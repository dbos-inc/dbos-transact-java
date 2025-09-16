package dev.dbos.transact;

import dev.dbos.transact.queue.Queue;

import java.time.Duration;
import java.util.OptionalInt;
import java.util.concurrent.TimeUnit;

public record StartWorkflowOptions(
    String workflowId,
    Duration timeout,
    String queueName,
    String deduplicationId,
    OptionalInt priority) {

  public StartWorkflowOptions() {
    this(null, null, null, null, OptionalInt.empty());
  }

  public StartWorkflowOptions(String workflowId) {
    this(workflowId, null, null, null, OptionalInt.empty());
  }

  public StartWorkflowOptions withTimeout(Duration timeout) {
    return new StartWorkflowOptions(
        this.workflowId, timeout, this.queueName, this.deduplicationId, this.priority);
  }

  public StartWorkflowOptions withTimeout(long value, TimeUnit unit) {
    var timeout = Duration.ofNanos(unit.toNanos(value));
    return new StartWorkflowOptions(
        this.workflowId, timeout, this.queueName, this.deduplicationId, this.priority);
  }

  public StartWorkflowOptions withQueue(Queue queue) {
    return new StartWorkflowOptions(
        this.workflowId, this.timeout, queue.getName(), this.deduplicationId, this.priority);
  }

  public StartWorkflowOptions withQueue(Queue queue, String deduplicationId) {
    return new StartWorkflowOptions(
        this.workflowId, this.timeout, queue.getName(), deduplicationId, this.priority);
  }

  public StartWorkflowOptions withQueue(Queue queue, String deduplicationId, int priority) {
    return new StartWorkflowOptions(
        this.workflowId, this.timeout, queue.getName(), deduplicationId, OptionalInt.of(priority));
  }
}
