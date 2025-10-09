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

  public StartWorkflowOptions {
    if (timeout != null && timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must not be negative");
    }
  }

  public StartWorkflowOptions() {
    this(null, null, null, null, OptionalInt.empty());
  }

  public StartWorkflowOptions(String workflowId) {
    this(workflowId, null, null, null, OptionalInt.empty());
  }

  public StartWorkflowOptions withWorkflowId(String workflowId) {
    return new StartWorkflowOptions(
        workflowId, this.timeout, this.queueName, this.deduplicationId, this.priority);
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
        this.workflowId, this.timeout, queue.name(), this.deduplicationId, this.priority);
  }

  public StartWorkflowOptions withDeduplicationId(String deduplicationId) {
    return new StartWorkflowOptions(
        this.workflowId, this.timeout, this.queueName, deduplicationId, this.priority);
  }

  public StartWorkflowOptions withPriority(int priority) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        this.queueName,
        this.deduplicationId,
        OptionalInt.of(priority));
  }

  @Override
  public String workflowId() {
    return workflowId != null && workflowId.isEmpty() ? null : workflowId;
  }
}
