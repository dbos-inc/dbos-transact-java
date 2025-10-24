package dev.dbos.transact;

import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public record StartWorkflowOptions(
    String workflowId,
    Timeout timeout,
    String queueName,
    String deduplicationId,
    Integer priority) {

  public StartWorkflowOptions {
    if (timeout instanceof Timeout.Explicit explicit) {
      if (explicit.value().isNegative() || explicit.value().isZero()) {
        throw new IllegalArgumentException("timeout must be a positive non-zero duration");
      }
    }
  }

  public StartWorkflowOptions() {
    this(null, null, null, null, null);
  }

  public StartWorkflowOptions(String workflowId) {
    this(workflowId, null, null, null, null);
  }

  public StartWorkflowOptions withWorkflowId(String workflowId) {
    return new StartWorkflowOptions(
        workflowId, this.timeout, this.queueName, this.deduplicationId, this.priority);
  }

  public StartWorkflowOptions withTimeout(Timeout timeout) {
    return new StartWorkflowOptions(
        this.workflowId, timeout, this.queueName, this.deduplicationId, this.priority);
  }

  public StartWorkflowOptions withTimeout(Duration timeout) {
    return withTimeout(Timeout.of(timeout));
  }

  public StartWorkflowOptions withTimeout(long value, TimeUnit unit) {
    return withTimeout(Duration.ofNanos(unit.toNanos(value)));
  }

  public StartWorkflowOptions withNoTimeout() {
    return new StartWorkflowOptions(
        this.workflowId, Timeout.none(), this.queueName, this.deduplicationId, this.priority);
  }

  public StartWorkflowOptions withQueue(String queue) {
    return new StartWorkflowOptions(
        this.workflowId, this.timeout, queue, this.deduplicationId, this.priority);
  }

  public StartWorkflowOptions withQueue(Queue queue) {
    return withQueue(queue.name());
  }

  public StartWorkflowOptions withDeduplicationId(String deduplicationId) {
    return new StartWorkflowOptions(
        this.workflowId, this.timeout, this.queueName, deduplicationId, this.priority);
  }

  public StartWorkflowOptions withPriority(Integer priority) {
    return new StartWorkflowOptions(
        this.workflowId, this.timeout, this.queueName, this.deduplicationId, priority);
  }

  @Override
  public String workflowId() {
    return workflowId != null && workflowId.isEmpty() ? null : workflowId;
  }
}
