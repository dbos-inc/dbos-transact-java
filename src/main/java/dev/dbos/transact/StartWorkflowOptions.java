package dev.dbos.transact;

import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Options for starting a workflow, including: Assigning the workflow idempotency ID Enqueuing, with
 * options Setting a timeout
 */
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

  /** Construct with default options */
  public StartWorkflowOptions() {
    this(null, null, null, null, null);
  }

  /** Construct with a specified workflow ID */
  public StartWorkflowOptions(String workflowId) {
    this(workflowId, null, null, null, null);
  }

  /** Produces a new StartWorkflowOptions that overrides the ID assigned to the started workflow */
  public StartWorkflowOptions withWorkflowId(String workflowId) {
    return new StartWorkflowOptions(
        workflowId, this.timeout, this.queueName, this.deduplicationId, this.priority);
  }

  /** Produces a new StartWorkflowOptions that overrides timeout value for the started workflow */
  public StartWorkflowOptions withTimeout(Timeout timeout) {
    return new StartWorkflowOptions(
        this.workflowId, timeout, this.queueName, this.deduplicationId, this.priority);
  }

  /** Produces a new StartWorkflowOptions that overrides timeout value for the started workflow */
  public StartWorkflowOptions withTimeout(Duration timeout) {
    return withTimeout(Timeout.of(timeout));
  }

  /** Produces a new StartWorkflowOptions that overrides timeout value for the started workflow */
  public StartWorkflowOptions withTimeout(long value, TimeUnit unit) {
    return withTimeout(Duration.ofNanos(unit.toNanos(value)));
  }

  /** Produces a new StartWorkflowOptions that removes the timeout behavior */
  public StartWorkflowOptions withNoTimeout() {
    return new StartWorkflowOptions(
        this.workflowId, Timeout.none(), this.queueName, this.deduplicationId, this.priority);
  }

  /** Produces a new StartWorkflowOptions that assigns the started workflow to a queue */
  public StartWorkflowOptions withQueue(String queue) {
    return new StartWorkflowOptions(
        this.workflowId, this.timeout, queue, this.deduplicationId, this.priority);
  }

  /** Produces a new StartWorkflowOptions that assigns the started workflow to a queue */
  public StartWorkflowOptions withQueue(Queue queue) {
    return withQueue(queue.name());
  }

  /**
   * Produces a new StartWorkflowOptions that assigns a queue deduplication ID.
   *  Note that the queue must also be specified.
   */
  public StartWorkflowOptions withDeduplicationId(String deduplicationId) {
    return new StartWorkflowOptions(
        this.workflowId, this.timeout, this.queueName, deduplicationId, this.priority);
  }

  /**
   * Produces a new StartWorkflowOptions that assigns a queue priority.
   *  Note that the queue must also be specified and have prioritization enabled
   */
  public StartWorkflowOptions withPriority(Integer priority) {
    return new StartWorkflowOptions(
        this.workflowId, this.timeout, this.queueName, this.deduplicationId, priority);
  }

  /** Get the assigned workflow ID, replacing empty with null */
  @Override
  public String workflowId() {
    return workflowId != null && workflowId.isEmpty() ? null : workflowId;
  }
}
