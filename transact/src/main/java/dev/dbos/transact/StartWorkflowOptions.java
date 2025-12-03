package dev.dbos.transact;

import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * Options for starting a workflow, including: Assigning the workflow idempotency ID Enqueuing, with
 * options Setting a timeout.
 *
 * @param workflowId The unique identifier for the workflow instance. Used for idempotency and
 *     tracking.
 * @param timeout The timeout configuration specifying how long the workflow may run before
 *     expiring; this is promoted to a deadline at execution time.
 * @param deadline The absolute time by which the workflow must start or complete before being
 *     canceled, if timeout is also set the deadline is derived from the timeout.
 * @param queueName An optional name of the queue to which the workflow should be enqueued for
 *     execution.
 * @param deduplicationId If `queueName` is specified, an optional ID used to prevent duplicate
 *     enqueued workflows.
 * @param priority If `queueName` is specified and refers to a queue with priority enabled, the
 *     priority to assign.
 */
public record StartWorkflowOptions(
    String workflowId,
    Timeout timeout,
    Instant deadline,
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
    this(null, null, null, null, null, null);
  }

  /** Construct with a specified workflow ID */
  public StartWorkflowOptions(String workflowId) {
    this(workflowId, null, null, null, null, null);
  }

  /** Produces a new StartWorkflowOptions that overrides the ID assigned to the started workflow */
  public StartWorkflowOptions withWorkflowId(String workflowId) {
    return new StartWorkflowOptions(
        workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority);
  }

  /** Produces a new StartWorkflowOptions that overrides timeout value for the started workflow */
  public StartWorkflowOptions withTimeout(Timeout timeout) {
    if (timeout != null && this.deadline != null) {
      throw new IllegalArgumentException(
          "should not specify a timeout if the deadline is already set");
    }
    return new StartWorkflowOptions(
        this.workflowId,
        timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority);
  }

  /** Produces a new StartWorkflowOptions that overrides deadline value for the started workflow */
  public StartWorkflowOptions withDeadline(Instant deadline) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        deadline,
        this.queueName,
        this.deduplicationId,
        this.priority);
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
        this.workflowId,
        Timeout.none(),
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority);
  }

  /** Produces a new StartWorkflowOptions that assigns the started workflow to a queue */
  public StartWorkflowOptions withQueue(String queue) {
    return new StartWorkflowOptions(
        this.workflowId, this.timeout, this.deadline, queue, this.deduplicationId, this.priority);
  }

  /** Produces a new StartWorkflowOptions that assigns the started workflow to a queue */
  public StartWorkflowOptions withQueue(Queue queue) {
    return withQueue(queue.name());
  }

  /**
   * Produces a new StartWorkflowOptions that assigns a queue deduplication ID. Note that the queue
   * must also be specified.
   */
  public StartWorkflowOptions withDeduplicationId(String deduplicationId) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        deduplicationId,
        this.priority);
  }

  /**
   * Produces a new StartWorkflowOptions that assigns a queue priority. Note that the queue must
   * also be specified and have prioritization enabled
   */
  public StartWorkflowOptions withPriority(Integer priority) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        priority);
  }

  /** Get the assigned workflow ID, replacing empty with null */
  @Override
  public String workflowId() {
    return workflowId != null && workflowId.isEmpty() ? null : workflowId;
  }

  /** Get timeout duration */
  public Duration getTimeoutDuration() {
    if (timeout instanceof Timeout.Explicit e) {
      return e.value();
    }
    return null;
  }
}
