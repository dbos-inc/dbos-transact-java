package dev.dbos.transact;

import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

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
 * @param queuePartitionKey If `queueName` is specified, an optional partition key used to
 *     distribute workflows across queue partitions for load balancing and ordered processing.
 */
public record StartWorkflowOptions(
    @Nullable String workflowId,
    @Nullable Timeout timeout,
    @Nullable Instant deadline,
    @Nullable String queueName,
    @Nullable String deduplicationId,
    @Nullable Integer priority,
    @Nullable String queuePartitionKey) {

  public StartWorkflowOptions {
    if (timeout instanceof Timeout.Explicit explicit) {
      if (explicit.value().isNegative() || explicit.value().isZero()) {
        throw new IllegalArgumentException(
            "StartWorkflowOptions explicit timeout must be a positive non-zero duration");
      }

      if (deadline != null) {
        throw new IllegalArgumentException(
            "StartWorkflowOptions explicit timeout and deadline cannot both be set");
      }
    }

    if (queuePartitionKey != null && queuePartitionKey.isEmpty()) {
      throw new IllegalArgumentException(
          "EnqueueOptions queuePartitionKey must not be empty if not null");
    }

    if (deduplicationId != null && deduplicationId.isEmpty()) {
      throw new IllegalArgumentException(
          "EnqueueOptions deduplicationId must not be empty if not null");
    }
  }

  /** Construct with default options */
  public StartWorkflowOptions() {
    this(null, null, null, null, null, null, null);
  }

  /** Construct with a specified workflow ID */
  public StartWorkflowOptions(String workflowId) {
    this(workflowId, null, null, null, null, null, null);
  }

  /** Construct with a specified queue */
  public StartWorkflowOptions(@NonNull Queue queue) {
    this(null, null, null, queue.name(), null, null, null);
  }

  /** Produces a new StartWorkflowOptions that overrides the ID assigned to the started workflow */
  public @NonNull StartWorkflowOptions withWorkflowId(@Nullable String workflowId) {
    return new StartWorkflowOptions(
        workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey);
  }

  /** Produces a new StartWorkflowOptions that overrides timeout value for the started workflow */
  public @NonNull StartWorkflowOptions withTimeout(@Nullable Timeout timeout) {
    return new StartWorkflowOptions(
        this.workflowId,
        timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey);
  }

  /** Produces a new StartWorkflowOptions that overrides timeout value for the started workflow */
  public @NonNull StartWorkflowOptions withTimeout(@NonNull Duration timeout) {
    return withTimeout(Timeout.of(timeout));
  }

  /** Produces a new StartWorkflowOptions that overrides timeout value for the started workflow */
  public @NonNull StartWorkflowOptions withTimeout(long value, @NonNull TimeUnit unit) {
    return withTimeout(Duration.ofNanos(unit.toNanos(value)));
  }

  /** Produces a new StartWorkflowOptions that removes the timeout behavior */
  public @NonNull StartWorkflowOptions withNoTimeout() {
    return withTimeout(Timeout.none());
  }

  /** Produces a new StartWorkflowOptions that overrides deadline value for the started workflow */
  public @NonNull StartWorkflowOptions withDeadline(@Nullable Instant deadline) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey);
  }

  /** Produces a new StartWorkflowOptions that assigns the started workflow to a queue */
  public @NonNull StartWorkflowOptions withQueue(@Nullable String queue) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        queue,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey);
  }

  /** Produces a new StartWorkflowOptions that assigns the started workflow to a queue */
  public @NonNull StartWorkflowOptions withQueue(@NonNull Queue queue) {
    return withQueue(queue.name());
  }

  /**
   * Produces a new StartWorkflowOptions that assigns a queue deduplication ID. Note that the queue
   * must also be specified.
   */
  public @NonNull StartWorkflowOptions withDeduplicationId(@Nullable String deduplicationId) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        deduplicationId,
        this.priority,
        this.queuePartitionKey);
  }

  /**
   * Produces a new StartWorkflowOptions that assigns a queue priority. Note that the queue must
   * also be specified and have prioritization enabled
   */
  public @NonNull StartWorkflowOptions withPriority(@Nullable Integer priority) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        priority,
        this.queuePartitionKey);
  }

  /** Produces a new StartWorkflowOptions that assigns a queue partition key */
  public @NonNull StartWorkflowOptions withQueuePartitionKey(@Nullable String queuePartitionKey) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        queuePartitionKey);
  }

  /** Get the assigned workflow ID, replacing empty with null */
  @Override
  public @Nullable String workflowId() {
    return workflowId != null && workflowId.isEmpty() ? null : workflowId;
  }
}
