package dev.dbos.transact.execution;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** Options for executing a workflow, including identifier, queue, and timeout settings, */
public record ExecuteWorkflowOptions(
    String workflowId,
    Duration timeout,
    Instant deadline,
    String queueName,
    String deduplicationId,
    Integer priority) {

  /**
   * Creates a new {@code ExecuteWorkflowOptions} instance, validating parameters.
   *
   * @param workflowId the unique identifier of the workflow; must not be null or empty
   * @param timeout the maximum allowed duration for workflow execution; may be null
   * @param deadline the absolute time by which the workflow must complete; may be null
   * @param queueName the name of the queue to dispatch the workflow to; may be null
   * @param deduplicationId an optional queue deduplication identifier (may be specified if
   *     queueName is not null)
   * @param priority an optional queue priority (may be specified if queueName is not null)
   */
  public ExecuteWorkflowOptions {
    if (Objects.requireNonNull(workflowId, "workflowId must not be null").isEmpty()) {
      throw new IllegalArgumentException("workflowId must not be empty");
    }

    if (timeout != null && timeout.isNegative()) {
      throw new IllegalStateException("negative timeout");
    }
  }

  /**
   * Creates a new {@code ExecuteWorkflowOptions} instance with most common parameters.
   *
   * @param workflowId the unique identifier of the workflow
   * @param timeout the maximum allowed duration for workflow execution
   * @param deadline the absolute deadline for workflow completion
   */
  public ExecuteWorkflowOptions(String workflowId, Duration timeout, Instant deadline) {
    this(workflowId, timeout, deadline, null, null, null);
  }

  /**
   * Returns the timeout duration in milliseconds, or zero if no timeout is set.
   *
   * @return the timeout value in milliseconds
   */
  public long getTimeoutMillis() {
    return Objects.requireNonNullElse(timeout, Duration.ZERO).toMillis();
  }
}
