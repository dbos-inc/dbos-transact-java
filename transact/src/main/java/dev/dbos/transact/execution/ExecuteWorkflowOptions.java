package dev.dbos.transact.execution;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

public record ExecuteWorkflowOptions(
    String workflowId,
    Duration timeout,
    Instant deadline,
    String queueName,
    String deduplicationId,
    Integer priority) {

  public ExecuteWorkflowOptions {
    if (Objects.requireNonNull(workflowId, "workflowId must not be null").isEmpty()) {
      throw new IllegalArgumentException("workflowId must not be empty");
    }

    if (timeout != null && timeout.isNegative()) {
      throw new IllegalStateException("negative timeout");
    }
  }

  public ExecuteWorkflowOptions(String workflowId, Duration timeout, Instant deadline) {
    this(workflowId, timeout, deadline, null, null, null);
  }

  public long getTimeoutMillis() {
    return Objects.requireNonNullElse(timeout, Duration.ZERO).toMillis();
  }
}
