package dev.dbos.transact.workflow;

import java.time.Duration;
import java.time.Instant;

public record WorkflowStatus(
    String workflowId,
    String status,
    String name,
    String className,
    String instanceName,
    String authenticatedUser,
    String assumedRole,
    String[] authenticatedRoles,
    Object[] input,
    Object output,
    ErrorResult error,
    Long createdAt,
    Long updatedAt,
    String queueName,
    String executorId,
    String appVersion,
    Long workflowTimeoutMs,
    Long workflowDeadlineEpochMs,
    String appId,
    Integer recoveryAttempts) {

  public Instant getDeadline() {
    if (workflowDeadlineEpochMs != null) {
      return Instant.ofEpochMilli(workflowDeadlineEpochMs);
    }
    return null;
  }

  public Duration getTimeout() {
    if (workflowTimeoutMs != null) {
      return Duration.ofMillis(workflowTimeoutMs);
    }

    return null;
  }
}
