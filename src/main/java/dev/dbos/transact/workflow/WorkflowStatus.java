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
    String executorId,
    Long createdAt,
    Long updatedAt,
    String appVersion,
    String appId,
    Integer recoveryAttempts,
    String queueName,
    Long timeoutMs,
    Long deadlineEpochMs,
    Long startedAtEpochMs,
    String deduplicationId,
    Integer priority) {

 
  public Instant getDeadline() {
    if (deadlineEpochMs != null) {
      return Instant.ofEpochMilli(deadlineEpochMs);
    }
    return null;
  }

  public Duration getTimeout() {
    if (timeoutMs != null) {
      return Duration.ofMillis(timeoutMs);
    }

    return null;
  }
}
