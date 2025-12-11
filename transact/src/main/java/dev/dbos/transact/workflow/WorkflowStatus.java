package dev.dbos.transact.workflow;

import java.time.Duration;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonProperty;

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
    Integer priority,
    String queuePartitionKey,
    String forkedFrom) {

  @com.fasterxml.jackson.annotation.JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public Instant getDeadline() {
    if (deadlineEpochMs != null) {
      return Instant.ofEpochMilli(deadlineEpochMs);
    }
    return null;
  }

  @com.fasterxml.jackson.annotation.JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public Duration getTimeout() {
    if (timeoutMs != null) {
      return Duration.ofMillis(timeoutMs);
    }

    return null;
  }
}
