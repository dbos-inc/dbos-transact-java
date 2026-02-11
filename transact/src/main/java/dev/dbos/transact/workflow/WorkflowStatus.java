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
    String forkedFrom,
    String parentWorkflowId) {

  @com.fasterxml.jackson.annotation.JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public Instant deadline() {
    if (deadlineEpochMs != null) {
      return Instant.ofEpochMilli(deadlineEpochMs);
    }
    return null;
  }

  @com.fasterxml.jackson.annotation.JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public Duration timeout() {
    if (timeoutMs != null) {
      return Duration.ofMillis(timeoutMs);
    }

    return null;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;

    WorkflowStatus that = (WorkflowStatus) obj;

    return java.util.Objects.equals(workflowId, that.workflowId)
        && java.util.Objects.equals(status, that.status)
        && java.util.Objects.equals(name, that.name)
        && java.util.Objects.equals(className, that.className)
        && java.util.Objects.equals(instanceName, that.instanceName)
        && java.util.Objects.equals(authenticatedUser, that.authenticatedUser)
        && java.util.Objects.equals(assumedRole, that.assumedRole)
        && java.util.Arrays.equals(authenticatedRoles, that.authenticatedRoles)
        && java.util.Arrays.deepEquals(input, that.input)
        && java.util.Objects.equals(output, that.output)
        && java.util.Objects.equals(error, that.error)
        && java.util.Objects.equals(executorId, that.executorId)
        && java.util.Objects.equals(createdAt, that.createdAt)
        && java.util.Objects.equals(updatedAt, that.updatedAt)
        && java.util.Objects.equals(appVersion, that.appVersion)
        && java.util.Objects.equals(appId, that.appId)
        && java.util.Objects.equals(recoveryAttempts, that.recoveryAttempts)
        && java.util.Objects.equals(queueName, that.queueName)
        && java.util.Objects.equals(timeoutMs, that.timeoutMs)
        && java.util.Objects.equals(deadlineEpochMs, that.deadlineEpochMs)
        && java.util.Objects.equals(startedAtEpochMs, that.startedAtEpochMs)
        && java.util.Objects.equals(deduplicationId, that.deduplicationId)
        && java.util.Objects.equals(priority, that.priority)
        && java.util.Objects.equals(queuePartitionKey, that.queuePartitionKey)
        && java.util.Objects.equals(forkedFrom, that.forkedFrom)
        && java.util.Objects.equals(parentWorkflowId, that.parentWorkflowId);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(
        workflowId,
        status,
        name,
        className,
        instanceName,
        authenticatedUser,
        assumedRole,
        java.util.Arrays.hashCode(authenticatedRoles),
        java.util.Arrays.deepHashCode(input),
        output,
        error,
        executorId,
        createdAt,
        updatedAt,
        appVersion,
        appId,
        recoveryAttempts,
        queueName,
        timeoutMs,
        deadlineEpochMs,
        startedAtEpochMs,
        deduplicationId,
        priority,
        queuePartitionKey,
        forkedFrom,
        parentWorkflowId);
  }
}
