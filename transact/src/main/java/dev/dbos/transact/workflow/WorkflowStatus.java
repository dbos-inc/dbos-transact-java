package dev.dbos.transact.workflow;

import java.time.Duration;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents the status and metadata of a workflow execution. Contains information such as workflow
 * identifiers, state, timing, user context, and execution details.
 */
public record WorkflowStatus(
    /** Unique identifier for the workflow instance. */
    String workflowId,
    /** Current state of the workflow. */
    WorkflowState status,
    /** Name of the workflow. */
    String workflowName,
    /** Class name of the workflow implementation. */
    String className,
    /** Instance name of the workflow. */
    String instanceName,
    /** Authenticated user who initiated the workflow. */
    String authenticatedUser,
    /** Assumed role for the workflow execution. */
    String assumedRole,
    /** Roles authenticated for the workflow. */
    String[] authenticatedRoles,
    /** Input arguments to the workflow. */
    Object[] input,
    /** Output/result of the workflow execution. */
    Object output,
    /** Error result if the workflow failed. */
    ErrorResult error,
    /** Identifier of the executor handling the workflow. */
    String executorId,
    /** Epoch time (ms) when the workflow was created. */
    Long createdAt,
    /** Epoch time (ms) when the workflow was last updated. */
    Long updatedAt,
    /** Application version. */
    String appVersion,
    /** Application identifier. */
    String appId,
    /** Number of recovery attempts made. */
    Integer recoveryAttempts,
    /** Name of the queue the workflow is assigned to. */
    String queueName,
    /** Timeout in milliseconds for the workflow execution. */
    Long timeoutMs,
    /** Deadline as epoch milliseconds for the workflow. */
    Long deadlineEpochMs,
    /** Epoch time (ms) when the workflow started. */
    Long startedAtEpochMs,
    /** Deduplication identifier for the workflow. */
    String deduplicationId,
    /** Priority of the workflow in the queue. */
    Integer priority,
    /** Partition key for the queue. */
    String queuePartitionKey,
    /** Workflow ID from which this workflow was forked. */
    String forkedFrom,
    /** Parent workflow ID if this is a sub-workflow. */
    String parentWorkflowId,
    /** Serialized representation of the workflow. */
    String serialization) {

  /**
   * Returns the workflow deadline as an {@link Instant}, if set.
   *
   * @return the deadline as Instant, or null if not set
   */
  @com.fasterxml.jackson.annotation.JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public Instant deadline() {
    if (deadlineEpochMs != null) {
      return Instant.ofEpochMilli(deadlineEpochMs);
    }
    return null;
  }

  /**
   * Returns the workflow timeout as a {@link Duration}, if set.
   *
   * @return the timeout as Duration, or null if not set
   */
  @com.fasterxml.jackson.annotation.JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public Duration timeout() {
    if (timeoutMs != null) {
      return Duration.ofMillis(timeoutMs);
    }
    return null;
  }

  /**
   * Checks equality based on all fields of the WorkflowStatus record.
   *
   * @param obj the object to compare
   * @return true if all fields are equal, false otherwise
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;

    WorkflowStatus that = (WorkflowStatus) obj;

    return java.util.Objects.equals(workflowId, that.workflowId)
        && java.util.Objects.equals(status, that.status)
        && java.util.Objects.equals(workflowName, that.workflowName)
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

  /**
   * Computes the hash code based on all fields of the WorkflowStatus record.
   *
   * @return the hash code
   */
  @Override
  public int hashCode() {
    return java.util.Objects.hash(
        workflowId,
        status,
        workflowName,
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
