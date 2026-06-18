package dev.dbos.transact.workflow;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

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
    List<String> authenticatedRoles,
    /** Input arguments to the workflow. */
    Object[] input,
    /** Output/result of the workflow execution. */
    Object output,
    /** Error result if the workflow failed. */
    ErrorResult error,
    /** Identifier of the executor handling the workflow. */
    String executorId,
    /** When the workflow was created. */
    Instant createdAt,
    /** When the workflow was last updated. */
    Instant updatedAt,
    /** Application version. */
    String appVersion,
    /** Application identifier. */
    String appId,
    /** Number of recovery attempts made. */
    Integer recoveryAttempts,
    /** Name of the queue the workflow is assigned to. */
    String queueName,
    /** Timeout for the workflow execution. */
    Duration timeout,
    /** Deadline for the workflow execution. */
    Instant deadline,
    /** When the workflow started executing. */
    Instant startedAt,
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
    /** Whether another workflow was forked from this one. */
    Boolean wasForkedFrom,
    /** Time until which the workflow is delayed. */
    Instant delayUntil,
    /** When the workflow completed (terminal states only). */
    Instant completedAt,
    /** Serialized representation of the workflow. */
    String serialization,
    /** Custom JSON-serializable key-value attributes attached to the workflow at creation. */
    Map<String, Object> attributes) {

  @JsonIgnore
  public Long timeoutMs() {
    return timeout == null ? null : timeout.toMillis();
  }

  @JsonIgnore
  public Long deadlineEpochMs() {
    return deadline == null ? null : deadline.toEpochMilli();
  }

  @JsonIgnore
  public Long createdAtEpochMs() {
    return createdAt == null ? null : createdAt.toEpochMilli();
  }

  @JsonIgnore
  public Long updatedAtEpochMs() {
    return updatedAt == null ? null : updatedAt.toEpochMilli();
  }

  @JsonIgnore
  public Long startedAtEpochMs() {
    return startedAt == null ? null : startedAt.toEpochMilli();
  }

  @JsonIgnore
  public Long delayUntilEpochMs() {
    return delayUntil == null ? null : delayUntil.toEpochMilli();
  }

  @JsonIgnore
  public Long completedAtEpochMs() {
    return completedAt == null ? null : completedAt.toEpochMilli();
  }

  /**
   * Custom equals required because {@code input} is an {@code Object[]} whose default {@code
   * Objects.equals} falls back to reference equality. {@code authenticatedRoles} is now a {@code
   * List<String>} and uses value-based equality naturally.
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
        && java.util.Objects.equals(authenticatedRoles, that.authenticatedRoles)
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
        && java.util.Objects.equals(timeout, that.timeout)
        && java.util.Objects.equals(deadline, that.deadline)
        && java.util.Objects.equals(startedAt, that.startedAt)
        && java.util.Objects.equals(deduplicationId, that.deduplicationId)
        && java.util.Objects.equals(priority, that.priority)
        && java.util.Objects.equals(queuePartitionKey, that.queuePartitionKey)
        && java.util.Objects.equals(forkedFrom, that.forkedFrom)
        && java.util.Objects.equals(parentWorkflowId, that.parentWorkflowId)
        && java.util.Objects.equals(wasForkedFrom, that.wasForkedFrom)
        && java.util.Objects.equals(delayUntil, that.delayUntil)
        && java.util.Objects.equals(completedAt, that.completedAt)
        && java.util.Objects.equals(serialization, that.serialization)
        && java.util.Objects.equals(attributes, that.attributes);
  }

  /**
   * Custom hashCode required because {@code input} is an {@code Object[]} that needs {@code
   * Arrays.deepHashCode} for value-based hashing.
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
        authenticatedRoles,
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
        timeout,
        deadline,
        startedAt,
        deduplicationId,
        priority,
        queuePartitionKey,
        forkedFrom,
        parentWorkflowId,
        wasForkedFrom,
        delayUntil,
        completedAt,
        serialization,
        attributes);
  }
}
