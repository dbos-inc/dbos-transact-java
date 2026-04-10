package dev.dbos.transact.workflow.internal;

import static dev.dbos.transact.internal.Validation.nullableIsEmpty;
import static dev.dbos.transact.internal.Validation.nullableIsNotPositive;

import java.time.Duration;
import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonIgnore;

public record WorkflowStatusInternal(
    String workflowId,
    String workflowName,
    String className,
    String instanceName,
    String queueName,
    String deduplicationId,
    Integer priority,
    String queuePartitionKey,
    Duration delay,
    String authenticatedUser,
    String assumedRole,
    String[] authenticatedRoles,
    String inputs,
    String executorId,
    String appVersion,
    String appId,
    Duration timeout,
    Instant deadline,
    String parentWorkflowId,
    String serialization) {

  public WorkflowStatusInternal {
    if (nullableIsEmpty(workflowId)) {
      throw new IllegalArgumentException("workflowId must not be empty");
    }
    if (nullableIsEmpty(workflowName)) {
      throw new IllegalArgumentException("workflowName must not be empty");
    }
    if (nullableIsEmpty(className)) {
      throw new IllegalArgumentException("className must not be empty");
    }
    if (nullableIsEmpty(instanceName)) {
      throw new IllegalArgumentException("instanceName must not be empty");
    }
    if (nullableIsEmpty(queueName)) {
      throw new IllegalArgumentException("queueName must not be empty");
    }
    if (nullableIsEmpty(deduplicationId)) {
      throw new IllegalArgumentException("deduplicationId must not be empty");
    }
    if (nullableIsEmpty(queuePartitionKey)) {
      throw new IllegalArgumentException("queuePartitionKey must not be empty");
    }
    if (nullableIsNotPositive(delay)) {
      throw new IllegalArgumentException("delay must be a positive non-zero duration");
    }
    if (nullableIsEmpty(authenticatedUser)) {
      throw new IllegalArgumentException("authenticatedUser must not be empty");
    }
    if (nullableIsEmpty(assumedRole)) {
      throw new IllegalArgumentException("assumedRole must not be empty");
    }
    if (nullableIsEmpty(inputs)) {
      throw new IllegalArgumentException("inputs must not be empty");
    }
    if (nullableIsEmpty(appVersion)) {
      throw new IllegalArgumentException("appVersion must not be empty");
    }
    // Note, appId can be empty
    if (nullableIsNotPositive(timeout)) {
      throw new IllegalArgumentException("timeout must be a positive non-zero duration");
    }
    if (nullableIsEmpty(parentWorkflowId)) {
      throw new IllegalArgumentException("parentWorkflowId must not be empty");
    }
    if (nullableIsEmpty(serialization)) {
      throw new IllegalArgumentException("serialization must not be empty");
    }
  }

  @JsonIgnore
  public Long timeoutMs() {
    return timeout == null ? null : timeout.toMillis();
  }

  @JsonIgnore
  public Long delayMs() {
    return delay == null ? null : delay.toMillis();
  }

  @JsonIgnore
  public Long deadlineEpochMs() {
    return deadline == null ? null : deadline.toEpochMilli();
  }
}
