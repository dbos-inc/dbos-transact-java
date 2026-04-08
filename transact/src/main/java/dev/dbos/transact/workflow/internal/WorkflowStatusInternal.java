package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.workflow.WorkflowState;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

public record WorkflowStatusInternal(
    String workflowId,
    WorkflowState status,
    String workflowName,
    String className,
    String instanceName,
    String queueName,
    String deduplicationId,
    Integer priority,
    String queuePartitionKey,
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
    if (Objects.requireNonNull(workflowId, "workflowId must not be null").isEmpty()) {
      throw new IllegalStateException("workflowId must not be empty");
    }
    Objects.requireNonNull(status, "status must not be null");
  }

  public WorkflowStatusInternal() {
    this(
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null);
  }

  public WorkflowStatusInternal(String workflowUUID, WorkflowState state) {
    this(
        workflowUUID,
        state,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null);
  }

  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public Long timeoutMs() {
    return timeout == null ? null : timeout.toMillis();
  }

  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public Long deadlineMs() {
    return deadline == null ? null : deadline.toEpochMilli();
  }
}
