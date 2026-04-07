package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.workflow.WorkflowState;

import java.util.Objects;

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
    Long timeoutMs,
    Long deadlineEpochMs,
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

  public static class Builder {
    private String workflowId;
    private String parentWorkflowId;
    private WorkflowState status;
    private String workflowName;
    private String className;
    private String instanceName;
    private String queueName;
    private String deduplicationId;
    private Integer priority;
    private String queuePartitionKey;
    private String authenticatedUser;
    private String assumedRole;
    private String[] authenticatedRoles;
    private String inputs;
    private String executorId;
    private String appVersion;
    private String appId;
    private Long timeoutMs;
    private Long deadlineEpochMs;
    private String serialization;

    public Builder workflowId(String workflowId) {
      this.workflowId = workflowId;
      return this;
    }

    public Builder parentWorkflowId(String parentWorkflowId) {
      this.parentWorkflowId = parentWorkflowId;
      return this;
    }

    public Builder status(WorkflowState status) {
      this.status = status;
      return this;
    }

    public Builder workflowName(String workflowName) {
      this.workflowName = workflowName;
      return this;
    }

    public Builder className(String className) {
      this.className = className;
      return this;
    }

    public Builder instanceName(String instanceName) {
      this.instanceName = instanceName;
      return this;
    }

    public Builder queueName(String queueName) {
      this.queueName = queueName;
      return this;
    }

    public Builder deduplicationId(String deduplicationId) {
      this.deduplicationId = deduplicationId;
      return this;
    }

    public Builder priority(Integer priority) {
      this.priority = priority;
      return this;
    }

    public Builder queuePartitionKey(String queuePartitionKey) {
      this.queuePartitionKey = queuePartitionKey;
      return this;
    }

    public Builder authenticatedUser(String authenticatedUser) {
      this.authenticatedUser = authenticatedUser;
      return this;
    }

    public Builder assumedRole(String assumedRole) {
      this.assumedRole = assumedRole;
      return this;
    }

    public Builder authenticatedRoles(String[] authenticatedRoles) {
      this.authenticatedRoles = authenticatedRoles;
      return this;
    }

    public Builder inputs(String inputs) {
      this.inputs = inputs;
      return this;
    }

    public Builder executorId(String executorId) {
      this.executorId = executorId;
      return this;
    }

    public Builder appVersion(String appVersion) {
      this.appVersion = appVersion;
      return this;
    }

    public Builder appId(String appId) {
      this.appId = appId;
      return this;
    }

    public Builder timeoutMs(Long timeoutMs) {
      this.timeoutMs = timeoutMs;
      return this;
    }

    public Builder deadlineEpochMs(Long deadlineEpochMs) {
      this.deadlineEpochMs = deadlineEpochMs;
      return this;
    }

    public Builder serialization(String serialization) {
      this.serialization = serialization;
      return this;
    }

    public WorkflowStatusInternal build() {
      return new WorkflowStatusInternal(
          workflowId,
          status,
          workflowName,
          className,
          instanceName,
          queueName,
          deduplicationId,
          priority,
          queuePartitionKey,
          authenticatedUser,
          assumedRole,
          authenticatedRoles,
          inputs,
          executorId,
          appVersion,
          appId,
          timeoutMs,
          deadlineEpochMs,
          parentWorkflowId,
          serialization);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(String workflowId, WorkflowState status) {
    return new Builder().workflowId(workflowId).status(status);
  }
}
