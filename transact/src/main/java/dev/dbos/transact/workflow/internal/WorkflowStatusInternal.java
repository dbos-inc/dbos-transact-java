package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.workflow.WorkflowState;

public record WorkflowStatusInternal(
    String workflowId,
    WorkflowState status,
    String name,
    String className,
    String instanceName,
    String queueName,
    String deduplicationId,
    Integer priority,
    String queuePartitionKey,
    String authenticatedUser,
    String assumedRole,
    String authenticatedRoles,
    String inputs,
    String output,
    String error,
    String executorId,
    String appVersion,
    String appId,
    Long createdAt,
    Long updatedAt,
    Long recoveryAttempts,
    Long startedAt,
    Long timeoutMs,
    Long deadlineEpochMs,
    String parentWorkflowId) {

  public WorkflowStatusInternal() {
    this(
        null, null, null, null, null, null, null, null, null, null, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null, null);
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
    private String name;
    private String className;
    private String instanceName;
    private String queueName;
    private String deduplicationId;
    private Integer priority;
    private String queuePartitionKey;
    private String authenticatedUser;
    private String assumedRole;
    private String authenticatedRoles;
    private String inputs;
    private String output;
    private String error;
    private String executorId;
    private String appVersion;
    private String appId;
    private Long createdAt;
    private Long updatedAt;
    private Long recoveryAttempts;
    private Long startedAt;
    private Long timeoutMs;
    private Long deadlineEpochMs;

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

    public Builder name(String name) {
      this.name = name;
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

    public Builder authenticatedRoles(String authenticatedRoles) {
      this.authenticatedRoles = authenticatedRoles;
      return this;
    }

    public Builder inputs(String inputs) {
      this.inputs = inputs;
      return this;
    }

    public Builder output(String output) {
      this.output = output;
      return this;
    }

    public Builder error(String error) {
      this.error = error;
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

    public Builder createdAt(Long createdAt) {
      this.createdAt = createdAt;
      return this;
    }

    public Builder updatedAt(Long updatedAt) {
      this.updatedAt = updatedAt;
      return this;
    }

    public Builder recoveryAttempts(Long recoveryAttempts) {
      this.recoveryAttempts = recoveryAttempts;
      return this;
    }

    public Builder startedAt(Long startedAt) {
      this.startedAt = startedAt;
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

    public WorkflowStatusInternal build() {
      return new WorkflowStatusInternal(
          workflowId,
          status,
          name,
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
          output,
          error,
          executorId,
          appVersion,
          appId,
          createdAt,
          updatedAt,
          recoveryAttempts,
          startedAt,
          timeoutMs,
          deadlineEpochMs,
          parentWorkflowId);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(String workflowId, WorkflowState status) {
    return new Builder().workflowId(workflowId).status(status);
  }
}
