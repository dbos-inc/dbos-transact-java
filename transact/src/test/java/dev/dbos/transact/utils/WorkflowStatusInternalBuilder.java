package dev.dbos.transact.utils;

import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.time.Duration;
import java.time.Instant;

public class WorkflowStatusInternalBuilder {
  private String workflowId;
  private String workflowName;
  private String className;
  private String instanceName;
  private String queueName;
  private String deduplicationId;
  private Integer priority;
  private String queuePartitionKey;
  private Duration delay;
  private String authenticatedUser;
  private String assumedRole;
  private String[] authenticatedRoles;
  private String inputs;
  private String executorId;
  private String appVersion;
  private String appId;
  private Duration timeout;
  private Instant deadline;
  private String parentWorkflowId;
  private String serialization;

  public static WorkflowStatusInternalBuilder create(String workflowId) {
    return new WorkflowStatusInternalBuilder().workflowId(workflowId);
  }

  public WorkflowStatusInternalBuilder workflowId(String workflowId) {
    this.workflowId = workflowId;
    return this;
  }

  public WorkflowStatusInternalBuilder workflowName(String workflowName) {
    this.workflowName = workflowName;
    return this;
  }

  public WorkflowStatusInternalBuilder className(String className) {
    this.className = className;
    return this;
  }

  public WorkflowStatusInternalBuilder instanceName(String instanceName) {
    this.instanceName = instanceName;
    return this;
  }

  public WorkflowStatusInternalBuilder queueName(String queueName) {
    this.queueName = queueName;
    return this;
  }

  public WorkflowStatusInternalBuilder deduplicationId(String deduplicationId) {
    this.deduplicationId = deduplicationId;
    return this;
  }

  public WorkflowStatusInternalBuilder priority(Integer priority) {
    this.priority = priority;
    return this;
  }

  public WorkflowStatusInternalBuilder queuePartitionKey(String queuePartitionKey) {
    this.queuePartitionKey = queuePartitionKey;
    return this;
  }

  public WorkflowStatusInternalBuilder delay(Duration delay) {
    this.delay = delay;
    return this;
  }

  public WorkflowStatusInternalBuilder authenticatedUser(String authenticatedUser) {
    this.authenticatedUser = authenticatedUser;
    return this;
  }

  public WorkflowStatusInternalBuilder assumedRole(String assumedRole) {
    this.assumedRole = assumedRole;
    return this;
  }

  public WorkflowStatusInternalBuilder authenticatedRoles(String[] authenticatedRoles) {
    this.authenticatedRoles = authenticatedRoles;
    return this;
  }

  public WorkflowStatusInternalBuilder inputs(String inputs) {
    this.inputs = inputs;
    return this;
  }

  public WorkflowStatusInternalBuilder executorId(String executorId) {
    this.executorId = executorId;
    return this;
  }

  public WorkflowStatusInternalBuilder appVersion(String appVersion) {
    this.appVersion = appVersion;
    return this;
  }

  public WorkflowStatusInternalBuilder appId(String appId) {
    this.appId = appId;
    return this;
  }

  public WorkflowStatusInternalBuilder timeout(Duration timeout) {
    this.timeout = timeout;
    return this;
  }

  public WorkflowStatusInternalBuilder deadline(Instant deadline) {
    this.deadline = deadline;
    return this;
  }

  public WorkflowStatusInternalBuilder parentWorkflowId(String parentWorkflowId) {
    this.parentWorkflowId = parentWorkflowId;
    return this;
  }

  public WorkflowStatusInternalBuilder serialization(String serialization) {
    this.serialization = serialization;
    return this;
  }

  public WorkflowStatusInternal build() {
    return new WorkflowStatusInternal(
        workflowId,
        workflowName,
        className,
        instanceName,
        queueName,
        deduplicationId,
        priority,
        queuePartitionKey,
        delay,
        authenticatedUser,
        assumedRole,
        authenticatedRoles,
        inputs,
        executorId,
        appVersion,
        appId,
        timeout,
        deadline,
        parentWorkflowId,
        serialization);
  }
}
