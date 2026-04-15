package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.util.Objects;

public class WorkflowsOutput {
  public String WorkflowUUID;

  // Note, remaining fields are optional
  public String Status;
  public String WorkflowName;
  public String WorkflowClassName;
  public String WorkflowConfigName;
  public String AuthenticatedUser;
  public String AssumedRole;
  public String AuthenticatedRoles;
  public String Input;
  public String Output;
  public String Request;
  public String Error;
  public String CreatedAt;
  public String UpdatedAt;
  public String QueueName;
  public String ApplicationVersion;
  public String ExecutorID;
  public String WorkflowTimeoutMS;
  public String WorkflowDeadlineEpochMS;
  public String DeduplicationID;
  public String Priority;
  public String QueuePartitionKey;
  public String ForkedFrom;
  public String ParentWorkflowID;
  public String DequeuedAt;
  public Boolean WasForkedFrom;
  public String DelayUntilEpochMS;

  public WorkflowsOutput(WorkflowStatus status) {
    Object[] input = status.input();
    Object output = status.output();
    String[] authenticatedRoles = status.authenticatedRoles();

    this.WorkflowUUID = status.workflowId();
    this.Status = status.status().name();
    this.WorkflowName = status.workflowName();
    this.WorkflowClassName = status.className();
    this.WorkflowConfigName = status.instanceName();
    this.AuthenticatedUser = status.authenticatedUser();
    this.AssumedRole = status.assumedRole();
    this.AuthenticatedRoles =
        authenticatedRoles != null && authenticatedRoles.length > 0
            ? JSONUtil.serializeArray(authenticatedRoles)
            : null;
    this.Input = input != null ? JSONUtil.toJson(input) : null;
    this.Output = output != null ? JSONUtil.toJson(output) : null;
    this.Request = null; // not used in Java TX
    this.Error =
        status.error() != null
            ? String.format("%s: %s", status.error().className(), status.error().message())
            : null;
    this.CreatedAt = status.createdAt() == null ? null : String.valueOf(status.createdAtEpochMs());
    this.UpdatedAt = status.updatedAt() == null ? null : String.valueOf(status.updatedAtEpochMs());
    this.QueueName = status.queueName();
    this.ApplicationVersion = status.appVersion();
    this.ExecutorID = status.executorId();
    this.WorkflowTimeoutMS = status.timeout() == null ? null : String.valueOf(status.timeoutMs());
    this.WorkflowDeadlineEpochMS =
        status.deadline() == null ? null : String.valueOf(status.deadlineEpochMs());
    this.DeduplicationID = status.deduplicationId();
    this.Priority = Objects.requireNonNullElse(status.priority(), 0).toString();
    this.QueuePartitionKey = status.queuePartitionKey();
    this.ForkedFrom = status.forkedFrom();
    this.ParentWorkflowID = status.parentWorkflowId();
    this.DequeuedAt = status.startedAt() == null ? null : String.valueOf(status.startedAtEpochMs());
    this.WasForkedFrom = status.wasForkedFrom();
    this.DelayUntilEpochMS =
        status.delayUntil() == null ? null : String.valueOf(status.delayUntilEpochMs());
  }
}
