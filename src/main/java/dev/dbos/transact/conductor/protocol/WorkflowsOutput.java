package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.WorkflowStatus;

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

  public WorkflowsOutput(WorkflowStatus status) {
    Object[] input = status.input();
    Object output = status.output();
    Long createdAt = status.createdAt();
    Long updatedAt = status.updatedAt();
    String[] authenticatedRoles = status.authenticatedRoles();

    this.WorkflowUUID = status.workflowId();
    this.Status = status.status();
    this.WorkflowName = status.name();
    this.WorkflowClassName = status.className();
    this.WorkflowConfigName = status.configName();
    this.AuthenticatedUser = status.authenticatedUser();
    this.AssumedRole = status.assumedRole();
    this.AuthenticatedRoles =
        authenticatedRoles != null && authenticatedRoles.length > 0
            ? JSONUtil.serializeArray(authenticatedRoles)
            : null;
    this.Input = input != null ? JSONUtil.serializeArray(input) : null;
    this.Output = output != null ? JSONUtil.toJson(output) : null;
    this.Request = null; // not used in Java TX
    this.Error =
        status.error() != null
            ? String.format("%s: %s", status.error().className(), status.error().message())
            : null;
    this.CreatedAt = createdAt != null ? Long.toString(createdAt) : null;
    this.UpdatedAt = updatedAt != null ? Long.toString(updatedAt) : null;
    this.QueueName = status.queueName();
    this.ApplicationVersion = status.appVersion();
    this.ExecutorID = status.executorId();
  }
}
