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
    Object[] input = status.getInput();
    Object output = status.getOutput();
    Long createdAt = status.getCreatedAt();
    Long updatedAt = status.getUpdatedAt();
    String[] authenticatedRoles = status.getAuthenticatedRoles();

    this.WorkflowUUID = status.getWorkflowId();
    this.Status = status.getStatus();
    this.WorkflowName = status.getName();
    this.WorkflowClassName = status.getClassName();
    this.WorkflowConfigName = status.getConfigName();
    this.AuthenticatedUser = status.getAuthenticatedUser();
    this.AssumedRole = status.getAssumedRole();
    this.AuthenticatedRoles =
        authenticatedRoles != null && authenticatedRoles.length > 0
            ? JSONUtil.serializeArray(authenticatedRoles)
            : null;
    this.Input = input != null ? JSONUtil.serializeArray(input) : null;
    this.Output = output != null ? JSONUtil.toJson(output) : null;
    this.Request = null; // not used in Java TX
    this.Error = status.getError();
    this.CreatedAt = createdAt != null ? Long.toString(createdAt) : null;
    this.UpdatedAt = updatedAt != null ? Long.toString(updatedAt) : null;
    this.QueueName = status.getQueueName();
    this.ApplicationVersion = status.getAppVersion();
    this.ExecutorID = status.getExecutorId();
  }
}
