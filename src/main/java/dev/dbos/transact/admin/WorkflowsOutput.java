package dev.dbos.transact.admin;

import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.WorkflowStatus;

public record WorkflowsOutput(
    String WorkflowUUID,
    String Status,
    String WorkflowName,
    String WorkflowClassName,
    String WorkflowConfigName,
    String AuthenticatedUser,
    String AssumedRole,
    String AuthenticatedRoles,
    String Input,
    String Output,
    String Request,
    String Error,
    String CreatedAt,
    String UpdatedAt,
    String QueueName,
    String ApplicationVersion,
    String ExecutorID) {

  static WorkflowsOutput of(WorkflowStatus status) {

    var roles =
        status.authenticatedRoles() == null ? "[]" : JSONUtil.toJson(status.authenticatedRoles());
    var input = status.input() == null ? "[]" : JSONUtil.toJson(status.input());
    var output = status.output() == null ? null : JSONUtil.toJson(status.output());
    var error = status.error() == null ? null : JSONUtil.toJson(status.error());

    return new WorkflowsOutput(
        status.workflowId(),
        status.status(),
        status.name(),
        status.className(),
        status.instanceName(),
        status.authenticatedUser(),
        status.assumedRole(),
        roles,
        input,
        output,
        null,
        error,
        status.createdAt().toString(),
        status.updatedAt().toString(),
        status.queueName(),
        status.appVersion(),
        status.executorId());
  }
}
