package dev.dbos.transact.admin;

import dev.dbos.transact.json.DBOSPortableSerializer;
import dev.dbos.transact.workflow.WorkflowStatus;

/**
 * This record object is used only within the admin server to convert to JSON using the admin
 * server's preferred response format.
 */
record WorkflowsOutput(
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
        status.authenticatedRoles() == null
            ? "[]"
            : DBOSPortableSerializer.toJson(status.authenticatedRoles());
    var input = status.input() == null ? "[]" : DBOSPortableSerializer.toJson(status.input());
    var output = status.output() == null ? null : DBOSPortableSerializer.toJson(status.output());
    var error = status.error() == null ? null : DBOSPortableSerializer.toJson(status.error());

    return new WorkflowsOutput(
        status.workflowId(),
        status.status().name(),
        status.workflowName(),
        status.className(),
        status.instanceName(),
        status.authenticatedUser(),
        status.assumedRole(),
        roles,
        input,
        output,
        null,
        error,
        status.createdAt() != null ? String.valueOf(status.createdAtEpochMs()) : null,
        status.updatedAt() != null ? String.valueOf(status.updatedAtEpochMs()) : null,
        status.queueName(),
        status.appVersion(),
        status.executorId());
  }
}
