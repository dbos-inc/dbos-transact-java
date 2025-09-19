package dev.dbos.transact.utils;

import java.sql.ResultSet;
import java.sql.SQLException;

public record WorkflowStatusRow(
    String workflowId,
    String status,
    String name,
    String authenticatedUser,
    String assumedRole,
    String authenticatedRoles,
    String request,
    String output,
    String error,
    String executorId,
    Long createdAt,
    Long updatedAt,
    String applicationVersion,
    String applicationId,
    String className,
    String configName,
    Long recoveryAttempts,
    String queueName,
    Long timeoutMs,
    Long deadlineEpochMs,
    String inputs,
    Long startedAtEpochMs,
    String deduplicationId,
    Integer priority) {

  public WorkflowStatusRow(ResultSet rs) throws SQLException {
    this(
        rs.getString("workflow_uuid"),
        rs.getString("status"),
        rs.getString("name"),
        rs.getString("authenticated_user"),
        rs.getString("assumed_role"),
        rs.getString("authenticated_roles"),
        rs.getString("request"),
        rs.getString("output"),
        rs.getString("error"),
        rs.getString("executor_id"),
        rs.getObject("created_at", Long.class),
        rs.getObject("updated_at", Long.class),
        rs.getString("application_version"),
        rs.getString("application_id"),
        rs.getString("class_name"),
        rs.getString("config_name"),
        rs.getObject("recovery_attempts", Long.class),
        rs.getString("queue_name"),
        rs.getObject("workflow_timeout_ms", Long.class),
        rs.getObject("workflow_deadline_epoch_ms", Long.class),
        rs.getString("inputs"),
        rs.getObject("started_at_epoch_ms", Long.class),
        rs.getString("deduplication_id"),
        rs.getObject("priority", Integer.class));
  }
}
