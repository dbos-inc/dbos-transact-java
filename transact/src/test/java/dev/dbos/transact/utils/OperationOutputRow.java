package dev.dbos.transact.utils;

import java.sql.ResultSet;
import java.sql.SQLException;

public record OperationOutputRow(
    String workflowId,
    int functionId,
    String output,
    String error,
    String functionName,
    String childWorkflowId,
    Long startedAt,
    Long completedAt) {

  public OperationOutputRow(ResultSet rs) throws SQLException {
    this(
        rs.getString("workflow_uuid"),
        rs.getInt("function_id"),
        rs.getString("output"),
        rs.getString("error"),
        rs.getString("function_name"),
        rs.getString("child_workflow_id"),
        rs.getObject("started_at_epoch_ms", Long.class),
        rs.getObject("completed_at_epoch_ms", Long.class)
    );
  }
}
