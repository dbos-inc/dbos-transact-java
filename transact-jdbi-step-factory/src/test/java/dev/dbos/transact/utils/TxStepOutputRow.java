package dev.dbos.transact.utils;

import java.sql.ResultSet;
import java.sql.SQLException;

public record TxStepOutputRow(
    String workflowId,
    int stepId,
    String output,
    String error,
    String serialization,
    Long createdAt) {

  public TxStepOutputRow(ResultSet rs) throws SQLException {
    this(
        rs.getString("workflow_id"),
        rs.getInt("step_id"),
        rs.getString("output"),
        rs.getString("error"),
        rs.getString("serialization"),
        rs.getObject("created_at", Long.class));
  }
}
