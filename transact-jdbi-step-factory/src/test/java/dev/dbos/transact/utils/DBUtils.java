package dev.dbos.transact.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import dev.dbos.transact.database.SystemDatabase;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.sql.DataSource;

public class DBUtils {

  public static List<TxStepOutputRow> getTxStepRows(DataSource ds, String workflowId)
      throws SQLException {
    var sql =
        "SELECT * FROM \"%s\".tx_step_outputs WHERE workflow_id = ? ORDER BY step_id"
            .formatted(SystemDatabase.sanitizeSchema(null));
    try (var conn = ds.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(workflowId));
      try (var rs = stmt.executeQuery()) {
        List<TxStepOutputRow> rows = new ArrayList<>();
        while (rs.next()) {
          rows.add(new TxStepOutputRow(rs));
        }
        return rows;
      }
    }
  }

  public static List<OperationOutputRow> getStepRows(DataSource ds, String workflowId)
      throws SQLException {
    var sql =
        "SELECT * FROM \"%s\".operation_outputs WHERE workflow_uuid = ? ORDER BY function_id"
            .formatted(SystemDatabase.sanitizeSchema(null));
    try (var conn = ds.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        List<OperationOutputRow> rows = new ArrayList<>();
        while (rs.next()) {
          rows.add(new OperationOutputRow(rs));
        }
        return rows;
      }
    }
  }

  public static void setWorkflowState(DataSource ds, String workflowId, String newState)
      throws SQLException {
    String sql =
        "UPDATE dbos.workflow_status SET status = ?, updated_at = ? WHERE workflow_uuid = ?";

    try (var connection = ds.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(sql)) {
      pstmt.setString(1, newState);
      pstmt.setLong(2, Instant.now().toEpochMilli());
      pstmt.setString(3, workflowId);

      int rowsAffected = pstmt.executeUpdate();
      assertEquals(1, rowsAffected);
    }
  }
}
