package dev.dbos.transact.txstep;

import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Optional;

/** Shared SQL DDL and query constants for the {@code tx_step_outputs} table. */
public class TxStepSchema {

  private TxStepSchema() {}

  public static void verifyPostgres(Connection conn) throws SQLException {
    var productName = conn.getMetaData().getDatabaseProductName();
    if (!productName.equalsIgnoreCase("PostgreSQL")) {
      throw new IllegalArgumentException(
          "TxStepFactory requires a PostgreSQL datasource, got: " + productName);
    }
  }

  public static void createTable(Connection conn, String schema) throws SQLException {
    try (var stmt = conn.createStatement()) {
      stmt.addBatch("CREATE SCHEMA IF NOT EXISTS \"%s\"".formatted(schema));
      stmt.addBatch(
          """
          CREATE TABLE IF NOT EXISTS "%1$s".tx_step_outputs (
            workflow_id TEXT NOT NULL,
            step_id INT NOT NULL,
            output TEXT,
            error TEXT,
            serialization TEXT,
            created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())*1000)::bigint,
            PRIMARY KEY (workflow_id, step_id)
          )"""
              .formatted(schema));
      stmt.executeBatch();
    }
  }

  public static String checkSql(String schema) {
    return """
        SELECT output, error, serialization
        FROM "%s".tx_step_outputs
        WHERE workflow_id = ? AND step_id = ?
        """
        .formatted(schema);
  }

  public static String upsertSql(String schema) {
    return """
        INSERT INTO "%s".tx_step_outputs
          (workflow_id, step_id, output, error, serialization)
        VALUES (?, ?, ?, ?, ?)
        """
        .formatted(schema);
  }

  public static Optional<StepResult> readResult(
      ResultSet rs, String workflowId, int stepId, String stepName) throws SQLException {
    if (!rs.next()) return Optional.empty();
    return Optional.of(
        new StepResult(
            workflowId,
            stepId,
            stepName,
            rs.getString("output"),
            rs.getString("error"),
            null,
            rs.getString("serialization")));
  }
}
