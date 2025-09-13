package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StepsDAO {

  private static final Logger logger = LoggerFactory.getLogger(StepsDAO.class);
  private HikariDataSource dataSource;

  StepsDAO(HikariDataSource dataSource) {
    this.dataSource = dataSource;
  }

  public static void recordStepResultTxn(HikariDataSource dataSource, StepResult result)
      throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    try (Connection connection = dataSource.getConnection(); ) {
      recordStepResultTxn(result, connection);
    }
  }

  public static void recordStepResultTxn(StepResult result, Connection connection)
      throws SQLException {

    String sql =
        String.format(
            """
                INSERT INTO %s.operation_outputs
                    (workflow_uuid, function_id, function_name, output, error)
                VALUES (?, ?, ?, ?, ?)
                """,
            Constants.DB_SCHEMA);

    try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
      int paramIdx = 1;
      pstmt.setString(paramIdx++, result.getWorkflowId());
      pstmt.setInt(paramIdx++, result.getFunctionId());
      pstmt.setString(paramIdx++, result.getFunctionName());

      if (result.getOutput() != null) {
        pstmt.setString(paramIdx++, result.getOutput());
      } else {
        pstmt.setNull(paramIdx++, Types.LONGVARCHAR);
      }

      if (result.getError() != null) {
        pstmt.setString(paramIdx++, result.getError());
      } else {
        pstmt.setNull(paramIdx++, Types.LONGVARCHAR);
      }

      pstmt.executeUpdate();

    } catch (SQLException e) {
      if ("23505".equals(e.getSQLState())) {
        throw new DBOSWorkflowConflictException(
            result.getWorkflowId(),
            String.format(
                "Workflow %s step %d already exists",
                result.getWorkflowId(), result.getFunctionId()));
      } else {
        throw e;
      }
    }
  }

  /**
   * Checks the execution status and output of a specific operation within a workflow. This method
   * corresponds to Python's '_check_operation_execution_txn'.
   *
   * @param workflowId The UUID of the workflow.
   * @param functionId The ID of the function/operation.
   * @param functionName The expected name of the function/operation.
   * @param connection The active JDBC connection (corresponding to Python's 'conn: sa.Connection').
   * @return A {@link StepResult} object if the operation has completed, otherwise {@code null}.
   * @throws IllegalStateException If the workflow does not exist in the status table.
   * @throws WorkflowCancelledException If the workflow is in a cancelled status.
   * @throws UnexpectedStepException If the recorded function name for the operation does not match
   *     the provided name.
   * @throws SQLException For other database access errors.
   */
  public static StepResult checkStepExecutionTxn(
      String workflowId, int functionId, String functionName, Connection connection)
      throws SQLException,
          IllegalStateException,
          WorkflowCancelledException,
          UnexpectedStepException {

    String workflowStatusSql =
        String.format(
            "SELECT status FROM %s.workflow_status WHERE workflow_uuid = ?", Constants.DB_SCHEMA);

    String workflowStatus = null;
    try (PreparedStatement pstmt = connection.prepareStatement(workflowStatusSql)) {
      pstmt.setString(1, workflowId);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) {
          workflowStatus = rs.getString("status");
        }
      }
    }

    if (workflowStatus == null) {
      throw new IllegalStateException(
          String.format("Error: Workflow %s does not exist", workflowId));
    }

    if (Objects.equals(workflowStatus, WorkflowState.CANCELLED.name())) {
      throw new WorkflowCancelledException(
          String.format("Workflow %s is cancelled. Aborting function.", workflowId));
    }

    String operationOutputSql =
        String.format(
            """
                SELECT output, error, function_name
                FROM %s.operation_outputs
                WHERE workflow_uuid = ? AND function_id = ?
                """,
            Constants.DB_SCHEMA);

    StepResult recordedResult = null;
    String recordedFunctionName = null;

    try (PreparedStatement pstmt = connection.prepareStatement(operationOutputSql)) {
      pstmt.setString(1, workflowId);
      pstmt.setInt(2, functionId);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) { // Check if any operation output row exists
          String output = rs.getString("output");
          String error = rs.getString("error");
          recordedFunctionName = rs.getString("function_name");
          recordedResult =
              new StepResult(workflowId, functionId, recordedFunctionName, output, error);
        }
      }
    }

    if (recordedResult == null) {
      return null;
    }

    if (!Objects.equals(functionName, recordedFunctionName)) {
      throw new UnexpectedStepException(workflowId, functionId, functionName, recordedFunctionName);
    }

    return recordedResult;
  }

  public List<StepInfo> listWorkflowSteps(String workflowId) throws SQLException {

    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    String sqlTemplate =
        """
                SELECT function_id, function_name, output, error, child_workflow_id
                FROM %s.operation_outputs
                WHERE workflow_uuid = ?
                ORDER BY function_id;
                """;
    final String sql = String.format(sqlTemplate, Constants.DB_SCHEMA);
    System.out.println(sql);

    List<StepInfo> steps = new ArrayList<>();

    try (Connection connection = dataSource.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {

      stmt.setString(1, workflowId);

      try (ResultSet rs = stmt.executeQuery()) {

        while (rs.next()) {
          int functionId = rs.getInt("function_id");
          String functionName = rs.getString("function_name");
          String outputData = rs.getString("output");
          String errorData = rs.getString("error");
          String childWorkflowId = rs.getString("child_workflow_id");
          System.out.println(functionId);

          // Deserialize output if present
          Object[] output = null;
          if (outputData != null) {
            try {
              output = JSONUtil.deserializeToArray(outputData);
            } catch (Exception e) {
              throw new RuntimeException(
                  "Failed to deserialize output for function " + functionId, e);
            }
          }

          // Deserialize error if present
          Exception error = null;
          if (errorData != null) {
            try {
              // TODO error = JSONUtil.deserialize(errorData);
              error = new Exception(errorData);
            } catch (Exception e) {
              throw new RuntimeException(
                  "Failed to deserialize error for function " + functionId, e);
            }
          }

          Object outputVal = output != null ? output[0] : null;
          steps.add(new StepInfo(functionId, functionName, outputVal, error, childWorkflowId));
        }
      }
    } catch (SQLException e) {
      throw new SQLException("Failed to retrieve workflow steps for workflow: " + workflowId, e);
    }

    return steps;
  }

  public double sleep(String workflowUuid, int functionId, double seconds, boolean skipSleep)
      throws SQLException {
    return StepsDAO.sleep(dataSource, workflowUuid, functionId, seconds, skipSleep);
  }

  public static double sleep(
      HikariDataSource dataSource,
      String workflowUuid,
      int functionId,
      double seconds,
      boolean skipSleep)
      throws SQLException {

    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    String functionName = "DBOS.sleep";

    StepResult recordedOutput;

    try (Connection connection = dataSource.getConnection()) {
      recordedOutput = checkStepExecutionTxn(workflowUuid, functionId, functionName, connection);
    }

    double endTime;

    if (recordedOutput != null) {
      logger.debug("Replaying sleep, id: {}, seconds: {}", functionId, seconds);
      if (recordedOutput.getOutput() == null) {
        throw new DBOSException(ErrorCode.UNEXPECTED.getCode(), "No recorded timeout for sleep");
      }
      Object[] dser = JSONUtil.deserializeToArray(recordedOutput.getOutput());
      endTime = (Double) dser[0];
    } else {
      logger.debug("Running sleep, id: {}, seconds: {}", functionId, seconds);
      endTime = System.currentTimeMillis() / 1000.0 + seconds;

      try {
        StepResult output = new StepResult();
        output.setWorkflowId(workflowUuid);
        output.setFunctionId(functionId);
        output.setFunctionName(functionName);
        output.setOutput(JSONUtil.serialize(endTime));
        output.setError(null);

        recordStepResultTxn(dataSource, output);
      } catch (DBOSWorkflowConflictException e) {
        logger.error("Error recording sleep", e);
      }
    }

    double currentTime = System.currentTimeMillis() / 1000.0;
    double duration = Math.max(0, endTime - currentTime);

    if (!skipSleep) {
      try {
        logger.debug("Sleeping for duration {}", duration);
        Thread.sleep((long) (duration * 1000));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Sleep interrupted", e);
      }
    }

    return duration;
  }
}
