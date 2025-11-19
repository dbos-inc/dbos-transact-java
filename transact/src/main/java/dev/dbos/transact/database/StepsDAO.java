package dev.dbos.transact.database;

import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.ErrorResult;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StepsDAO {

  private static final Logger logger = LoggerFactory.getLogger(StepsDAO.class);

  private final HikariDataSource dataSource;
  private final String schema;

  StepsDAO(HikariDataSource ds, String schema) {
    this.dataSource = ds;
    this.schema = Objects.requireNonNull(schema);
  }

  public static void recordStepResultTxn(
      HikariDataSource dataSource, StepResult result, long startTimeEpochMs, String schema)
      throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    try (Connection connection = dataSource.getConnection(); ) {
      recordStepResultTxn(result, startTimeEpochMs, connection, schema);
    }
  }

  public static void recordStepResultTxn(
      StepResult result, Long startTimeEpochMs, Connection connection, String schema)
      throws SQLException {

    Objects.requireNonNull(schema);
    String sql =
        """
          INSERT INTO %s.operation_outputs
            (workflow_uuid, function_id, function_name, output, error, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
            .formatted(schema);

    try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
      pstmt.setString(1, result.workflowId());
      pstmt.setInt(2, result.stepId());
      pstmt.setString(3, result.functionName());

      if (result.output() != null) {
        pstmt.setString(4, result.output());
      } else {
        pstmt.setNull(4, Types.LONGVARCHAR);
      }

      if (result.error() != null) {
        pstmt.setString(5, result.error());
      } else {
        pstmt.setNull(5, Types.LONGVARCHAR);
      }

      if (result.childWorkflowId() != null) {
        pstmt.setString(6, result.childWorkflowId());
      } else {
        pstmt.setNull(6, Types.VARCHAR);
      }

      pstmt.setObject(7, startTimeEpochMs);
      Long endTime = startTimeEpochMs == null ? null : System.currentTimeMillis();
      pstmt.setObject(8, endTime);

      pstmt.executeUpdate();

    } catch (SQLException e) {
      logger.debug("recordStepResultTxn error", e);
      if ("23505".equals(e.getSQLState())) {
        throw new DBOSWorkflowExecutionConflictException(result.workflowId());
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
   * @throws DBOSNonExistentWorkflowException If the workflow does not exist in the status table.
   * @throws DBOSWorkflowCancelledException If the workflow is in a cancelled status.
   * @throws DBOSUnexpectedStepException If the recorded function name for the operation does not
   *     match the provided name.
   * @throws SQLException For other database access errors.
   */
  public static StepResult checkStepExecutionTxn(
      String workflowId, int functionId, String functionName, Connection connection, String schema)
      throws SQLException, DBOSWorkflowCancelledException, DBOSUnexpectedStepException {

    Objects.requireNonNull(schema);
    final String sql =
        """
          SELECT status FROM %s.workflow_status WHERE workflow_uuid = ?
        """
            .formatted(schema);

    String workflowStatus = null;
    try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
      pstmt.setString(1, workflowId);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) {
          workflowStatus = rs.getString("status");
        }
      }
    }

    if (workflowStatus == null) {
      throw new DBOSNonExistentWorkflowException(workflowId);
    }

    if (Objects.equals(workflowStatus, WorkflowState.CANCELLED.name())) {
      throw new DBOSWorkflowCancelledException(
          String.format("Workflow %s is cancelled. Aborting function.", workflowId));
    }

    String operationOutputSql =
        """
          SELECT output, error, function_name
          FROM %s.operation_outputs
          WHERE workflow_uuid = ? AND function_id = ?
        """
            .formatted(schema);

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
              new StepResult(workflowId, functionId, recordedFunctionName, output, error, null);
        }
      }
    }

    if (recordedResult == null) {
      return null;
    }

    if (!Objects.equals(functionName, recordedFunctionName)) {
      throw new DBOSUnexpectedStepException(
          workflowId, functionId, functionName, recordedFunctionName);
    }

    return recordedResult;
  }

  public List<StepInfo> listWorkflowSteps(String workflowId) throws SQLException {

    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    final String sql =
        """
          SELECT function_id, function_name, output, error, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms
          FROM %s.operation_outputs
          WHERE workflow_uuid = ?
          ORDER BY function_id;
        """
            .formatted(this.schema);

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
          Long startedAt = rs.getObject("started_at_epoch_ms", Long.class);
          Long completedAt = rs.getObject("completed_at_epoch_ms", Long.class);

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
          ErrorResult stepError = null;
          if (errorData != null) {
            Exception error = null;
            try {
              error = (Exception) JSONUtil.deserializeAppException(errorData);
            } catch (Exception e) {
              throw new RuntimeException(
                  "Failed to deserialize error for function " + functionId, e);
            }
            var errorWrapper = JSONUtil.deserializeAppExceptionWrapper(errorData);
            stepError = new ErrorResult(errorWrapper.type, errorWrapper.message, errorData, error);
          }

          Object outputVal = output != null ? output[0] : null;
          steps.add(
              new StepInfo(
                  functionId,
                  functionName,
                  outputVal,
                  stepError,
                  childWorkflowId,
                  startedAt,
                  completedAt));
        }
      }
    }

    return steps;
  }

  public Duration sleep(String workflowUuid, int functionId, Duration duration, boolean skipSleep)
      throws SQLException {
    return StepsDAO.sleep(dataSource, workflowUuid, functionId, duration, skipSleep, this.schema);
  }

  public static Duration sleep(
      HikariDataSource dataSource,
      String workflowUuid,
      int functionId,
      Duration duration,
      boolean skipSleep,
      String schema)
      throws SQLException {

    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    Objects.requireNonNull(schema);
    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.sleep";

    StepResult recordedOutput;

    try (Connection connection = dataSource.getConnection()) {
      recordedOutput =
          checkStepExecutionTxn(workflowUuid, functionId, functionName, connection, schema);
    }

    double endTime;

    if (recordedOutput != null) {
      logger.debug("Replaying sleep, id: {}, millis: {}", functionId, duration.toMillis());
      if (recordedOutput.output() == null) {
        throw new IllegalStateException("No recorded timeout for sleep");
      }
      Object[] dser = JSONUtil.deserializeToArray(recordedOutput.output());
      endTime = (Double) dser[0];
    } else {
      logger.debug("Running sleep, id: {}, millis: {}", functionId, duration.toMillis());
      endTime = System.currentTimeMillis() + duration.toMillis();

      try {
        StepResult output =
            new StepResult(workflowUuid, functionId, functionName)
                .withOutput(JSONUtil.serialize(endTime));
        recordStepResultTxn(dataSource, output, startTime, schema);
      } catch (DBOSWorkflowExecutionConflictException e) {
        logger.error("Error recording sleep", e);
      }
    }

    double currentTime = System.currentTimeMillis();
    double durationms = Math.max(0, endTime - currentTime);

    if (!skipSleep) {
      try {
        logger.debug("Sleeping for duration {}", duration);
        Thread.sleep((long) (durationms));
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Sleep interrupted", e);
      }
    }

    return Duration.ofMillis((long) durationms);
  }
}
