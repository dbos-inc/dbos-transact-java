package dev.dbos.transact.database;

import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.ErrorResult;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StepsDAO {

  private static final Logger logger = LoggerFactory.getLogger(StepsDAO.class);

  private final DataSource dataSource;
  private final String schema;
  private final DBOSSerializer serializer;

  StepsDAO(DataSource ds, String schema, DBOSSerializer serializer) {
    this.dataSource = ds;
    this.schema = Objects.requireNonNull(schema);
    this.serializer = serializer;
  }

  static void recordStepResultTxn(
      DataSource dataSource,
      StepResult result,
      long startTimeEpochMs,
      long endTimeEpochMs,
      String schema)
      throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      recordStepResultTxn(result, startTimeEpochMs, endTimeEpochMs, connection, schema);
    }
    DebugTriggers.debugTriggerPoint(DebugTriggers.DEBUG_TRIGGER_STEP_COMMIT);
  }

  static void recordStepResultTxn(
      StepResult result,
      Long startTimeEpochMs,
      Long endTimeEpochMs,
      Connection connection,
      String schema)
      throws SQLException {

    Objects.requireNonNull(schema);
    String sql =
        """
          INSERT INTO "%s".operation_outputs
            (workflow_uuid, function_id, function_name, output, error, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT DO NOTHING RETURNING completed_at_epoch_ms
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
      pstmt.setObject(8, endTimeEpochMs);

      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next() && endTimeEpochMs != null) {
          long completedAt = rs.getLong("completed_at_epoch_ms");
          if (completedAt != endTimeEpochMs) {
            logger.warn(
                String.format(
                    "Step output for %s:%d-%s was already recorded",
                    result.workflowId(), result.stepId(), result.functionName()));
            throw new DBOSWorkflowExecutionConflictException(result.workflowId());
          }
        }
      }
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
  static StepResult checkStepExecutionTxn(
      String workflowId, int functionId, String functionName, Connection connection, String schema)
      throws SQLException, DBOSWorkflowCancelledException, DBOSUnexpectedStepException {

    Objects.requireNonNull(schema);
    final String sql =
        """
          SELECT status FROM "%s".workflow_status WHERE workflow_uuid = ?
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
          SELECT output, error, function_name, serialization
          FROM "%s".operation_outputs
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
          String serialization = rs.getString("serialization");
          recordedResult =
              new StepResult(
                  workflowId, functionId, recordedFunctionName, output, error, null, serialization);
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

  List<StepInfo> listWorkflowSteps(String workflowId) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      return listWorkflowSteps(connection, workflowId);
    }
  }

  List<StepInfo> listWorkflowSteps(Connection connection, String workflowId) throws SQLException {

    final String sql =
        """
          SELECT function_id, function_name, output, error, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization
          FROM "%s".operation_outputs
          WHERE workflow_uuid = ?
          ORDER BY function_id;
        """
            .formatted(this.schema);

    List<StepInfo> steps = new ArrayList<>();

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {

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
          String serialization = rs.getString("serialization");

          // Deserialize output if present
          Object outputVal = null;
          if (outputData != null) {
            try {
              outputVal =
                  SerializationUtil.deserializeValue(outputData, serialization, this.serializer);
            } catch (Exception e) {
              throw new RuntimeException(
                  "Failed to deserialize output for function " + functionId, e);
            }
          }

          // Deserialize error if present
          ErrorResult stepError =
              ErrorResult.deserialize(errorData, serialization, this.serializer);
          steps.add(
              new StepInfo(
                  functionId,
                  functionName,
                  outputVal,
                  stepError,
                  childWorkflowId,
                  startedAt,
                  completedAt,
                  serialization));
        }
      }
    }

    return steps;
  }

  void sleep(String workflowUuid, int functionId, Duration duration) throws SQLException {
    var sleepDuration =
        StepsDAO.durableSleepDuration(
            dataSource, workflowUuid, functionId, duration, this.schema, this.serializer);
    logger.debug("Sleeping for duration {}", sleepDuration);
    try {
      Thread.sleep(sleepDuration.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Sleep was interrupted for workflow " + workflowUuid, e);
    }
  }

  static Duration durableSleepDuration(
      DataSource dataSource,
      String workflowUuid,
      int functionId,
      Duration duration,
      String schema,
      DBOSSerializer serializer)
      throws SQLException {

    Objects.requireNonNull(schema);
    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.sleep";

    StepResult recordedOutput;

    try (Connection connection = dataSource.getConnection()) {
      recordedOutput =
          checkStepExecutionTxn(workflowUuid, functionId, functionName, connection, schema);
    }

    long endTime;
    if (recordedOutput != null) {
      logger.debug(
          "Replaying sleep, workflow {}, id: {}, duration: {}", workflowUuid, functionId, duration);
      if (recordedOutput.output() == null) {
        throw new IllegalStateException("No recorded timeout for sleep");
      }
      Object deserialized =
          SerializationUtil.deserializeValue(
              recordedOutput.output(), recordedOutput.serialization(), serializer);
      endTime = ((Number) deserialized).longValue();
    } else {
      logger.debug(
          "Running sleep, workflow {}, id: {}, duration: {}", workflowUuid, functionId, duration);
      endTime = System.currentTimeMillis() + duration.toMillis();

      try {
        StepResult output =
            new StepResult(workflowUuid, functionId, functionName, null, null, null, null)
                .withOutput(JSONUtil.serialize(endTime));
        recordStepResultTxn(dataSource, output, startTime, (long) endTime, schema);
      } catch (DBOSWorkflowExecutionConflictException e) {
        logger.error("Error recording sleep", e);
      }
    }

    var durationms = Math.max(0, endTime - System.currentTimeMillis());
    return Duration.ofMillis(durationms);
  }
}
