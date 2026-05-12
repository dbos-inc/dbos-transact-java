package dev.dbos.transact.database;

import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.ErrorResult;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
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

  StepsDAO(DataSource dataSource, String schema, DBOSSerializer serializer) {
    this.dataSource = dataSource;
    this.schema = schema;
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
      pstmt.setString(3, result.stepName());

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
                    result.workflowId(), result.stepId(), result.stepName()));
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

  List<StepInfo> listWorkflowSteps(
      String workflowId, Boolean loadOutput, Integer limit, Integer offset) throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      return listWorkflowSteps(connection, workflowId, loadOutput, limit, offset);
    }
  }

  List<StepInfo> listWorkflowSteps(
      Connection connection, String workflowId, Boolean loadOutput, Integer limit, Integer offset)
      throws SQLException {

    StringBuilder sqlBuilder =
        new StringBuilder(
            """
          SELECT function_id, function_name, output, error, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization
          FROM "%s".operation_outputs
          WHERE workflow_uuid = ?
          ORDER BY function_id
        """
                .formatted(this.schema));

    if (limit != null) {
      sqlBuilder.append(" LIMIT ?");
    }
    if (offset != null) {
      sqlBuilder.append(" OFFSET ?");
    }

    final String sql = sqlBuilder.toString();

    List<StepInfo> steps = new ArrayList<>();

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {

      int paramIndex = 1;
      stmt.setString(paramIndex++, workflowId);

      if (limit != null) {
        stmt.setInt(paramIndex++, limit);
      }
      if (offset != null) {
        stmt.setInt(paramIndex++, offset);
      }

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

          Object outputVal = null;
          ErrorResult stepError = null;

          if (Objects.requireNonNullElse(loadOutput, true)) {
            if (outputData != null) {
              try {
                outputVal =
                    SerializationUtil.deserializeValue(outputData, serialization, this.serializer);
              } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to deserialize output for function " + functionId, e);
              }
            }
            stepError = ErrorResult.deserialize(errorData, serialization, this.serializer);
          }
          steps.add(
              new StepInfo(
                  functionId,
                  functionName,
                  outputVal,
                  stepError,
                  childWorkflowId,
                  startedAt == null ? null : Instant.ofEpochMilli(startedAt),
                  completedAt == null ? null : Instant.ofEpochMilli(completedAt),
                  serialization));
        }
      }
    }

    return steps;
  }

  static void sleep(
      DataSource dataSource,
      String workflowUuid,
      int functionId,
      Duration duration,
      String schema,
      DBOSSerializer serializer)
      throws SQLException {
    var sleepDuration =
        durableSleepDuration(dataSource, workflowUuid, functionId, duration, schema, serializer);
    logger.debug("Sleeping for duration {}", sleepDuration);
    try {
      Thread.sleep(sleepDuration.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Sleep was interrupted for workflow " + workflowUuid, e);
    }
  }

  static String getCheckpointName(Connection conn, String workflowId, int functionId, String schema)
      throws SQLException {
    var sql =
        """
          SELECT function_name
          FROM "%s".operation_outputs
          WHERE workflow_uuid = ? AND function_id = ?
        """
            .formatted(schema);

    try (var ps = conn.prepareStatement(sql)) {
      ps.setString(1, workflowId);
      ps.setInt(2, functionId);
      try (var rs = ps.executeQuery()) {
        if (rs.next()) {
          return rs.getString("function_name");
        } else {
          return null;
        }
      }
    }
  }

  static boolean patch(
      DataSource dataSource, String workflowId, int functionId, String patchName, String schema)
      throws SQLException {
    Objects.requireNonNull(patchName, "patchName cannot be null");
    try (Connection conn = dataSource.getConnection()) {
      var checkpointName = getCheckpointName(conn, workflowId, functionId, schema);
      if (checkpointName == null) {
        var output = new StepResult(workflowId, functionId, patchName, null, null, null, null);
        recordStepResultTxn(output, System.currentTimeMillis(), null, conn, schema);
        return true;
      } else {
        return patchName.equals(checkpointName);
      }
    }
  }

  static boolean deprecatePatch(
      DataSource dataSource, String workflowId, int functionId, String patchName, String schema)
      throws SQLException {
    Objects.requireNonNull(patchName, "patchName cannot be null");
    try (Connection conn = dataSource.getConnection()) {
      var checkpointName = getCheckpointName(conn, workflowId, functionId, schema);
      return patchName.equals(checkpointName);
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
      if (deserialized instanceof Long durationLong) {
        endTime = durationLong;
      } else {
        throw new IllegalStateException("Recorded sleep timeout is not a number: " + deserialized);
      }
    } else {
      logger.debug(
          "Running sleep, workflow {}, id: {}, duration: {}", workflowUuid, functionId, duration);
      endTime = System.currentTimeMillis() + duration.toMillis();

      try {
        var serializedValue =
            SerializationUtil.serializeValue(
                endTime, serializer != null ? serializer.name() : null, serializer);
        StepResult output =
            new StepResult(
                workflowUuid,
                functionId,
                functionName,
                serializedValue.serializedValue(),
                null,
                null,
                serializedValue.serialization());
        recordStepResultTxn(dataSource, output, startTime, (long) endTime, schema);
      } catch (DBOSWorkflowExecutionConflictException e) {
        logger.error("Error recording sleep", e);
      }
    }

    var durationms = Math.max(0, endTime - System.currentTimeMillis());
    return Duration.ofMillis(durationms);
  }
}
