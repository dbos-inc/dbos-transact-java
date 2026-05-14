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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class StepsDAO {

  private StepsDAO() {}

  private static final Logger logger = LoggerFactory.getLogger(StepsDAO.class);

  static void recordStepResultTxn(
      DbContext ctx, StepResult result, long startTimeEpochMs, long endTimeEpochMs)
      throws SQLException {
    try (var conn = ctx.getConnection()) {
      recordStepResultTxn(conn, ctx.schema(), result, startTimeEpochMs, endTimeEpochMs);
    }
    DebugTriggers.debugTriggerPoint(DebugTriggers.DEBUG_TRIGGER_STEP_COMMIT);
  }

  static void recordStepResultTxn(
      Connection conn, String schema, StepResult result, Long startTimeEpochMs, Long endTimeEpochMs)
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

    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, result.workflowId());
      stmt.setInt(2, result.stepId());
      stmt.setString(3, result.stepName());

      if (result.output() != null) {
        stmt.setString(4, result.output());
      } else {
        stmt.setNull(4, Types.LONGVARCHAR);
      }

      if (result.error() != null) {
        stmt.setString(5, result.error());
      } else {
        stmt.setNull(5, Types.LONGVARCHAR);
      }

      if (result.childWorkflowId() != null) {
        stmt.setString(6, result.childWorkflowId());
      } else {
        stmt.setNull(6, Types.VARCHAR);
      }

      stmt.setObject(7, startTimeEpochMs);
      stmt.setObject(8, endTimeEpochMs);

      try (ResultSet rs = stmt.executeQuery()) {
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
      Connection conn, String schema, String workflowId, int functionId, String functionName)
      throws SQLException, DBOSWorkflowCancelledException, DBOSUnexpectedStepException {

    Objects.requireNonNull(schema);
    final String sql =
        """
          SELECT status FROM "%s".workflow_status WHERE workflow_uuid = ?
        """
            .formatted(schema);

    String workflowStatus = null;
    try (var pstmt = conn.prepareStatement(sql)) {
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

    try (var pstmt = conn.prepareStatement(operationOutputSql)) {
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

  static List<StepInfo> listWorkflowSteps(
      DbContext ctx, String workflowId, Boolean loadOutput, Integer limit, Integer offset)
      throws SQLException {
    try (var conn = ctx.getConnection()) {
      return listWorkflowSteps(
          conn, ctx.schema(), ctx.serializer(), workflowId, loadOutput, limit, offset);
    }
  }

  static List<StepInfo> listWorkflowSteps(
      Connection conn,
      String schema,
      DBOSSerializer serializer,
      String workflowId,
      Boolean loadOutput,
      Integer limit,
      Integer offset)
      throws SQLException {

    StringBuilder sqlBuilder =
        new StringBuilder(
            """
          SELECT function_id, function_name, output, error, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization
          FROM "%s".operation_outputs
          WHERE workflow_uuid = ?
          ORDER BY function_id
        """
                .formatted(schema));

    if (limit != null) {
      sqlBuilder.append(" LIMIT ?");
    }
    if (offset != null) {
      sqlBuilder.append(" OFFSET ?");
    }

    final String sql = sqlBuilder.toString();

    List<StepInfo> steps = new ArrayList<>();

    try (var stmt = conn.prepareStatement(sql)) {

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
                    SerializationUtil.deserializeValue(outputData, serialization, serializer);
              } catch (Exception e) {
                throw new RuntimeException(
                    "Failed to deserialize output for function " + functionId, e);
              }
            }
            stepError = ErrorResult.deserialize(errorData, serialization, serializer);
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

  static void sleep(DbContext ctx, String workflowUuid, int functionId, Duration duration)
      throws SQLException {
    var sleepDuration = durableSleepDuration(ctx, workflowUuid, functionId, duration);
    logger.debug("Sleeping for duration {}", sleepDuration);
    try {
      Thread.sleep(sleepDuration.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Sleep was interrupted for workflow " + workflowUuid, e);
    }
  }

  static String getCheckpointName(Connection conn, String schema, String workflowId, int functionId)
      throws SQLException {
    var sql =
        """
          SELECT function_name
          FROM "%s".operation_outputs
          WHERE workflow_uuid = ? AND function_id = ?
        """
            .formatted(schema);

    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setInt(2, functionId);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getString("function_name");
        } else {
          return null;
        }
      }
    }
  }

  static boolean patch(DbContext ctx, String workflowId, int functionId, String patchName)
      throws SQLException {
    Objects.requireNonNull(patchName, "patchName cannot be null");
    try (var conn = ctx.getConnection()) {
      var checkpointName = getCheckpointName(conn, ctx.schema(), workflowId, functionId);
      if (checkpointName == null) {
        var output = new StepResult(workflowId, functionId, patchName, null, null, null, null);
        recordStepResultTxn(conn, ctx.schema(), output, System.currentTimeMillis(), null);
        return true;
      } else {
        return patchName.equals(checkpointName);
      }
    }
  }

  static boolean deprecatePatch(DbContext ctx, String workflowId, int functionId, String patchName)
      throws SQLException {
    Objects.requireNonNull(patchName, "patchName cannot be null");
    try (var conn = ctx.getConnection()) {
      var checkpointName = getCheckpointName(conn, ctx.schema(), workflowId, functionId);
      return patchName.equals(checkpointName);
    }
  }

  static Duration durableSleepDuration(
      DbContext ctx, String workflowUuid, int functionId, Duration duration) throws SQLException {

    DBOSSerializer serializer = ctx.serializer();
    Objects.requireNonNull(ctx.schema());
    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.sleep";

    StepResult recordedOutput;

    try (var conn = ctx.getConnection()) {
      recordedOutput =
          checkStepExecutionTxn(conn, ctx.schema(), workflowUuid, functionId, functionName);
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
        recordStepResultTxn(ctx, output, startTime, (long) endTime);
      } catch (DBOSWorkflowExecutionConflictException e) {
        logger.error("Error recording sleep", e);
      }
    }

    var durationms = Math.max(0, endTime - System.currentTimeMillis());
    return Duration.ofMillis(durationms);
  }
}
