package dev.dbos.transact.database.dao;

import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.ErrorResult;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.internal.StepResult;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StepsDAO {

  private StepsDAO() {}

  private static final Logger logger = LoggerFactory.getLogger(StepsDAO.class);

  static void recordStepResult(DbContext ctx, StepResult result, long startTimeEpochMs)
      throws SQLException {
    recordStepResult(ctx, result, startTimeEpochMs, System.currentTimeMillis());
  }

  public static void recordStepResult(
      DbContext ctx, StepResult result, long startTimeEpochMs, long endTimeEpochMs)
      throws SQLException {
    try (var conn = ctx.getConnection()) {
      recordStepResult(conn, ctx.schema(), result, startTimeEpochMs, endTimeEpochMs);
    }
    DebugTriggers.debugTriggerPoint(DebugTriggers.DEBUG_TRIGGER_STEP_COMMIT);
  }

  static void recordStepResult(
      Connection conn, String schema, StepResult result, long startTimeEpochMs)
      throws SQLException {
    recordStepResult(conn, schema, result, startTimeEpochMs, System.currentTimeMillis());
  }

  static void recordStepResult(
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

  static StepResult checkStepResult(
      DbContext ctx, String workflowId, int functionId, String functionName) throws SQLException {
    try (var conn = ctx.getConnection()) {
      return checkStepResult(conn, ctx.schema(), workflowId, functionId, functionName);
    }
  }

  public static StepResult checkStepResult(
      Connection conn, String schema, String workflowId, int stepId, String stepName)
      throws SQLException {

    WorkflowDAO.checkWorkflow(conn, schema, workflowId);

    String sql =
        """
          SELECT output, error, function_name, serialization
          FROM "%s".operation_outputs
          WHERE workflow_uuid = ? AND function_id = ?
        """
            .formatted(schema);

    StepResult result = null;
    try (var pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, workflowId);
      pstmt.setInt(2, stepId);
      try (ResultSet rs = pstmt.executeQuery()) {
        if (rs.next()) { // Check if any operation output row exists
          String output = rs.getString("output");
          String error = rs.getString("error");
          String _stepName = rs.getString("function_name");
          String serialization = rs.getString("serialization");
          result =
              new StepResult(workflowId, stepId, _stepName, output, error, null, serialization);
        }
      }
    }

    if (result == null) {
      return null;
    }

    if (!Objects.equals(stepName, result.stepName())) {
      throw new DBOSUnexpectedStepException(workflowId, stepId, stepName, result.stepName());
    }

    return result;
  }

  public static List<StepInfo> listWorkflowSteps(
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

  public static void sleep(DbContext ctx, String workflowUuid, int functionId, Duration duration)
      throws SQLException {
    var endTime = durableSleepEndTime(ctx, workflowUuid, functionId, duration);
    var sleepDuration = Duration.between(Instant.now(), endTime);
    logger.debug("Sleeping until {} (duration {})", endTime, sleepDuration);
    if (sleepDuration.isNegative() || sleepDuration.isZero()) {
      return;
    }
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

  public static boolean patch(DbContext ctx, String workflowId, int functionId, String patchName)
      throws SQLException {
    Objects.requireNonNull(patchName, "patchName cannot be null");
    try (var conn = ctx.getConnection()) {
      var checkpointName = getCheckpointName(conn, ctx.schema(), workflowId, functionId);
      if (checkpointName == null) {
        var output = new StepResult(workflowId, functionId, patchName, null, null, null, null);
        recordStepResult(conn, ctx.schema(), output, System.currentTimeMillis(), null);
        return true;
      } else {
        return patchName.equals(checkpointName);
      }
    }
  }

  public static boolean deprecatePatch(
      DbContext ctx, String workflowId, int functionId, String patchName) throws SQLException {
    Objects.requireNonNull(patchName, "patchName cannot be null");
    try (var conn = ctx.getConnection()) {
      var checkpointName = getCheckpointName(conn, ctx.schema(), workflowId, functionId);
      return patchName.equals(checkpointName);
    }
  }

  static Instant durableSleepEndTime(
      DbContext ctx, String workflowUuid, int functionId, Duration duration) throws SQLException {

    DBOSSerializer serializer = ctx.serializer();
    Objects.requireNonNull(ctx.schema());
    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.sleep";

    StepResult recordedOutput;

    try (var conn = ctx.getConnection()) {
      recordedOutput = checkStepResult(conn, ctx.schema(), workflowUuid, functionId, functionName);
    }

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
        return Instant.ofEpochMilli(durationLong);
      } else {
        throw new IllegalStateException("Recorded sleep timeout is not a number: " + deserialized);
      }
    } else {
      logger.debug(
          "Running sleep, workflow {}, id: {}, duration: {}", workflowUuid, functionId, duration);

      var endTime = System.currentTimeMillis() + duration.toMillis();
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
        recordStepResult(ctx, output, startTime, (long) endTime);
      } catch (DBOSWorkflowExecutionConflictException e) {
        logger.error("Error recording sleep", e);
      }
      return Instant.ofEpochMilli(endTime);
    }
  }
}
