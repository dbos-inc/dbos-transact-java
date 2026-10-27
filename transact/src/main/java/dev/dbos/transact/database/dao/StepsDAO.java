package dev.dbos.transact.database.dao;

import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.database.SqlTransaction;
import dev.dbos.transact.database.SystemDatabase;
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

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StepsDAO {

  private StepsDAO() {}

  private static final Logger logger = LoggerFactory.getLogger(StepsDAO.class);

  /** Records a step in a transaction of its own. */
  public static void recordStepResult(
      DbContext ctx, StepResult result, long startTimeEpochMs, long endTimeEpochMs, String ownerXid)
      throws SQLException {
    try (var conn = ctx.getConnection()) {
      SqlTransaction.run(
          conn, c -> recordStepResult(ctx, c, result, startTimeEpochMs, endTimeEpochMs, ownerXid));
    }
    DebugTriggers.debugTriggerPoint(DebugTriggers.DEBUG_TRIGGER_STEP_COMMIT);
  }

  /**
   * Records a step in the caller's transaction, after the writes the step made in it.
   *
   * <p>The step is recorded only while the execution holding {@code ownerXid} still owns the
   * workflow: the owner check comes first, and a failed one throws {@link
   * DBOSWorkflowExecutionConflictException}, rolling back the caller's whole transaction. Once the
   * check passes, no other execution can be writing this workflow's steps, so a different row
   * already at this step was written by this execution itself, and is a {@link
   * DBOSStepNondeterminismException}.
   */
  public static void recordStepResult(
      DbContext ctx,
      Connection conn,
      StepResult result,
      Long startTimeEpochMs,
      Long endTimeEpochMs,
      String ownerXid)
      throws SQLException {

    String schema = Objects.requireNonNull(ctx.schema());
    WorkflowDAO.checkOwner(conn, schema, result.workflowId(), ownerXid);
    // On a conflict, the update applies only when the row already there is the one this call is
    // writing: then it is this call's own, committed by an attempt whose acknowledgement was lost,
    // and RETURNING hands it back. A row that differs leaves the update untouched and RETURNING
    // comes back empty.
    String sql =
        """
          INSERT INTO "%s".operation_outputs AS oo
            (workflow_uuid, function_id, function_name, output, error, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, application_name, serialization)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT (workflow_uuid, function_id)
            DO UPDATE SET completed_at_epoch_ms = oo.completed_at_epoch_ms
            WHERE oo.function_name IS NOT DISTINCT FROM EXCLUDED.function_name
              AND oo.output IS NOT DISTINCT FROM EXCLUDED.output
              AND oo.error IS NOT DISTINCT FROM EXCLUDED.error
              AND oo.child_workflow_id IS NOT DISTINCT FROM EXCLUDED.child_workflow_id
              AND oo.started_at_epoch_ms IS NOT DISTINCT FROM EXCLUDED.started_at_epoch_ms
              AND oo.completed_at_epoch_ms IS NOT DISTINCT FROM EXCLUDED.completed_at_epoch_ms
              AND oo.serialization IS NOT DISTINCT FROM EXCLUDED.serialization
          RETURNING oo.function_id
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
      stmt.setString(9, ctx.appName());
      stmt.setString(10, result.serialization());

      try (ResultSet rs = stmt.executeQuery()) {
        if (!rs.next()) {
          throw new DBOSStepNondeterminismException(result.workflowId(), result.stepId());
        }
      }
    }
  }

  static StepResult checkStepResult(
      DbContext ctx,
      String workflowId,
      int functionId,
      String functionName,
      @Nullable String ownerXid)
      throws SQLException {
    try (var conn = ctx.getConnection()) {
      return checkStepResult(conn, ctx.schema(), workflowId, functionId, functionName, ownerXid);
    }
  }

  /**
   * The step recorded at {@code stepId}, or null if none. Given an {@code ownerXid}, fails with
   * {@link dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException} once that execution
   * no longer owns the workflow, so it stops before running the step.
   */
  public static StepResult checkStepResult(
      Connection conn,
      String schema,
      String workflowId,
      int stepId,
      String stepName,
      @Nullable String ownerXid)
      throws SQLException {

    WorkflowDAO.checkWorkflow(conn, schema, workflowId, ownerXid);

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
          String error = SystemDatabase.errorOrNull(rs.getString("error"));
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

    StringBuilder sqlBuilder = new StringBuilder(workflowStepsSql(schema));

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
          String outputData = rs.getString("output");
          String errorData = SystemDatabase.errorOrNull(rs.getString("error"));
          String serialization = rs.getString("serialization");

          Object outputVal = null;
          ErrorResult stepError = null;

          // As for a workflow's status: the steps are what is wanted, the payloads one field of
          // each. See SerializationUtil.canDeserialize.
          if (Objects.requireNonNullElse(loadOutput, true)
              && SerializationUtil.canDeserialize(serialization, serializer)) {
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
          steps.add(stepInfo(rs, outputVal, stepError));
        }
      }
    }

    return steps;
  }

  /** Every step of a workflow, in step ID order. */
  private static String workflowStepsSql(String schema) {
    return """
          SELECT function_id, function_name, output, error, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization, application_name
          FROM "%s".operation_outputs
          WHERE workflow_uuid = ?
          ORDER BY function_id
        """
        .formatted(schema);
  }

  /** The step on the current row, with the payloads given rather than read from the row. */
  private static StepInfo stepInfo(ResultSet rs, Object output, ErrorResult error)
      throws SQLException {
    Long startedAt = rs.getObject("started_at_epoch_ms", Long.class);
    Long completedAt = rs.getObject("completed_at_epoch_ms", Long.class);
    return new StepInfo(
        rs.getInt("function_id"),
        rs.getString("function_name"),
        output,
        error,
        rs.getString("child_workflow_id"),
        startedAt == null ? null : Instant.ofEpochMilli(startedAt),
        completedAt == null ? null : Instant.ofEpochMilli(completedAt),
        rs.getString("serialization"),
        rs.getString("application_name"));
  }

  public static void sleep(
      DbContext ctx, String workflowUuid, int functionId, Duration duration, String ownerXid)
      throws SQLException {
    var endTime = durableSleepEndTime(ctx, workflowUuid, functionId, duration, ownerXid);
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

  public static boolean patch(
      DbContext ctx, String workflowId, int functionId, String patchName, String ownerXid)
      throws SQLException {
    Objects.requireNonNull(patchName, "patchName cannot be null");
    try (var txConn = ctx.getConnection()) {
      // Reading an existing marker writes nothing, so only a new marker is checked against the
      // owner, in the transaction that records it.
      return SqlTransaction.call(
          txConn,
          conn -> {
            var checkpointName = getCheckpointName(conn, ctx.schema(), workflowId, functionId);
            if (checkpointName != null) {
              return patchName.equals(checkpointName);
            }
            var output = new StepResult(workflowId, functionId, patchName, null, null, null, null);
            recordStepResult(ctx, conn, output, System.currentTimeMillis(), null, ownerXid);
            return true;
          });
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

  /**
   * A durable sleep's wake time, recorded at {@code functionId} so a replay wakes at the same
   * instant. A wake time past the largest instant epoch milliseconds can hold is refused with an
   * {@link IllegalArgumentException}, and the refusal is recorded in its place: whether the sum
   * fits depends on the clock, so a replay throws what the first run recorded rather than deciding
   * again against a later clock.
   */
  static Instant durableSleepEndTime(
      DbContext ctx, String workflowUuid, int functionId, Duration duration, String ownerXid)
      throws SQLException {
    return durableEndTime(ctx, workflowUuid, functionId, duration, ownerXid, true);
  }

  /**
   * A blocking read's deadline, recorded at {@code functionId} as a sleep's wake time is. A
   * deadline too far out to store is one that never elapses, which is what the caller asked for, so
   * it is held at the largest instant rather than refused. See {@link #deadlineEpochMs}.
   */
  static Instant durableDeadline(
      DbContext ctx, String workflowUuid, int functionId, Duration timeout, String ownerXid)
      throws SQLException {
    return durableEndTime(ctx, workflowUuid, functionId, timeout, ownerXid, false);
  }

  /**
   * {@code timeout} from {@code nowEpochMs}, held within the range of epoch milliseconds where the
   * sum would leave it: a deadline too far out to store never elapses.
   */
  static long deadlineEpochMs(long nowEpochMs, Duration timeout) {
    try {
      return Math.addExact(nowEpochMs, timeout.toMillis());
    } catch (ArithmeticException e) {
      return timeout.isNegative() ? Long.MIN_VALUE : Long.MAX_VALUE;
    }
  }

  private static Instant durableEndTime(
      DbContext ctx,
      String workflowUuid,
      int functionId,
      Duration duration,
      String ownerXid,
      boolean isSleep)
      throws SQLException {

    DBOSSerializer serializer = ctx.serializer();
    Objects.requireNonNull(ctx.schema());
    var startTime = System.currentTimeMillis();
    String functionName = "DBOS.sleep";

    StepResult recordedOutput;

    try (var conn = ctx.getConnection()) {
      recordedOutput =
          checkStepResult(conn, ctx.schema(), workflowUuid, functionId, functionName, ownerXid);
    }

    if (recordedOutput != null) {
      logger.debug(
          "Replaying sleep, workflow {}, id: {}, duration: {}", workflowUuid, functionId, duration);
      // Only a sleep records a refusal; a deadline is held at the largest instant instead.
      if (isSleep && recordedOutput.error() != null) {
        var refusal =
            SerializationUtil.deserializeError(
                recordedOutput.error(), recordedOutput.serialization(), serializer);
        if (refusal instanceof RuntimeException runtimeException) {
          throw runtimeException;
        }
        throw new RuntimeException(refusal.getMessage(), refusal);
      }
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

      var now = System.currentTimeMillis();
      long endTime;
      if (isSleep) {
        try {
          endTime = Math.addExact(now, duration.toMillis());
        } catch (ArithmeticException e) {
          // Recording a different wake time would wake the workflow at the wrong time on every
          // replay, so the sleep is refused, and the refusal recorded as the step's outcome.
          var refusal =
              new IllegalArgumentException(
                  "sleep duration must not end past the latest instant 64-bit epoch milliseconds"
                      + " can hold");
          // In the native format whatever serializer is configured, as a child start's refusal is:
          // a replay must rebuild the exact type a workflow catches.
          var serializedError =
              SerializationUtil.serializeError(refusal, SerializationUtil.NATIVE, null);
          recordStepResult(
              ctx,
              new StepResult(
                  workflowUuid,
                  functionId,
                  functionName,
                  null,
                  serializedError.serializedValue(),
                  null,
                  serializedError.serialization()),
              startTime,
              System.currentTimeMillis(),
              ownerXid);
          throw refusal;
        }
      } else {
        endTime = deadlineEpochMs(now, duration);
      }
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
      // A conflict means this execution no longer owns the workflow. It propagates, so this run
      // stops and adopts the workflow's recorded outcome rather than sleeping to a wake time the
      // workflow never recorded.
      // A sleep completes at its wake time. A deadline only bounds a wait, and may be held at the
      // largest instant, so it is stamped with the time it was recorded.
      recordStepResult(ctx, output, startTime, isSleep ? endTime : now, ownerXid);
      return Instant.ofEpochMilli(endTime);
    }
  }
}
