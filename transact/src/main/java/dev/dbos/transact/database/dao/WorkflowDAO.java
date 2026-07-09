package dev.dbos.transact.database.dao;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.database.MetricData;
import dev.dbos.transact.database.Result;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.database.WorkflowInitResult;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.exceptions.DBOSConflictingWorkflowException;
import dev.dbos.transact.exceptions.DBOSMaxRecoveryAttemptsExceededException;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.exceptions.DBOSWorkflowCancelledException;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.JsonUtility;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.ErrorResult;
import dev.dbos.transact.workflow.ExportedWorkflow;
import dev.dbos.transact.workflow.ForkFromFailureOptions;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.GetStepAggregatesInput;
import dev.dbos.transact.workflow.GetWorkflowAggregatesInput;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepAggregateRow;
import dev.dbos.transact.workflow.WorkflowAggregateRow;
import dev.dbos.transact.workflow.WorkflowDelay;
import dev.dbos.transact.workflow.WorkflowEvent;
import dev.dbos.transact.workflow.WorkflowEventHistory;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.WorkflowStream;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.type.TypeReference;

public class WorkflowDAO {

  private static final Logger logger = LoggerFactory.getLogger(WorkflowDAO.class);

  // All workflow_status columns except inputs/output/error/serialization, which are loaded
  // conditionally. Add new columns here so both getWorkflowStatus and listWorkflows stay in sync.
  private static final String WORKFLOW_STATUS_COLUMNS =
      """
        workflow_uuid, status,
        name, class_name, config_name,
        queue_name, deduplication_id, priority, queue_partition_key, delay_until_epoch_ms,
        executor_id, application_version, application_id,
        authenticated_user, assumed_role, authenticated_roles,
        created_at, updated_at, completed_at, started_at_epoch_ms,
        recovery_attempts, workflow_timeout_ms, workflow_deadline_epoch_ms,
        forked_from, parent_workflow_id, was_forked_from, attributes, schedule_name
      """;

  private WorkflowDAO() {}

  public static WorkflowInitResult initWorkflowStatus(
      DbContext ctx,
      WorkflowStatusInternal initStatus,
      Integer maxRetries,
      boolean isRecoveryRequest,
      boolean isDequeuedRequest,
      String ownerXid)
      throws SQLException {

    logger.debug("initWorkflowStatus workflowId {}", initStatus.workflowId());

    try (var conn = ctx.getConnection()) {

      boolean shouldCommit = false;

      try {
        conn.setAutoCommit(false);
        conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        InsertWorkflowResult resRow =
            insertWorkflowStatus(
                conn, ctx.schema(), initStatus, ownerXid, isRecoveryRequest || isDequeuedRequest);

        if (!Objects.equals(resRow.workflowName(), initStatus.workflowName())) {
          String msg =
              String.format(
                  "Workflow already exists with a different function name: %s, but the provided function name is: %s",
                  resRow.workflowName(), initStatus.workflowName());
          throw new DBOSConflictingWorkflowException(initStatus.workflowId(), msg);
        } else if (!Objects.equals(resRow.className(), initStatus.className())) {
          String msg =
              String.format(
                  "Workflow already exists with a different class name: %s, but the provided class name is: %s",
                  resRow.className(), initStatus.className());
          throw new DBOSConflictingWorkflowException(initStatus.workflowId(), msg);
        } else if (!Objects.equals(
            resRow.instanceName() != null ? resRow.instanceName() : "",
            initStatus.instanceName() != null ? initStatus.instanceName() : "")) {
          String msg =
              String.format(
                  "Workflow already exists with a different class configuration: %s, but the provided class configuration is: %s",
                  resRow.instanceName(), initStatus.instanceName());
          throw new DBOSConflictingWorkflowException(initStatus.workflowId(), msg);
        }

        var state = resRow.status;

        // If there is an existing DB record and we aren't here to recover it,
        //  leave it be.  Roll back the change to max recovery attempts.
        if (!ownerXid.equals(resRow.ownerXid) && !isRecoveryRequest && !isDequeuedRequest) {
          if (resRow.status == WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED) {
            throw new DBOSMaxRecoveryAttemptsExceededException(initStatus.workflowId(), maxRetries);
          }
          return new WorkflowInitResult(state, resRow.deadline(), false, resRow.serialization());
        }

        // Upsert above already set executor assignment and incremented the recovery attempt
        shouldCommit = true;

        final int attempts = resRow.recoveryAttempts();
        if (maxRetries != null && attempts > maxRetries + 1) {

          var sql =
              """
                UPDATE "%s".workflow_status
                SET status = ?, deduplication_id = NULL, started_at_epoch_ms = NULL, queue_name = NULL
                WHERE workflow_uuid = ? AND status = ?
              """
                  .formatted(ctx.schema());

          try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED.name());
            stmt.setString(2, initStatus.workflowId());
            stmt.setString(3, WorkflowState.PENDING.name());

            stmt.executeUpdate();
          }

          throw new DBOSMaxRecoveryAttemptsExceededException(initStatus.workflowId(), maxRetries);
        }

        return new WorkflowInitResult(state, resRow.deadline(), true, resRow.serialization());

      } finally {
        if (shouldCommit) {
          conn.commit();
        } else {
          conn.rollback();
        }
        DebugTriggers.debugTriggerPoint(DebugTriggers.DEBUG_TRIGGER_INITWF_COMMIT);
      }
    } // end try with resources connection closed
  }

  record InsertWorkflowResult(
      int recoveryAttempts,
      WorkflowState status,
      String workflowName,
      String className,
      String instanceName,
      String queueName,
      Instant deadline,
      String serialization,
      String ownerXid) {}

  /**
   * Insert into the workflow_status table
   *
   * @param status WorkflowStatusInternal holds the data for a workflow_status row
   * @return InsertWorkflowResult some of the column inserted
   * @throws SQLException
   */
  static InsertWorkflowResult insertWorkflowStatus(
      Connection conn,
      String schema,
      WorkflowStatusInternal status,
      String ownerXid,
      boolean incrementAttempts)
      throws SQLException {

    logger.debug("insertWorkflowStatus workflowId {}", status.workflowId());

    String insertSQL =
        """
          INSERT INTO "%s".workflow_status (
            workflow_uuid, status, inputs,
            name, class_name, config_name,
            queue_name, deduplication_id, priority, queue_partition_key, delay_until_epoch_ms,
            authenticated_user, assumed_role, authenticated_roles,
            executor_id, application_version, application_id,
            created_at, updated_at, recovery_attempts,
            workflow_timeout_ms, workflow_deadline_epoch_ms,
            parent_workflow_id, owner_xid, serialization, attributes, schedule_name
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
          ON CONFLICT (workflow_uuid)
            DO UPDATE SET
              recovery_attempts = CASE
                  WHEN workflow_status.status !='ENQUEUED' AND workflow_status.status !='DELAYED'
                  THEN workflow_status.recovery_attempts + ?
                  ELSE workflow_status.recovery_attempts
              END,
              updated_at = EXCLUDED.updated_at,
              executor_id = CASE
                  WHEN EXCLUDED.status != 'ENQUEUED' AND EXCLUDED.status != 'DELAYED'
                  THEN EXCLUDED.executor_id
                  ELSE workflow_status.executor_id
              END
          RETURNING recovery_attempts, status, name, class_name, config_name, queue_name, workflow_deadline_epoch_ms, owner_xid, serialization
        """
            .formatted(schema);

    Objects.requireNonNull(status, "status must not be null");
    Objects.requireNonNull(status.workflowId(), "workflowId must not be null");
    var state =
        status.queueName() == null
            ? WorkflowState.PENDING
            : status.delay() == null ? WorkflowState.ENQUEUED : WorkflowState.DELAYED;
    var recoveryAttempts =
        state == WorkflowState.ENQUEUED || state == WorkflowState.DELAYED ? 0 : 1;

    var authenticatedRolesJson =
        status.authenticatedRoles() != null
            ? JsonUtility.toJson(status.authenticatedRoles())
            : null;
    var attributesJson = attributesToJson(status.attributes());
    try (var stmt = conn.prepareStatement(insertSQL)) {

      var now = System.currentTimeMillis();
      stmt.setString(1, status.workflowId());
      stmt.setString(2, state.name());
      stmt.setString(3, status.inputs());

      stmt.setString(4, status.workflowName());
      stmt.setString(5, status.className());
      stmt.setString(6, status.instanceName());

      stmt.setString(7, status.queueName());
      stmt.setString(8, status.deduplicationId());
      stmt.setInt(9, Objects.requireNonNullElse(status.priority(), 0));
      stmt.setString(10, status.queuePartitionKey());
      stmt.setObject(11, status.delayMs() != null ? now + status.delayMs() : null);

      stmt.setString(12, status.authenticatedUser());
      stmt.setString(13, status.assumedRole());
      stmt.setString(14, authenticatedRolesJson);

      stmt.setString(15, status.executorId());
      stmt.setString(16, status.appVersion());
      stmt.setString(17, status.appId());

      stmt.setLong(18, now); // created_at
      stmt.setLong(19, now); // updated_at
      stmt.setInt(20, recoveryAttempts);

      stmt.setObject(21, status.timeoutMs());
      stmt.setObject(22, status.deadlineEpochMs());
      stmt.setString(23, status.parentWorkflowId());

      stmt.setObject(24, ownerXid);
      stmt.setString(25, status.serialization());
      stmt.setString(26, attributesJson);
      stmt.setString(27, status.scheduleName());
      stmt.setInt(28, incrementAttempts ? 1 : 0);

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          InsertWorkflowResult result =
              new InsertWorkflowResult(
                  rs.getInt("recovery_attempts"),
                  WorkflowState.valueOf(rs.getString("status")),
                  rs.getString("name"),
                  rs.getString("class_name"),
                  rs.getString("config_name"),
                  rs.getString("queue_name"),
                  SystemDatabase.toInstant(rs.getObject("workflow_deadline_epoch_ms", Long.class)),
                  rs.getString("serialization"),
                  rs.getString("owner_xid"));

          return result;
        } else {
          throw new RuntimeException(
              "Attempt to insert workflow " + status.workflowId() + " failed: No rows returned.");
        }

      } catch (SQLException e) {
        if ("23505".equals(e.getSQLState())) {
          throw new DBOSQueueDuplicatedException(
              status.workflowId(),
              status.queueName() != null ? status.queueName() : "",
              status.deduplicationId() != null ? status.deduplicationId() : "");
        }
        // Re-throw other SQL exceptions
        throw e;
      }
    }
  }

  static void updateWorkflowOutcome(
      Connection conn,
      String schema,
      String workflowId,
      WorkflowState status,
      String output,
      String error)
      throws SQLException {

    logger.debug("updateWorkflowOutcome wfid {} status {}", workflowId, status);

    if (status != WorkflowState.SUCCESS
        && status != WorkflowState.ERROR
        && status != WorkflowState.CANCELLED) {
      throw new IllegalArgumentException(
          "updateWorkflowOutcome called with non-terminal status: " + status);
    }

    // Never overwrite a CANCELLED workflow: a workflow cancelled during its final step must not
    // subsequently complete.
    var sql =
        """
          UPDATE "%s".workflow_status
          SET status = ?, output = ?, error = ?, updated_at = ?, completed_at = ?, deduplication_id = NULL
          WHERE workflow_uuid = ? AND status != ?
        """
            .formatted(schema);

    try (var stmt = conn.prepareStatement(sql)) {
      long now = System.currentTimeMillis();
      stmt.setString(1, status.name());
      stmt.setString(2, output);
      stmt.setString(3, error);
      stmt.setLong(4, now);
      stmt.setLong(5, now);
      stmt.setString(6, workflowId);
      stmt.setString(7, WorkflowState.CANCELLED.name());

      if (stmt.executeUpdate() == 0) {
        // The guarded UPDATE matched no rows. Re-read status to check whether the workflow
        // was cancelled; if so, raise so it ends as CANCELLED rather than completing.
        var readSql =
            """
            SELECT status FROM "%s".workflow_status WHERE workflow_uuid = ?
            """
                .formatted(schema);
        try (var readStmt = conn.prepareStatement(readSql)) {
          readStmt.setString(1, workflowId);
          try (var rs = readStmt.executeQuery()) {
            if (rs.next() && WorkflowState.CANCELLED.name().equals(rs.getString(1))) {
              throw new DBOSWorkflowCancelledException(workflowId);
            }
          }
        }
      }
    }
  }

  /**
   * Store the result to workflow_status
   *
   * @param workflowId id of the workflow
   * @param result output serialized as json
   */
  public static void recordWorkflowOutput(DbContext ctx, String workflowId, String result)
      throws SQLException {

    try (var conn = ctx.getConnection()) {
      updateWorkflowOutcome(conn, ctx.schema(), workflowId, WorkflowState.SUCCESS, result, null);
    }
  }

  /**
   * Store the error to workflow_status
   *
   * @param workflowId id of the workflow
   * @param error output serialized as json
   */
  public static void recordWorkflowError(DbContext ctx, String workflowId, String error)
      throws SQLException {

    try (var conn = ctx.getConnection()) {
      updateWorkflowOutcome(conn, ctx.schema(), workflowId, WorkflowState.ERROR, null, error);
    }
  }

  /**
   * Insert a workflow_status row and immediately mark it ERROR, for a workflow that was never
   * actually started. Used when an internal workflow that is responsible for starting a user
   * workflow fails before it can do so: without a status row, any handle awaiting the user workflow
   * would poll {@link #awaitWorkflowResult} forever.
   *
   * @param initStatus metadata for the workflow that will be recorded as failed
   * @param error the error serialized as json
   */
  public static void recordErrorForUnstartedWorkflow(
      DbContext ctx, WorkflowStatusInternal initStatus, String error) throws SQLException {

    // No explicit transaction: the calling debouncer workflow is itself durable, so a crash
    // between these two statements is replayed and retried. ON CONFLICT makes the insert
    // idempotent and the outcome update is safe to repeat.
    try (var conn = ctx.getConnection()) {
      insertWorkflowStatus(conn, ctx.schema(), initStatus, UUID.randomUUID().toString(), false);
      updateWorkflowOutcome(
          conn, ctx.schema(), initStatus.workflowId(), WorkflowState.ERROR, null, error);
    }
  }

  public static String getWorkflowSerialization(DbContext ctx, String workflowId)
      throws SQLException {
    var sql =
        "SELECT serialization FROM \"%s\".workflow_status WHERE workflow_uuid = ?"
            .formatted(ctx.schema());
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getString("serialization");
        }
      }
    }
    return null;
  }

  public static WorkflowStatus getWorkflowStatus(DbContext ctx, String workflowId)
      throws SQLException {

    try (var conn = ctx.getConnection()) {
      return getWorkflowStatus(conn, ctx.schema(), ctx.serializer(), workflowId);
    }
  }

  public static WorkflowStatus getWorkflowStatus(
      Connection conn, String schema, DBOSSerializer serializer, String workflowId)
      throws SQLException {
    if (Objects.requireNonNull(workflowId, "workflowId must not be null").isEmpty()) {
      throw new IllegalArgumentException("workflowId must not be empty");
    }

    var sql =
        ("SELECT " + WORKFLOW_STATUS_COLUMNS + ", inputs, output, error, serialization")
            + " FROM \"%s\".workflow_status WHERE workflow_uuid = ?".formatted(schema);

    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return resultsToWorkflowStatus(rs, true, true, serializer);
        }
      }
    }

    return null;
  }

  public static void checkWorkflow(DbContext ctx, String workflowId) throws SQLException {
    try (var conn = ctx.getConnection()) {
      checkWorkflow(conn, ctx.schema(), workflowId);
    }
  }

  public static void checkWorkflow(Connection conn, String schema, String workflowId)
      throws SQLException {
    var workflowState =
        WorkflowDAO.getWorkflowState(
            conn, Objects.requireNonNull(schema), Objects.requireNonNull(workflowId));
    if (workflowState == null) {
      throw new DBOSNonExistentWorkflowException(workflowId);
    }

    if (workflowState == WorkflowState.CANCELLED) {
      throw new DBOSWorkflowCancelledException(workflowId);
    }
  }

  public static @Nullable WorkflowState getWorkflowState(DbContext ctx, String workflowId)
      throws SQLException {
    try (var conn = ctx.getConnection()) {
      return getWorkflowState(conn, ctx.schema(), workflowId);
    }
  }

  public static @Nullable WorkflowState getWorkflowState(
      Connection conn, String schema, String workflowId) throws SQLException {
    var sql =
        """
        SELECT status FROM "%s".workflow_status WHERE workflow_uuid = ?
        """
            .formatted(Objects.requireNonNull(schema));
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(workflowId));
      try (var rs = stmt.executeQuery()) {
        return rs.next() ? WorkflowState.valueOf(rs.getString("status")) : null;
      }
    }
  }

  /**
   * Look up the workflow_uuid of the currently-enqueued or running workflow with a given
   * (queue_name, deduplication_id) pair. Uses the UNIQUE index on that pair for O(1) lookup.
   * Returns {@code null} if no active workflow with that deduplication id exists.
   */
  public static @Nullable String findWorkflowIdByDeduplicationId(
      DbContext ctx, String queueName, String deduplicationId) throws SQLException {
    var sql =
        """
          SELECT workflow_uuid
            FROM "%s".workflow_status
           WHERE queue_name = ?
             AND deduplication_id = ?
           LIMIT 1
        """
            .formatted(ctx.schema());
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, queueName);
      stmt.setString(2, deduplicationId);
      try (var rs = stmt.executeQuery()) {
        return rs.next() ? rs.getString("workflow_uuid") : null;
      }
    }
  }

  public static void setWorkflowDelay(DbContext ctx, String workflowId, WorkflowDelay delay)
      throws SQLException {
    Objects.requireNonNull(workflowId, "workflowId must not be null");
    Objects.requireNonNull(delay, "delay must not be null");

    Instant resolved = null;
    if (delay instanceof WorkflowDelay.Delay d) {
      resolved = Instant.now().plus(d.delay());
    } else if (delay instanceof WorkflowDelay.DelayUntil du) {
      resolved = du.delayUntil();
    }

    if (resolved == null) {
      throw new IllegalArgumentException("Unexpected WorkflowDelay value");
    }

    var sql =
        """
          UPDATE "%s".workflow_status
             SET delay_until_epoch_ms = ?,
                 updated_at = ?
           WHERE workflow_uuid = ?
             AND status = ?
        """
            .formatted(ctx.schema());
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setLong(1, resolved.toEpochMilli());
      stmt.setLong(2, System.currentTimeMillis());
      stmt.setString(3, workflowId);
      stmt.setString(4, WorkflowState.DELAYED.name());

      stmt.executeUpdate();
    }
  }

  // Normalize an empty attributes map to SQL NULL so "no attributes" has a single representation
  // and an empty map (e.g. from withAttributes(Map.of())) clears rather than recording "{}".
  private static String attributesToJson(Map<String, Object> attributes) {
    return (attributes != null && !attributes.isEmpty()) ? JsonUtility.toJson(attributes) : null;
  }

  public static void updateWorkflowAttributes(
      DbContext ctx, String workflowId, Map<String, Object> attributes) throws SQLException {
    Objects.requireNonNull(workflowId, "workflowId must not be null");

    var attributesJson = attributesToJson(attributes);
    var sql =
        """
          UPDATE "%s".workflow_status
             SET attributes = ?::jsonb,
                 updated_at = ?
           WHERE workflow_uuid = ?
        """
            .formatted(ctx.schema());
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, attributesJson);
      stmt.setLong(2, System.currentTimeMillis());
      stmt.setString(3, workflowId);
      stmt.executeUpdate();
    }
  }

  public static void transitionDelayedWorkflows(DbContext ctx) throws SQLException {
    var sql =
        """
          UPDATE "%s".workflow_status
             SET status = ?
           WHERE status = ?
             AND delay_until_epoch_ms <= ?
        """
            .formatted(ctx.schema());

    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, WorkflowState.ENQUEUED.name());
      stmt.setString(2, WorkflowState.DELAYED.name());
      stmt.setLong(3, System.currentTimeMillis());

      stmt.executeUpdate();
    }
  }

  public static List<WorkflowStatus> listWorkflows(DbContext ctx, ListWorkflowsInput input)
      throws SQLException {

    DBOSSerializer serializer = ctx.serializer();
    if (input == null) {
      input = new ListWorkflowsInput();
    }

    List<WorkflowStatus> workflows = new ArrayList<>();

    StringBuilder sqlBuilder = new StringBuilder();
    List<Object> parameters = new ArrayList<>();

    sqlBuilder.append("SELECT ").append(WORKFLOW_STATUS_COLUMNS);

    var loadInput = input.loadInput() == null || input.loadInput();
    var loadOutput = input.loadOutput() == null || input.loadOutput();
    if (loadInput) {
      sqlBuilder.append(", inputs");
    }
    if (loadOutput) {
      sqlBuilder.append(", output, error");
    }
    if (loadInput || loadOutput) {
      sqlBuilder.append(", serialization");
    }

    sqlBuilder.append(" FROM \"%s\".workflow_status ".formatted(ctx.schema()));

    // --- WHERE Clauses ---
    StringJoiner whereConditions = new StringJoiner(" AND ");

    if (input.workflowName() != null && !input.workflowName().isEmpty()) {
      whereConditions.add("name = ANY(?)");
      parameters.add(input.workflowName());
    }
    if (input.className() != null) {
      whereConditions.add("class_name = ?");
      parameters.add(input.className());
    }
    if (input.instanceName() != null) {
      whereConditions.add("config_name = ?");
      parameters.add(input.instanceName());
    }
    if (input.queueName() != null && !input.queueName().isEmpty()) {
      whereConditions.add("queue_name = ANY(?)");
      parameters.add(input.queueName());
    }
    if (input.queuesOnly() != null && input.queuesOnly()) {
      whereConditions.add("queue_name IS NOT NULL");
      if (input.status() == null || input.status().isEmpty()) {
        whereConditions.add("status IN (?, ?, ?)");
        parameters.add(WorkflowState.ENQUEUED.name());
        parameters.add(WorkflowState.PENDING.name());
        parameters.add(WorkflowState.DELAYED.name());
      }
    }
    if (input.forkedFrom() != null && !input.forkedFrom().isEmpty()) {
      whereConditions.add("forked_from = ANY(?)");
      parameters.add(input.forkedFrom());
    }
    if (input.scheduleName() != null && !input.scheduleName().isEmpty()) {
      whereConditions.add("schedule_name = ANY(?)");
      parameters.add(input.scheduleName());
    }
    if (input.parentWorkflowId() != null && !input.parentWorkflowId().isEmpty()) {
      whereConditions.add("parent_workflow_id = ANY(?)");
      parameters.add(input.parentWorkflowId());
    }
    if (input.wasForkedFrom() != null) {
      if (input.wasForkedFrom()) {
        whereConditions.add("was_forked_from = TRUE");
      } else {
        whereConditions.add("was_forked_from = FALSE");
      }
    }
    if (input.hasParent() != null) {
      if (input.hasParent()) {
        whereConditions.add("parent_workflow_id IS NOT NULL");
      } else {
        whereConditions.add("parent_workflow_id IS NULL");
      }
    }
    if (input.workflowIdPrefix() != null && !input.workflowIdPrefix().isEmpty()) {
      StringJoiner prefixConditions = new StringJoiner(" OR ", "(", ")");
      for (String prefix : input.workflowIdPrefix()) {
        prefixConditions.add("workflow_uuid LIKE ?");
        parameters.add(prefix + "%");
      }
      whereConditions.add(prefixConditions.toString());
    }
    if (input.workflowIds() != null && !input.workflowIds().isEmpty()) {
      whereConditions.add("workflow_uuid = ANY(?)");
      parameters.add(input.workflowIds());
    }
    if (input.authenticatedUser() != null && !input.authenticatedUser().isEmpty()) {
      whereConditions.add("authenticated_user = ANY(?)");
      parameters.add(input.authenticatedUser());
    }
    if (input.startTime() != null) {
      whereConditions.add("created_at >= ?");
      parameters.add(input.startTime().toEpochMilli());
    }
    if (input.endTime() != null) {
      whereConditions.add("created_at <= ?");
      parameters.add(input.endTime().toEpochMilli());
    }
    if (input.completedAfter() != null) {
      whereConditions.add("completed_at >= ?");
      parameters.add(input.completedAfter().toEpochMilli());
    }
    if (input.completedBefore() != null) {
      whereConditions.add("completed_at <= ?");
      parameters.add(input.completedBefore().toEpochMilli());
    }
    if (input.dequeuedAfter() != null) {
      whereConditions.add("started_at_epoch_ms >= ?");
      parameters.add(input.dequeuedAfter().toEpochMilli());
    }
    if (input.dequeuedBefore() != null) {
      whereConditions.add("started_at_epoch_ms <= ?");
      parameters.add(input.dequeuedBefore().toEpochMilli());
    }
    if (input.status() != null && !input.status().isEmpty()) {
      whereConditions.add("status = ANY(?)");
      parameters.add(input.status());
    }
    if (input.applicationVersion() != null && !input.applicationVersion().isEmpty()) {
      whereConditions.add("application_version = ANY(?)");
      parameters.add(input.applicationVersion());
    }
    if (input.executorIds() != null && !input.executorIds().isEmpty()) {
      whereConditions.add("executor_id = ANY(?)");
      parameters.add(input.executorIds());
    }
    if (input.attributes() != null && !input.attributes().isEmpty()) {
      // Containment (@>) is served by the GIN index on the attributes column and matches
      // workflows whose attributes contain all the given key-value pairs.
      whereConditions.add("attributes @> ?::jsonb");
      parameters.add(JsonUtility.toJson(input.attributes()));
    }

    // Only append WHERE keyword if there are actual conditions
    if (whereConditions.length() > 0) {
      sqlBuilder.append(" WHERE ").append(whereConditions.toString());
    }

    // --- ORDER BY Clause ---
    sqlBuilder.append(" ORDER BY created_at ");
    if (Objects.requireNonNullElse(input.sortDesc(), false)) {
      sqlBuilder.append("DESC");
    } else {
      sqlBuilder.append("ASC");
    }

    // --- LIMIT and OFFSET Clauses ---
    if (input.limit() != null) {
      sqlBuilder.append(" LIMIT ?");
      parameters.add(input.limit());
    }
    if (input.offset() != null) {
      sqlBuilder.append(" OFFSET ?");
      parameters.add(input.offset());
    }

    try (Connection connection = ctx.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(sqlBuilder.toString())) {
      List<Array> arrays = new ArrayList<>();
      try {
        for (int i = 0; i < parameters.size(); i++) {
          Object param = parameters.get(i);
          if (param instanceof String v) {
            pstmt.setString(i + 1, v);
          } else if (param instanceof Long v) {
            pstmt.setLong(i + 1, v);
          } else if (param instanceof Integer v) {
            pstmt.setInt(i + 1, v);
          } else if (param instanceof List<?> v) {
            Array sqlArray = connection.createArrayOf("text", v.toArray());
            arrays.add(sqlArray);
            pstmt.setArray(i + 1, sqlArray);
          } else {
            pstmt.setObject(i + 1, param);
          }
        }

        try (ResultSet rs = pstmt.executeQuery()) {
          while (rs.next()) {
            WorkflowStatus info = resultsToWorkflowStatus(rs, loadInput, loadOutput, serializer);
            workflows.add(info);
          }
        }
      } finally {
        for (Array array : arrays) {
          array.free();
        }
      }
    }

    return workflows;
  }

  public static List<WorkflowAggregateRow> getWorkflowAggregates(
      DbContext ctx, GetWorkflowAggregatesInput input) throws SQLException {

    if (input == null) {
      input = new GetWorkflowAggregatesInput();
    }

    // --- GROUP BY dimensions (stable order) ---
    record GroupDim(String name, String expr) {}
    var dims = new ArrayList<GroupDim>();
    if (input.groupByStatus()) dims.add(new GroupDim("status", "status"));
    if (input.groupByName()) dims.add(new GroupDim("name", "name"));
    if (input.groupByQueueName()) dims.add(new GroupDim("queue_name", "queue_name"));
    if (input.groupByExecutorId()) dims.add(new GroupDim("executor_id", "executor_id"));
    if (input.groupByApplicationVersion())
      dims.add(new GroupDim("application_version", "application_version"));
    // Time bucket: floor(created_at / bucket) * bucket
    boolean hasBucket = input.timeBucketSize() != null;
    if (hasBucket) {
      long ms = input.timeBucketSize().toMillis();
      String bucketExpr = "(floor(created_at / %d) * %d)::bigint".formatted(ms, ms);
      dims.add(new GroupDim("time_bucket", bucketExpr));
    }

    if (dims.isEmpty()) {
      throw new IllegalArgumentException(
          "GetWorkflowAggregatesInput requires at least one groupBy* flag set to true"
              + " (e.g. groupByStatus, groupByName, groupByQueueName)");
    }

    // --- SELECT metrics ---
    record Metric(String alias, String expr) {}
    var metrics = new ArrayList<Metric>();
    if (input.selectCount()) metrics.add(new Metric("count", "COUNT(*)"));
    if (input.selectMinCreatedAt()) metrics.add(new Metric("min_created_at", "MIN(created_at)"));
    if (input.selectMaxQueueWait())
      metrics.add(new Metric("max_queue_wait_ms", "MAX(started_at_epoch_ms - created_at)"));
    if (input.selectMaxTotalLatency())
      metrics.add(new Metric("max_total_latency_ms", "MAX(completed_at - created_at)"));

    if (metrics.isEmpty()) {
      throw new IllegalArgumentException(
          "GetWorkflowAggregatesInput requires at least one select* flag set to true"
              + " (e.g. selectCount, selectMinCreatedAt, selectMaxQueueWait)");
    }

    List<Object> parameters = new ArrayList<>();
    StringBuilder sqlBuilder = new StringBuilder("SELECT ");

    StringJoiner selectCols = new StringJoiner(", ");
    for (var dim : dims) selectCols.add(dim.expr() + " AS " + dim.name());
    for (var m : metrics) selectCols.add(m.expr() + " AS " + m.alias());
    sqlBuilder.append(selectCols).append(" FROM \"%s\".workflow_status".formatted(ctx.schema()));

    // --- WHERE ---
    StringJoiner whereConditions = new StringJoiner(" AND ");

    if (input.workflowName() != null && !input.workflowName().isEmpty()) {
      whereConditions.add("name = ANY(?)");
      parameters.add(input.workflowName());
    }
    if (input.status() != null && !input.status().isEmpty()) {
      whereConditions.add("status = ANY(?)");
      parameters.add(input.status());
    }
    if (input.queueName() != null && !input.queueName().isEmpty()) {
      whereConditions.add("queue_name = ANY(?)");
      parameters.add(input.queueName());
    }
    if (input.executorIds() != null && !input.executorIds().isEmpty()) {
      whereConditions.add("executor_id = ANY(?)");
      parameters.add(input.executorIds());
    }
    if (input.applicationVersion() != null && !input.applicationVersion().isEmpty()) {
      whereConditions.add("application_version = ANY(?)");
      parameters.add(input.applicationVersion());
    }
    if (input.startTime() != null) {
      whereConditions.add("created_at >= ?");
      parameters.add(input.startTime().toEpochMilli());
    }
    if (input.endTime() != null) {
      whereConditions.add("created_at <= ?");
      parameters.add(input.endTime().toEpochMilli());
    }
    if (input.completedAfter() != null) {
      whereConditions.add("completed_at >= ?");
      parameters.add(input.completedAfter().toEpochMilli());
    }
    if (input.completedBefore() != null) {
      whereConditions.add("completed_at <= ?");
      parameters.add(input.completedBefore().toEpochMilli());
    }
    if (input.dequeuedAfter() != null) {
      whereConditions.add("started_at_epoch_ms >= ?");
      parameters.add(input.dequeuedAfter().toEpochMilli());
    }
    if (input.dequeuedBefore() != null) {
      whereConditions.add("started_at_epoch_ms <= ?");
      parameters.add(input.dequeuedBefore().toEpochMilli());
    }
    if (input.workflowIdPrefix() != null && !input.workflowIdPrefix().isEmpty()) {
      StringJoiner prefixOr = new StringJoiner(" OR ", "(", ")");
      for (var prefix : input.workflowIdPrefix()) {
        prefixOr.add("workflow_uuid LIKE ?");
        parameters.add(prefix + "%");
      }
      whereConditions.add(prefixOr.toString());
    }
    if (input.attributes() != null && !input.attributes().isEmpty()) {
      // Containment (@>) is served by the GIN index on the attributes column and matches
      // workflows whose attributes contain all the given key-value pairs.
      whereConditions.add("attributes @> ?::jsonb");
      parameters.add(JsonUtility.toJson(input.attributes()));
    }

    if (whereConditions.length() > 0) {
      sqlBuilder.append(" WHERE ").append(whereConditions);
    }

    // --- GROUP BY ---
    StringJoiner groupByCols = new StringJoiner(", ");
    for (var dim : dims) groupByCols.add(dim.expr());
    sqlBuilder.append(" GROUP BY ").append(groupByCols);

    List<WorkflowAggregateRow> results = new ArrayList<>();
    try (Connection connection = ctx.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(sqlBuilder.toString())) {
      List<Array> arrays = new ArrayList<>();
      try {
        for (int i = 0; i < parameters.size(); i++) {
          Object param = parameters.get(i);
          if (param instanceof Long v) {
            pstmt.setLong(i + 1, v);
          } else if (param instanceof List<?> v) {
            Array sqlArray = connection.createArrayOf("text", v.toArray());
            arrays.add(sqlArray);
            pstmt.setArray(i + 1, sqlArray);
          } else {
            pstmt.setObject(i + 1, param);
          }
        }
        try (ResultSet rs = pstmt.executeQuery()) {
          int groupCount = dims.size();
          while (rs.next()) {
            var group = new LinkedHashMap<String, String>();
            for (int i = 0; i < groupCount; i++) {
              String val = rs.getString(dims.get(i).name());
              group.put(dims.get(i).name(), val);
            }
            Long count = null;
            Instant minCreatedAt = null;
            Duration maxQueueWait = null;
            Duration maxTotalLatency = null;
            for (var m : metrics) {
              Object v = rs.getObject(m.alias());
              Long lv = v == null ? null : ((Number) v).longValue();
              switch (m.alias()) {
                case "count" -> count = lv;
                case "min_created_at" ->
                    minCreatedAt = lv != null ? Instant.ofEpochMilli(lv) : null;
                case "max_queue_wait_ms" ->
                    maxQueueWait = lv != null ? Duration.ofMillis(lv) : null;
                case "max_total_latency_ms" ->
                    maxTotalLatency = lv != null ? Duration.ofMillis(lv) : null;
              }
            }
            results.add(
                new WorkflowAggregateRow(
                    group, count, minCreatedAt, maxQueueWait, maxTotalLatency));
          }
        }
      } finally {
        for (Array array : arrays) {
          array.free();
        }
      }
    }

    return results;
  }

  public static List<StepAggregateRow> getStepAggregates(
      DbContext ctx, GetStepAggregatesInput input) throws SQLException {

    if (input == null) {
      input = new GetStepAggregatesInput();
    }

    // Status is derived: error IS NULL → SUCCESS, otherwise ERROR
    String statusExpr = "CASE WHEN error IS NULL THEN 'SUCCESS' ELSE 'ERROR' END";

    // --- GROUP BY dimensions ---
    record GroupDim(String name, String expr) {}
    var dims = new ArrayList<GroupDim>();
    if (input.groupByFunctionName()) dims.add(new GroupDim("function_name", "function_name"));
    if (input.groupByStatus()) dims.add(new GroupDim("status", statusExpr));
    if (input.timeBucketSize() != null) {
      long ms = input.timeBucketSize().toMillis();
      String bucketExpr = "(floor(completed_at_epoch_ms / %d) * %d)::bigint".formatted(ms, ms);
      dims.add(new GroupDim("time_bucket", bucketExpr));
    }

    if (dims.isEmpty()) {
      throw new IllegalArgumentException(
          "GetStepAggregatesInput requires at least one groupBy* flag set to true"
              + " (e.g. groupByFunctionName, groupByStatus)");
    }

    // --- SELECT metrics ---
    record Metric(String alias, String expr) {}
    var metrics = new ArrayList<Metric>();
    if (input.selectCount()) metrics.add(new Metric("count", "COUNT(*)"));
    if (input.selectMaxDuration())
      metrics.add(
          new Metric("max_duration_ms", "MAX(completed_at_epoch_ms - started_at_epoch_ms)"));

    if (metrics.isEmpty()) {
      throw new IllegalArgumentException(
          "GetStepAggregatesInput requires at least one select* flag set to true"
              + " (e.g. selectCount, selectMaxDuration)");
    }

    List<Object> parameters = new ArrayList<>();
    StringBuilder sqlBuilder = new StringBuilder("SELECT ");

    StringJoiner selectCols = new StringJoiner(", ");
    for (var dim : dims) selectCols.add(dim.expr() + " AS " + dim.name());
    for (var m : metrics) selectCols.add(m.expr() + " AS " + m.alias());
    sqlBuilder.append(selectCols).append(" FROM \"%s\".operation_outputs".formatted(ctx.schema()));

    // --- WHERE ---
    StringJoiner whereConditions = new StringJoiner(" AND ");

    if (input.status() != null && !input.status().isEmpty()) {
      // Translate status filter to error IS NULL / IS NOT NULL conditions
      boolean wantSuccess = input.status().contains("SUCCESS");
      boolean wantError = input.status().contains("ERROR");
      if (wantSuccess && !wantError) {
        whereConditions.add("error IS NULL");
      } else if (wantError && !wantSuccess) {
        whereConditions.add("error IS NOT NULL");
      }
      // if both or neither: no filter needed
    }
    if (input.functionName() != null && !input.functionName().isEmpty()) {
      whereConditions.add("function_name = ANY(?)");
      parameters.add(input.functionName());
    }
    if (input.workflowIdPrefix() != null && !input.workflowIdPrefix().isEmpty()) {
      StringJoiner prefixOr = new StringJoiner(" OR ", "(", ")");
      for (var prefix : input.workflowIdPrefix()) {
        prefixOr.add("workflow_uuid LIKE ?");
        parameters.add(prefix + "%");
      }
      whereConditions.add(prefixOr.toString());
    }
    if (input.completedAfter() != null) {
      whereConditions.add("completed_at_epoch_ms >= ?");
      parameters.add(input.completedAfter().toEpochMilli());
    }
    if (input.completedBefore() != null) {
      whereConditions.add("completed_at_epoch_ms <= ?");
      parameters.add(input.completedBefore().toEpochMilli());
    }

    if (whereConditions.length() > 0) {
      sqlBuilder.append(" WHERE ").append(whereConditions);
    }

    // --- GROUP BY ---
    StringJoiner groupByCols = new StringJoiner(", ");
    for (var dim : dims) groupByCols.add(dim.expr());
    sqlBuilder.append(" GROUP BY ").append(groupByCols);

    List<StepAggregateRow> results = new ArrayList<>();
    try (Connection connection = ctx.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(sqlBuilder.toString())) {
      List<Array> arrays = new ArrayList<>();
      try {
        for (int i = 0; i < parameters.size(); i++) {
          Object param = parameters.get(i);
          if (param instanceof Long v) {
            pstmt.setLong(i + 1, v);
          } else if (param instanceof List<?> v) {
            Array sqlArray = connection.createArrayOf("text", v.toArray());
            arrays.add(sqlArray);
            pstmt.setArray(i + 1, sqlArray);
          } else {
            pstmt.setObject(i + 1, param);
          }
        }
        try (ResultSet rs = pstmt.executeQuery()) {
          int groupCount = dims.size();
          while (rs.next()) {
            var group = new LinkedHashMap<String, String>();
            for (int i = 0; i < groupCount; i++) {
              String val = rs.getString(dims.get(i).name());
              group.put(dims.get(i).name(), val);
            }
            Long count = null;
            Duration maxDuration = null;
            for (var m : metrics) {
              Object v = rs.getObject(m.alias());
              Long lv = v == null ? null : ((Number) v).longValue();
              switch (m.alias()) {
                case "count" -> count = lv;
                case "max_duration_ms" -> maxDuration = lv != null ? Duration.ofMillis(lv) : null;
              }
            }
            results.add(new StepAggregateRow(group, count, maxDuration));
          }
        }
      } finally {
        for (Array array : arrays) {
          array.free();
        }
      }
    }

    return results;
  }

  private static WorkflowStatus resultsToWorkflowStatus(
      ResultSet rs, boolean loadInput, boolean loadOutput, DBOSSerializer serializer)
      throws SQLException {
    String authenticatedRolesJson = rs.getString("authenticated_roles");
    String attributesJson = rs.getString("attributes");
    String serializedInput = loadInput ? rs.getString("inputs") : null;
    String serializedOutput = loadOutput ? rs.getString("output") : null;
    String serializedError = loadOutput ? rs.getString("error") : null;
    String serialization = loadInput || loadOutput ? rs.getString("serialization") : null;
    WorkflowStatus info =
        new WorkflowStatus(
            rs.getString("workflow_uuid"),
            WorkflowState.valueOf(rs.getString("status")),
            rs.getString("name"),
            rs.getString("class_name"),
            rs.getString("config_name"),
            rs.getString("authenticated_user"),
            rs.getString("assumed_role"),
            (authenticatedRolesJson != null)
                ? JsonUtility.fromJson(authenticatedRolesJson, new TypeReference<List<String>>() {})
                : null,
            loadInput
                ? SerializationUtil.deserializePositionalArgs(
                    serializedInput, serialization, serializer)
                : null,
            loadOutput
                ? SerializationUtil.deserializeValue(serializedOutput, serialization, serializer)
                : null,
            loadOutput ? ErrorResult.deserialize(serializedError, serialization, serializer) : null,
            rs.getString("executor_id"),
            SystemDatabase.toInstant(rs.getObject("created_at", Long.class)),
            SystemDatabase.toInstant(rs.getObject("updated_at", Long.class)),
            rs.getString("application_version"),
            rs.getString("application_id"),
            rs.getInt("recovery_attempts"),
            rs.getString("queue_name"),
            SystemDatabase.toDuration(rs.getObject("workflow_timeout_ms", Long.class)),
            SystemDatabase.toInstant(rs.getObject("workflow_deadline_epoch_ms", Long.class)),
            SystemDatabase.toInstant(rs.getObject("started_at_epoch_ms", Long.class)),
            rs.getString("deduplication_id"),
            rs.getObject("priority", Integer.class),
            rs.getString("queue_partition_key"),
            rs.getString("forked_from"),
            rs.getString("parent_workflow_id"),
            rs.getObject("was_forked_from", Boolean.class),
            SystemDatabase.toInstant(rs.getObject("delay_until_epoch_ms", Long.class)),
            SystemDatabase.toInstant(rs.getObject("completed_at", Long.class)),
            serialization,
            (attributesJson != null)
                ? JsonUtility.fromJson(attributesJson, new TypeReference<Map<String, Object>>() {})
                : null,
            rs.getString("schedule_name"));
    return info;
  }

  @SuppressWarnings("unchecked")
  public static <T> Result<T> awaitWorkflowResult(
      DbContext ctx, Duration dbPollingInterval, String workflowId) throws SQLException {

    DBOSSerializer serializer = ctx.serializer();
    final String sql =
        """
          SELECT status, output, error, serialization
          FROM "%s".workflow_status
          WHERE workflow_uuid = ?
        """
            .formatted(ctx.schema());

    while (true) {
      ctx.checkClosed();
      try (Connection connection = ctx.getConnection();
          PreparedStatement stmt = connection.prepareStatement(sql)) {

        stmt.setString(1, workflowId);

        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next()) {
            String status = rs.getString("status");
            String serialization = rs.getString("serialization");

            switch (WorkflowState.valueOf(status.toUpperCase())) {
              case SUCCESS -> {
                String output = rs.getString("output");
                Object outputValue =
                    SerializationUtil.deserializeValue(output, serialization, serializer);
                return Result.success((T) outputValue);
              }

              case ERROR -> {
                String error = rs.getString("error");
                Throwable t = SerializationUtil.deserializeError(error, serialization, serializer);
                return Result.failure(t);
              }
              case CANCELLED -> throw new DBOSAwaitedWorkflowCancelledException(workflowId);

              default -> {}
            }
            // Status is PENDING or other - continue polling
          }
          // Row not found - workflow hasn't appeared yet, continue polling
        }
      }

      try {
        Thread.sleep(dbPollingInterval.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Workflow polling interrupted for " + workflowId, e);
      }
    }
  }

  public static void recordChildWorkflow(
      DbContext ctx,
      String parentId,
      String childId, // workflowId of the child
      int functionId, // func id in the parent
      String functionName,
      long startTime)
      throws SQLException {

    var result =
        new StepResult(parentId, functionId, functionName, null, null, null, null)
            .withChildWorkflowId(childId);
    try (var conn = ctx.getConnection()) {
      StepsDAO.recordStepResult(conn, ctx.schema(), result, null, null);
    }
  }

  public static Optional<String> checkChildWorkflow(
      DbContext ctx, String workflowUuid, int functionId) throws SQLException {

    final String sql =
        """
          SELECT child_workflow_id FROM "%s".operation_outputs WHERE workflow_uuid = ? AND function_id = ?
        """
            .formatted(ctx.schema());

    try (Connection connection = ctx.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {

      stmt.setString(1, workflowUuid);
      stmt.setInt(2, functionId);

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          String childWorkflowId = rs.getString("child_workflow_id");
          return childWorkflowId != null ? Optional.of(childWorkflowId) : Optional.empty();
        }
        return Optional.empty();
      }
    }
  }

  private static Collection<String> filterNullsAndBlanks(Collection<String> workflowIds) {
    if (workflowIds == null) {
      return List.of();
    }
    return workflowIds.stream().filter(id -> id != null && !id.isBlank()).toList();
  }

  public static void cancelWorkflows(
      DbContext ctx, List<String> workflowIds, boolean cancelChildren) throws SQLException {
    var roots = filterNullsAndBlanks(workflowIds);
    if (roots.isEmpty()) {
      return;
    }

    if (!cancelChildren) {
      cancelBatch(ctx, roots);
      return;
    }

    // Cancel level-by-level so newly-spawned children are also caught
    var visited = new HashSet<>(roots);
    List<String> frontier = new ArrayList<>(roots);
    while (!frontier.isEmpty()) {
      cancelBatch(ctx, frontier);
      var children = getDirectChildren(ctx, frontier);
      frontier = children.stream().filter(c -> !visited.contains(c)).toList();
      visited.addAll(frontier);
    }
  }

  private static void cancelBatch(DbContext ctx, Collection<String> workflowIds)
      throws SQLException {
    String sql =
        """
          UPDATE "%s".workflow_status
          SET status = ?,
              queue_name = NULL,
              deduplication_id = NULL,
              started_at_epoch_ms = NULL,
              updated_at = ?,
              completed_at = ?
          WHERE workflow_uuid = ANY(?)
            AND status NOT IN (?, ?)
        """
            .formatted(ctx.schema());

    try (Connection conn = ctx.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      Array array = conn.createArrayOf("text", workflowIds.toArray(String[]::new));
      long now = System.currentTimeMillis();
      try {
        stmt.setString(1, WorkflowState.CANCELLED.name());
        stmt.setLong(2, now);
        stmt.setLong(3, now);
        stmt.setArray(4, array);
        stmt.setString(5, WorkflowState.SUCCESS.name());
        stmt.setString(6, WorkflowState.ERROR.name());
        stmt.executeUpdate();
      } finally {
        array.free();
      }
    }
  }

  public static void resumeWorkflows(DbContext ctx, List<String> workflowIds, String queueName)
      throws SQLException {
    var filtered = filterNullsAndBlanks(workflowIds);
    if (filtered.isEmpty()) {
      return;
    }

    String sql =
        """
          UPDATE "%s".workflow_status
          SET status = ?,
              queue_name = ?,
              recovery_attempts = 0,
              workflow_deadline_epoch_ms = NULL,
              deduplication_id = NULL,
              started_at_epoch_ms = NULL,
              completed_at = NULL,
              updated_at = ?
          WHERE workflow_uuid = ANY(?)
            AND status NOT IN (?, ?)
        """
            .formatted(ctx.schema());

    try (Connection conn = ctx.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      Array array = conn.createArrayOf("text", filtered.toArray(String[]::new));
      try {
        stmt.setString(1, WorkflowState.ENQUEUED.name());
        stmt.setString(2, Objects.requireNonNullElse(queueName, Constants.DBOS_INTERNAL_QUEUE));
        stmt.setLong(3, System.currentTimeMillis());
        stmt.setArray(4, array);
        stmt.setString(5, WorkflowState.SUCCESS.name());
        stmt.setString(6, WorkflowState.ERROR.name());
        stmt.executeUpdate();
      } finally {
        array.free();
      }
    }
  }

  public static void deleteWorkflows(
      DbContext ctx, List<String> workflowIds, boolean deleteChildren) throws SQLException {
    var filtered = filterNullsAndBlanks(workflowIds);
    if (filtered.isEmpty()) {
      return;
    }

    var wfIdSet = new HashSet<String>(filtered);
    if (deleteChildren) {
      for (var wfid : filtered) {
        var children = getWorkflowChildren(ctx, wfid);
        wfIdSet.addAll(children);
      }
    }

    var sql =
        """
          DELETE FROM "%s".workflow_status
          WHERE workflow_uuid = ANY(?);
        """
            .formatted(ctx.schema());

    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      var array = conn.createArrayOf("text", wfIdSet.toArray(String[]::new));
      try {
        stmt.setArray(1, array);
        stmt.executeUpdate();
      } finally {
        array.free();
      }
    }
  }

  private static List<String> getDirectChildren(DbContext ctx, Collection<String> workflowIds)
      throws SQLException {
    if (workflowIds.isEmpty()) {
      return List.of();
    }
    var sql =
        """
          SELECT workflow_uuid
          FROM "%s".workflow_status
          WHERE parent_workflow_id = ANY(?)
        """
            .formatted(ctx.schema());

    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      var array = conn.createArrayOf("text", workflowIds.toArray(String[]::new));
      try {
        stmt.setArray(1, array);
        try (var rs = stmt.executeQuery()) {
          var result = new ArrayList<String>();
          while (rs.next()) {
            result.add(rs.getString(1));
          }
          return result;
        }
      } finally {
        array.free();
      }
    }
  }

  public static Set<String> getWorkflowChildren(DbContext ctx, String workflowId)
      throws SQLException {
    var descendants = new HashSet<String>();
    List<String> frontier = List.of(workflowId);
    while (!frontier.isEmpty()) {
      var children = getDirectChildren(ctx, frontier);
      frontier = children.stream().filter(c -> !descendants.contains(c)).toList();
      descendants.addAll(frontier);
    }
    return descendants;
  }

  public static String forkWorkflow(
      DbContext ctx, String workflowId, int startStep, ForkOptions options) throws SQLException {

    options = Objects.requireNonNullElseGet(options, ForkOptions::new);

    String forkedWorkflowId =
        Objects.requireNonNullElseGet(
            options.forkedWorkflowId(), () -> UUID.randomUUID().toString());

    logger.debug("forkWorkflow Original id {} forked id {}", workflowId, forkedWorkflowId);

    forkWorkflows(
        ctx,
        List.of(workflowId),
        List.of(forkedWorkflowId),
        List.of(startStep),
        options.applicationVersion(),
        options.queueName(),
        options.queuePartitionKey(),
        options.timeout());
    return forkedWorkflowId;
  }

  public static List<String> forkFromFailure(
      DbContext ctx, List<String> workflowIds, ForkFromFailureOptions options) throws SQLException {

    Objects.requireNonNull(options, "ForkFromFailureOptions must not be null");

    if (workflowIds.isEmpty()) {
      return List.of();
    }

    var startSteps =
        (options instanceof ForkFromFailureOptions.FromStep fromStep)
            ? workflowIds.stream().map(ignored -> fromStep.step()).collect(Collectors.toList())
            : resolveStartSteps(ctx, workflowIds, options);

    List<String> forkedIds = new ArrayList<>(workflowIds.size());
    for (int i = 0; i < workflowIds.size(); i++) {
      forkedIds.add(UUID.randomUUID().toString());
    }

    forkWorkflows(
        ctx,
        workflowIds,
        forkedIds,
        startSteps,
        options.applicationVersion(),
        options.queueName(),
        options.queuePartitionKey(),
        null);
    return forkedIds;
  }

  private static List<Integer> resolveStartSteps(
      DbContext ctx, List<String> workflowIds, ForkFromFailureOptions options) throws SQLException {

    String sql =
        (options instanceof ForkFromFailureOptions.FromLastFailure
                ? """
                    SELECT workflow_uuid,
                          COALESCE(
                            MAX(function_id) FILTER (WHERE error IS NOT NULL),
                            MAX(function_id)
                          ) AS start_step
                    FROM "%s".operation_outputs
                    WHERE workflow_uuid = ANY(?)
                    GROUP BY workflow_uuid
                  """
                : options instanceof ForkFromFailureOptions.FromLastStep
                    ? """
                        SELECT workflow_uuid, MAX(function_id) AS start_step
                        FROM "%s".operation_outputs
                        WHERE workflow_uuid = ANY(?)
                        GROUP BY workflow_uuid
                      """
                    : """
                        SELECT workflow_uuid, MAX(function_id) AS start_step
                        FROM "%s".operation_outputs
                        WHERE workflow_uuid = ANY(?) AND function_name = ?
                        GROUP BY workflow_uuid
                      """)
            .formatted(ctx.schema());

    Map<String, Integer> startStepByWorkflowId = new HashMap<>();
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      Array array = conn.createArrayOf("text", workflowIds.toArray(String[]::new));
      try {
        stmt.setArray(1, array);
        if (options instanceof ForkFromFailureOptions.FromStepName fromStepName) {
          stmt.setString(2, fromStepName.stepName());
        }
        try (var rs = stmt.executeQuery()) {
          while (rs.next()) {
            startStepByWorkflowId.put(rs.getString("workflow_uuid"), rs.getInt("start_step"));
          }
        }
      } finally {
        array.free();
      }
    }

    List<Integer> startSteps = new ArrayList<>(workflowIds.size());
    for (String wid : workflowIds) {
      if (!startStepByWorkflowId.containsKey(wid)) {
        if (options instanceof ForkFromFailureOptions.FromStepName fromStepName) {
          throw new IllegalArgumentException(
              "Workflow " + wid + " has no step named '" + fromStepName.stepName() + "'");
        }
        throw new IllegalArgumentException("Workflow " + wid + " has no steps");
      }
      startSteps.add(startStepByWorkflowId.get(wid));
    }
    return startSteps;
  }

  private static void forkWorkflows(
      DbContext ctx,
      List<String> workflowIds,
      List<String> forkIds,
      List<Integer> startSteps,
      @Nullable String applicationVersion,
      @Nullable String queueName,
      @Nullable String queuePartitionKey,
      @Nullable Duration timeout)
      throws SQLException {

    if (workflowIds.isEmpty()) {
      return;
    }

    if (workflowIds.size() != forkIds.size() || workflowIds.size() != startSteps.size()) {
      throw new IllegalArgumentException(
          "workflowIds, forkIds and startSteps must have the same length");
    }

    var timeoutMs = timeout != null ? timeout.toMillis() : null;
    queueName = Objects.requireNonNullElse(queueName, Constants.DBOS_INTERNAL_QUEUE);

    try (var conn = ctx.getConnection()) {
      conn.setAutoCommit(false);
      try {
        var wfDataMap = fetchForkWorkflowData(conn, ctx.schema(), workflowIds);
        for (String id : workflowIds) {
          if (!wfDataMap.containsKey(id)) {
            throw new DBOSNonExistentWorkflowException(id);
          }
        }
        var dataList = workflowIds.stream().map(wfDataMap::get).toList();

        batchInsertForkedStatuses(
            conn,
            ctx.schema(),
            workflowIds,
            forkIds,
            dataList,
            applicationVersion,
            queueName,
            queuePartitionKey,
            timeoutMs);

        markWasForkedFrom(conn, ctx.schema(), workflowIds);

        List<String> copyOrigIds = new ArrayList<>();
        List<String> copyForkIds = new ArrayList<>();
        List<Integer> copyStartSteps = new ArrayList<>();
        for (int i = 0; i < workflowIds.size(); i++) {
          if (startSteps.get(i) > 0) {
            copyOrigIds.add(workflowIds.get(i));
            copyForkIds.add(forkIds.get(i));
            copyStartSteps.add(startSteps.get(i));
          }
        }
        if (!copyOrigIds.isEmpty()) {
          batchCopyWorkflowData(conn, ctx.schema(), copyOrigIds, copyForkIds, copyStartSteps);
        }

        conn.commit();
      } catch (SQLException e) {
        conn.rollback();
        throw e;
      }
    }
  }

  private record ForkWorkflowData(
      String name,
      String className,
      String configName,
      String applicationVersion,
      String applicationId,
      String authenticatedUser,
      String authenticatedRoles,
      String assumedRole,
      String inputs,
      String serialization,
      String attributes) {}

  private static Map<String, ForkWorkflowData> fetchForkWorkflowData(
      Connection conn, String schema, List<String> workflowIds) throws SQLException {
    String sql =
        """
          SELECT workflow_uuid, name, class_name, config_name, application_version,
                 application_id, authenticated_user, authenticated_roles, assumed_role,
                 inputs, serialization, attributes
          FROM "%s".workflow_status
          WHERE workflow_uuid = ANY(?)
        """
            .formatted(schema);

    Map<String, ForkWorkflowData> result = new HashMap<>();
    try (var stmt = conn.prepareStatement(sql)) {
      Array array = conn.createArrayOf("text", workflowIds.toArray(String[]::new));
      try {
        stmt.setArray(1, array);
        try (var rs = stmt.executeQuery()) {
          while (rs.next()) {
            result.put(
                rs.getString("workflow_uuid"),
                new ForkWorkflowData(
                    rs.getString("name"),
                    rs.getString("class_name"),
                    rs.getString("config_name"),
                    rs.getString("application_version"),
                    rs.getString("application_id"),
                    rs.getString("authenticated_user"),
                    rs.getString("authenticated_roles"),
                    rs.getString("assumed_role"),
                    rs.getString("inputs"),
                    rs.getString("serialization"),
                    rs.getString("attributes")));
          }
        }
      } finally {
        array.free();
      }
    }
    return result;
  }

  private static void batchInsertForkedStatuses(
      Connection conn,
      String schema,
      List<String> origIds,
      List<String> forkIds,
      List<ForkWorkflowData> dataList,
      String applicationVersion,
      String queueName,
      String queuePartitionKey,
      @Nullable Long timeoutMs)
      throws SQLException {

    StringBuilder sql =
        new StringBuilder(
            """
              INSERT INTO "%s".workflow_status (
                workflow_uuid, status, name, class_name, config_name,
                application_version, application_id, authenticated_user,
                authenticated_roles, assumed_role, queue_name, queue_partition_key,
                inputs, workflow_timeout_ms, forked_from, serialization, attributes
              ) VALUES\s
            """
                .formatted(schema));

    StringJoiner rows = new StringJoiner(", ");
    for (int i = 0; i < origIds.size(); i++) {
      rows.add("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb)");
    }
    sql.append(rows);

    try (var stmt = conn.prepareStatement(sql.toString())) {
      int p = 1;
      for (int i = 0; i < origIds.size(); i++) {
        ForkWorkflowData rd = dataList.get(i);
        stmt.setString(p++, forkIds.get(i));
        stmt.setString(p++, WorkflowState.ENQUEUED.name());
        stmt.setString(p++, rd.name());
        stmt.setString(p++, rd.className());
        stmt.setString(p++, rd.configName());
        // Fall back to the original workflow's application_version when none is specified.
        // Matches TypeScript behavior; Python does not fall back (passes null).
        stmt.setString(
            p++, applicationVersion != null ? applicationVersion : rd.applicationVersion());
        stmt.setString(p++, rd.applicationId());
        stmt.setString(p++, rd.authenticatedUser());
        stmt.setString(p++, rd.authenticatedRoles());
        stmt.setString(p++, rd.assumedRole());
        stmt.setString(p++, Objects.requireNonNullElse(queueName, Constants.DBOS_INTERNAL_QUEUE));
        stmt.setString(p++, queuePartitionKey);
        stmt.setString(p++, rd.inputs());
        stmt.setObject(p++, timeoutMs);
        stmt.setString(p++, origIds.get(i));
        stmt.setString(p++, rd.serialization());
        stmt.setString(p++, rd.attributes());
      }
      stmt.executeUpdate();
    }
  }

  private static void markWasForkedFrom(Connection conn, String schema, List<String> origIds)
      throws SQLException {
    String sql =
        """
            UPDATE "%s".workflow_status SET was_forked_from = TRUE WHERE workflow_uuid = ANY(?)
          """
            .formatted(schema);
    Array arr = conn.createArrayOf("text", origIds.toArray(String[]::new));
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setArray(1, arr);
      stmt.executeUpdate();
    } finally {
      arr.free();
    }
  }

  private static void batchCopyWorkflowData(
      Connection conn,
      String schema,
      List<String> origIds,
      List<String> forkIds,
      List<Integer> startSteps)
      throws SQLException {

    StringJoiner valueRows = new StringJoiner(", ");
    for (int i = 0; i < origIds.size(); i++) {
      valueRows.add("(?::text, ?::text, ?::int)");
    }
    String mappingCTE =
        "WITH mapping(orig_id, fork_id, start_step) AS (VALUES " + valueRows + ")\n";

    String ooSql =
        mappingCTE
            + """
              INSERT INTO "%1$s".operation_outputs
                (workflow_uuid, function_id, output, error, function_name,
                 child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization)
              SELECT m.fork_id, oo.function_id, oo.output, oo.error, oo.function_name,
                     oo.child_workflow_id, oo.started_at_epoch_ms, oo.completed_at_epoch_ms,
                     oo.serialization
              FROM mapping m
              JOIN "%1$s".operation_outputs oo
                ON oo.workflow_uuid = m.orig_id AND oo.function_id < m.start_step
            """
                .formatted(schema);

    String wehSql =
        mappingCTE
            + """
              INSERT INTO "%1$s".workflow_events_history
                (workflow_uuid, function_id, key, value, serialization)
              SELECT m.fork_id, weh.function_id, weh.key, weh.value, weh.serialization
              FROM mapping m
              JOIN "%1$s".workflow_events_history weh
                ON weh.workflow_uuid = m.orig_id AND weh.function_id < m.start_step
            """
                .formatted(schema);

    // Copy only the latest value per event key using a window function
    String weSql =
        mappingCTE
            + """
              , ranked AS (
                SELECT m.fork_id,
                       weh.key,
                       weh.value,
                       weh.serialization,
                       ROW_NUMBER() OVER (
                         PARTITION BY weh.workflow_uuid, weh.key
                         ORDER BY weh.function_id DESC
                       ) AS rn
                FROM mapping m
                JOIN "%1$s".workflow_events_history weh
                  ON weh.workflow_uuid = m.orig_id AND weh.function_id < m.start_step
              )
              INSERT INTO "%1$s".workflow_events (workflow_uuid, key, value, serialization)
              SELECT fork_id, key, value, serialization
              FROM ranked
              WHERE rn = 1
            """
                .formatted(schema);

    String streamSql =
        mappingCTE
            + """
              INSERT INTO "%1$s".streams
                (workflow_uuid, function_id, key, value, "offset", serialization)
              SELECT m.fork_id, s.function_id, s.key, s.value, s."offset", s.serialization
              FROM mapping m
              JOIN "%1$s".streams s
                ON s.workflow_uuid = m.orig_id AND s.function_id < m.start_step
            """
                .formatted(schema);

    for (String sql : List.of(ooSql, wehSql, weSql, streamSql)) {
      try (var stmt = conn.prepareStatement(sql)) {
        int p = 1;
        for (int i = 0; i < origIds.size(); i++) {
          stmt.setString(p++, origIds.get(i));
          stmt.setString(p++, forkIds.get(i));
          stmt.setInt(p++, startSteps.get(i));
        }
        stmt.executeUpdate();
      }
    }
  }

  private static Instant getRowsCutoff(Connection conn, String schema, long rowsThreshold)
      throws SQLException {
    String sql =
        """
          SELECT created_at FROM "%s".workflow_status ORDER BY created_at DESC OFFSET ? LIMIT 1
        """
            .formatted(schema);
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setLong(1, rowsThreshold - 1);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return Instant.ofEpochMilli(rs.getLong("created_at"));
        }
      }
    }

    return null;
  }

  public static void garbageCollect(DbContext ctx, Instant cutoff, Long rowsThreshold)
      throws SQLException {

    try (var conn = ctx.getConnection()) {
      if (rowsThreshold != null) {
        var rowsCutoff = getRowsCutoff(conn, ctx.schema(), rowsThreshold);
        if (rowsCutoff != null) {
          if (cutoff == null || rowsCutoff.isAfter(cutoff)) {
            cutoff = rowsCutoff;
          }
        }
      }

      if (cutoff != null) {
        String sql =
            """
              DELETE FROM "%s".workflow_status WHERE created_at < ? AND status NOT IN (?, ?, ?)
            """
                .formatted(ctx.schema());
        try (var stmt = conn.prepareStatement(sql)) {
          stmt.setLong(1, cutoff.toEpochMilli());
          stmt.setString(2, WorkflowState.PENDING.name());
          stmt.setString(3, WorkflowState.ENQUEUED.name());
          stmt.setString(4, WorkflowState.DELAYED.name());

          stmt.executeUpdate();
        }
      }
    }
  }

  public static List<MetricData> getMetrics(DbContext ctx, Instant startTime, Instant endTime)
      throws SQLException {
    final var start = Objects.requireNonNull(startTime).toEpochMilli();
    final var end = Objects.requireNonNull(endTime).toEpochMilli();
    logger.debug("getMetrics {} {}", start, end);
    List<MetricData> metrics = new ArrayList<>();
    final var wfSQL =
        """
          SELECT name, COUNT(workflow_uuid) as count
          FROM "%s".workflow_status
          WHERE created_at >= ? AND created_at < ?
          GROUP BY name
        """
            .formatted(ctx.schema());
    final var stepSQL =
        """
          SELECT function_name, COUNT(*) as count
          FROM "%s".operation_outputs
          WHERE completed_at_epoch_ms >= ? AND completed_at_epoch_ms < ?
          GROUP BY function_name
        """
            .formatted(ctx.schema());

    try (var conn = ctx.getConnection();
        var ps1 = conn.prepareStatement(wfSQL);
        var ps2 = conn.prepareStatement(stepSQL)) {

      ps1.setLong(1, start);
      ps1.setLong(2, end);

      try (var rs = ps1.executeQuery()) {
        while (rs.next()) {
          var name = rs.getString("name");
          var count = rs.getInt("count");
          metrics.add(new MetricData("workflow_count", name, count));
        }
      }

      ps2.setLong(1, start);
      ps2.setLong(2, end);

      try (var rs = ps2.executeQuery()) {
        while (rs.next()) {
          var name = rs.getString("function_name");
          var count = rs.getInt("count");
          metrics.add(new MetricData("step_count", name, count));
        }
      }
    }

    return metrics;
  }

  static List<WorkflowEvent> listWorkflowEvents(Connection conn, String schema, String workflowId)
      throws SQLException {
    var sql =
        """
        SELECT key, value, serialization
        FROM "%s".workflow_events
        WHERE workflow_uuid = ?
        """
            .formatted(schema);

    var events = new ArrayList<WorkflowEvent>();
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        while (rs.next()) {
          var key = rs.getString("key");
          var value = rs.getString("value");
          var serialization = rs.getString("serialization");
          events.add(new WorkflowEvent(key, value, serialization));
        }
      }
    }
    return events;
  }

  static List<WorkflowEventHistory> listWorkflowEventHistory(
      Connection conn, String schema, String workflowId) throws SQLException {
    var sql =
        """
        SELECT key, value, function_id, serialization
        FROM "%s".workflow_events_history
        WHERE workflow_uuid = ?
        """
            .formatted(schema);

    var history = new ArrayList<WorkflowEventHistory>();
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        while (rs.next()) {
          var key = rs.getString("key");
          var value = rs.getString("value");
          var stepId = rs.getInt("function_id");
          var serialization = rs.getString("serialization");
          history.add(new WorkflowEventHistory(key, value, stepId, serialization));
        }
      }
    }
    return history;
  }

  static List<WorkflowStream> listWorkflowStreams(Connection conn, String schema, String workflowId)
      throws SQLException {
    var sql =
        """
        SELECT key, value, "offset", function_id, serialization
        FROM "%s".streams
        WHERE workflow_uuid = ?
        """
            .formatted(schema);

    var streams = new ArrayList<WorkflowStream>();
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        while (rs.next()) {
          var key = rs.getString("key");
          var value = rs.getString("value");
          var offset = rs.getInt("offset");
          var stepId = rs.getInt("function_id");
          var serialization = rs.getString("serialization");
          streams.add(new WorkflowStream(key, value, offset, stepId, serialization));
        }
      }
    }
    return streams;
  }

  public static List<ExportedWorkflow> exportWorkflow(
      DbContext ctx, String workflowId, boolean exportChildren) throws SQLException {

    var workflowIds =
        exportChildren
            ? Stream.concat(
                    getWorkflowChildren(ctx, workflowId).stream(), List.of(workflowId).stream())
                .toList()
            : List.of(workflowId);

    var workflows = new ArrayList<ExportedWorkflow>();
    for (var wfid : workflowIds) {
      try (var conn = ctx.getConnection()) {
        var status = getWorkflowStatus(conn, ctx.schema(), ctx.serializer(), wfid);
        var steps =
            StepsDAO.listWorkflowSteps(
                conn, ctx.schema(), ctx.serializer(), wfid, true, null, null);
        var events = listWorkflowEvents(conn, ctx.schema(), wfid);
        var eventHistory = listWorkflowEventHistory(conn, ctx.schema(), wfid);
        var streams = listWorkflowStreams(conn, ctx.schema(), wfid);
        workflows.add(new ExportedWorkflow(status, steps, events, eventHistory, streams));
      }
    }
    return workflows;
  }

  public static void importWorkflow(DbContext ctx, List<ExportedWorkflow> workflows)
      throws SQLException {

    DBOSSerializer serializer = ctx.serializer();
    var wfSQL =
        """
        INSERT INTO "%s".workflow_status (
          workflow_uuid, status,
          name, class_name, config_name,
          authenticated_user, assumed_role, authenticated_roles,
          output, error, inputs,
          executor_id, application_version, application_id,
          created_at, updated_at, started_at_epoch_ms,
          queue_name, deduplication_id, priority, queue_partition_key,
          workflow_timeout_ms, workflow_deadline_epoch_ms,
          recovery_attempts, forked_from, parent_workflow_id, serialization,
          delay_until_epoch_ms, completed_at
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """
            .formatted(ctx.schema());

    var stepSQL =
        """
        INSERT INTO "%s".operation_outputs (
          workflow_uuid, function_id, function_name,
          output, error, child_workflow_id,
          started_at_epoch_ms, completed_at_epoch_ms,
          serialization
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """
            .formatted(ctx.schema());

    var eventSQL =
        """
        INSERT INTO "%s".workflow_events (
          workflow_uuid, key, value, serialization
        ) VALUES (
          ?, ?, ?, ?
        )
        """
            .formatted(ctx.schema());

    var eventHistorySQL =
        """
        INSERT INTO "%s".workflow_events_history (
          workflow_uuid, key, value, function_id, serialization
        ) VALUES (
          ?, ?, ?, ?, ?
        )
        """
            .formatted(ctx.schema());

    var streamsSQL =
        """
        INSERT INTO "%s".streams (
          workflow_uuid, key, value, function_id, "offset", serialization
        ) VALUES (
          ?, ?, ?, ?, ?, ?
        )
        """
            .formatted(ctx.schema());

    try (var conn = ctx.getConnection()) {
      conn.setAutoCommit(false);

      try (var wfStmt = conn.prepareStatement(wfSQL);
          var stepStmt = conn.prepareStatement(stepSQL);
          var eventStmt = conn.prepareStatement(eventSQL);
          var eventHistoryStmt = conn.prepareStatement(eventHistorySQL);
          var streamsStmt = conn.prepareStatement(streamsSQL)) {

        for (var workflow : workflows) {
          var status = workflow.status();

          wfStmt.setString(1, status.workflowId());
          wfStmt.setString(2, status.status().name());
          wfStmt.setString(3, status.workflowName());
          wfStmt.setString(4, status.className());
          wfStmt.setString(5, status.instanceName());
          wfStmt.setString(6, status.authenticatedUser());
          wfStmt.setString(7, status.assumedRole());
          wfStmt.setString(
              8,
              status.authenticatedRoles() == null
                  ? null
                  : JsonUtility.toJson(status.authenticatedRoles()));
          wfStmt.setString(
              9,
              status.output() == null
                  ? null
                  : SerializationUtil.serializeValue(
                          status.output(), status.serialization(), serializer)
                      .serializedValue());
          wfStmt.setString(
              10,
              status.error() == null
                  ? null
                  : SerializationUtil.serializeError(
                          status.error().throwable(), status.serialization(), serializer)
                      .serializedValue());
          wfStmt.setString(
              11,
              status.input() == null
                  ? null
                  : SerializationUtil.serializeArgs(
                          status.input(), null, status.serialization(), serializer)
                      .serializedValue());
          wfStmt.setString(12, status.executorId());
          wfStmt.setString(13, status.appVersion());
          wfStmt.setString(14, status.appId());
          wfStmt.setObject(15, status.createdAtEpochMs());
          wfStmt.setObject(16, status.updatedAtEpochMs());
          wfStmt.setObject(17, status.startedAtEpochMs());
          wfStmt.setString(18, status.queueName());
          wfStmt.setString(19, status.deduplicationId());
          wfStmt.setObject(20, status.priority());
          wfStmt.setString(21, status.queuePartitionKey());
          wfStmt.setObject(22, status.timeoutMs());
          wfStmt.setObject(23, status.deadlineEpochMs());
          wfStmt.setObject(24, status.recoveryAttempts());
          wfStmt.setString(25, status.forkedFrom());
          wfStmt.setString(26, status.parentWorkflowId());
          wfStmt.setString(27, status.serialization());
          wfStmt.setObject(28, status.delayUntilEpochMs());
          wfStmt.setObject(29, status.completedAtEpochMs());
          wfStmt.addBatch();

          for (var step : workflow.steps()) {
            stepStmt.setString(1, status.workflowId());
            stepStmt.setInt(2, step.functionId());
            stepStmt.setString(3, step.functionName());
            stepStmt.setString(
                4,
                step.output() == null
                    ? null
                    : SerializationUtil.serializeValue(
                            step.output(), step.serialization(), serializer)
                        .serializedValue());
            stepStmt.setString(5, step.error() == null ? null : step.error().serializedError());
            stepStmt.setString(6, step.childWorkflowId());
            stepStmt.setObject(7, step.startedAtEpochMs());
            stepStmt.setObject(8, step.completedAtEpochMs());
            stepStmt.setString(9, step.serialization());
            stepStmt.addBatch();
          }

          for (var event : workflow.events()) {
            eventStmt.setString(1, status.workflowId());
            eventStmt.setString(2, event.key());
            eventStmt.setString(3, event.value());
            eventStmt.setString(4, event.serialization());
            eventStmt.addBatch();
          }

          for (var history : workflow.eventHistory()) {
            eventHistoryStmt.setString(1, status.workflowId());
            eventHistoryStmt.setString(2, history.key());
            eventHistoryStmt.setString(3, history.value());
            eventHistoryStmt.setInt(4, history.stepId());
            eventHistoryStmt.setString(5, history.serialization());
            eventHistoryStmt.addBatch();
          }

          for (var stream : workflow.streams()) {
            streamsStmt.setString(1, status.workflowId());
            streamsStmt.setString(2, stream.key());
            streamsStmt.setString(3, stream.value());
            streamsStmt.setInt(4, stream.stepId());
            streamsStmt.setInt(5, stream.offset());
            streamsStmt.setString(6, stream.serialization());
            streamsStmt.addBatch();
          }
        }

        wfStmt.executeBatch();
        stepStmt.executeBatch();
        eventStmt.executeBatch();
        eventHistoryStmt.executeBatch();
        streamsStmt.executeBatch();

        conn.commit();
      } catch (SQLException e) {
        conn.rollback();
        throw e;
      }
    }
  }

  public static Map<String, Object> getAllEvents(DbContext ctx, String workflowId)
      throws SQLException {
    try (var conn = ctx.getConnection()) {
      var events = listWorkflowEvents(conn, ctx.schema(), workflowId);
      var result = new LinkedHashMap<String, Object>();
      for (var event : events) {
        result.put(
            event.key(),
            SerializationUtil.deserializeValue(
                event.value(), event.serialization(), ctx.serializer()));
      }
      return result;
    }
  }
}
