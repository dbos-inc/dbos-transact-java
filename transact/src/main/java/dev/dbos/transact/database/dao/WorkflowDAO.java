package dev.dbos.transact.database.dao;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.database.MetricData;
import dev.dbos.transact.database.Result;
import dev.dbos.transact.database.SqlTransaction;
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
import dev.dbos.transact.workflow.DeduplicationHolder;
import dev.dbos.transact.workflow.ErrorResult;
import dev.dbos.transact.workflow.ExportedWorkflow;
import dev.dbos.transact.workflow.ForkFromFailureOptions;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.GetStepAggregatesInput;
import dev.dbos.transact.workflow.GetWorkflowAggregatesInput;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepAggregateRow;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowAggregateRow;
import dev.dbos.transact.workflow.WorkflowDelay;
import dev.dbos.transact.workflow.WorkflowEvent;
import dev.dbos.transact.workflow.WorkflowEventHistory;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.WorkflowStream;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.zaxxer.hikari.HikariDataSource;
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
        forked_from, parent_workflow_id, was_forked_from, attributes, schedule_name,
        application_name
      """;

  // Migration 109 created the tables that input and output payloads move into. The Java SDK
  // currently still writes the legacy workflow_status columns, but a later release will write to
  // the new tables. So every read prefers the new table but falls back to the old column. We are
  // splitting the read and write changes into separate releases to enable rolling upgrades.
  // Correlated on the primary key, so each is an index lookup.
  private static String inputsColumn(String schema) {
    return ("COALESCE((SELECT wi.inputs FROM \"%s\".workflow_input wi"
            + " WHERE wi.workflow_uuid = workflow_status.workflow_uuid),"
            + " workflow_status.inputs) AS inputs")
        .formatted(schema);
  }

  private static String outputColumns(String schema) {
    return ("COALESCE((SELECT wo.output FROM \"%1$s\".workflow_output wo"
            + " WHERE wo.workflow_uuid = workflow_status.workflow_uuid),"
            + " workflow_status.output) AS output,"
            + " COALESCE((SELECT wo.error FROM \"%1$s\".workflow_output wo"
            + " WHERE wo.workflow_uuid = workflow_status.workflow_uuid),"
            + " workflow_status.error) AS error")
        .formatted(schema);
  }

  private WorkflowDAO() {}

  /**
   * Scopes a listing to the applications asked for, or to this one by default, plus unclaimed rows,
   * which belong to every application. Adds nothing when the scope is empty: an explicitly empty
   * filter, or a nameless owner, lists every application's rows.
   *
   * <p>{@code idKeyed} suppresses the default. A workflow ID is a global address, so a listing that
   * names IDs is an identity read: it honours an explicit filter but is never narrowed to this
   * application behind the caller's back, which is what lets one application look up a workflow it
   * handed to a peer.
   */
  private static void addAppScope(
      DbContext ctx,
      StringJoiner whereConditions,
      List<Object> parameters,
      @Nullable List<String> requested,
      boolean idKeyed) {
    var names = idKeyed ? ctx.requestedNames(requested) : ctx.scopeNames(requested);
    if (names != null) {
      whereConditions.add("(application_name = ANY(?) OR application_name IS NULL)");
      parameters.add(names);
    }
  }

  /**
   * The application a workflow row belongs to. A status naming one is enqueuing for that
   * application -- the whole of the cross-application contract -- and the handle's own is the
   * default for everything else, including a nameless handle, which owns nothing.
   */
  private static @Nullable String owner(DbContext ctx, WorkflowStatusInternal status) {
    return status.applicationName() != null ? status.applicationName() : ctx.appName();
  }

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
                conn,
                ctx.schema(),
                initStatus,
                ownerXid,
                isRecoveryRequest || isDequeuedRequest,
                owner(ctx, initStatus));

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
                SET status = ?, deduplication_id = NULL, started_at_epoch_ms = NULL, queue_name = NULL,
                    updated_at = ?, completed_at = ?
                WHERE workflow_uuid = ? AND status = ?
              """
                  .formatted(ctx.schema());

          try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            long now = System.currentTimeMillis();
            stmt.setString(1, WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED.name());
            stmt.setLong(2, now);
            stmt.setLong(3, now);
            stmt.setString(4, initStatus.workflowId());
            stmt.setString(5, WorkflowState.PENDING.name());

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
      boolean incrementAttempts,
      @Nullable String appName)
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
            parent_workflow_id, owner_xid, serialization, attributes, schedule_name,
            application_name
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?)
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
              END,
              application_name = COALESCE(workflow_status.application_name, EXCLUDED.application_name)
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
      stmt.setString(28, appName);
      stmt.setInt(29, incrementAttempts ? 1 : 0);

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

  /**
   * Record a workflow's terminal outcome, reporting whether the write landed. The write applies
   * only to a PENDING row: a run owns its workflow's outcome exactly as long as the row says that
   * run is what the workflow is doing. (Note: this does not prevent a write when another concurrent
   * execution is already running and the status is PENDING. However, both executions should be
   * deterministic and idempotent.)
   *
   * <p>Returning false means the row was CANCELLED, dead-lettered, already terminal, handed to
   * another execution (ENQUEUED/DELAYED, e.g. by a concurrent resume), or gone entirely. Callers
   * that need to distinguish a deleted row do so when they park on the recorded outcome (see {@link
   * #awaitWorkflowResult(DbContext, Duration, String, boolean)}).
   */
  static boolean updateWorkflowOutcome(
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

    var sql =
        """
          UPDATE "%s".workflow_status
          SET status = ?, output = ?, error = ?, updated_at = ?, completed_at = ?, deduplication_id = NULL
          WHERE workflow_uuid = ? AND status = ?
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
      stmt.setString(7, WorkflowState.PENDING.name());

      return stmt.executeUpdate() != 0;
    }
  }

  /**
   * Store the result to workflow_status
   *
   * @param workflowId id of the workflow
   * @param result output serialized as json
   * @return true if the outcome was recorded, false if the row is no longer PENDING
   */
  public static boolean recordWorkflowOutput(DbContext ctx, String workflowId, String result)
      throws SQLException {

    try (var conn = ctx.getConnection()) {
      return updateWorkflowOutcome(
          conn, ctx.schema(), workflowId, WorkflowState.SUCCESS, result, null);
    }
  }

  /**
   * Store the error to workflow_status
   *
   * @param workflowId id of the workflow
   * @param error output serialized as json
   * @return true if the outcome was recorded, false if the row is no longer PENDING
   */
  public static boolean recordWorkflowError(DbContext ctx, String workflowId, String error)
      throws SQLException {

    try (var conn = ctx.getConnection()) {
      return updateWorkflowOutcome(
          conn, ctx.schema(), workflowId, WorkflowState.ERROR, null, error);
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
      insertWorkflowStatus(
          conn,
          ctx.schema(),
          initStatus,
          UUID.randomUUID().toString(),
          false,
          owner(ctx, initStatus));
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
        ("SELECT "
                + WORKFLOW_STATUS_COLUMNS
                + ", "
                + inputsColumn(schema)
                + ", "
                + outputColumns(schema)
                + ", serialization")
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
    var holder = findDeduplicationHolder(ctx, queueName, deduplicationId);
    return holder == null ? null : holder.workflowId();
  }

  /**
   * The workflow currently holding a given (queue_name, deduplication_id) pair, with the
   * application that owns it, or {@code null} if the pair is unheld. Uses the UNIQUE index on that
   * pair for O(1) lookup.
   *
   * <p>That index is global across the applications sharing the system database, so the holder is
   * not necessarily ours. The read is deliberately unscoped: a caller cannot steer around a holder
   * it cannot see, and the debouncer needs the owner precisely so it can refuse.
   */
  public static @Nullable DeduplicationHolder findDeduplicationHolder(
      DbContext ctx, String queueName, String deduplicationId) throws SQLException {
    var sql =
        """
          SELECT workflow_uuid, application_name
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
        return rs.next()
            ? new DeduplicationHolder(
                rs.getString("workflow_uuid"), rs.getString("application_name"))
            : null;
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
                .formatted(ctx.schema())
            + ctx.andAppScope();

    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, WorkflowState.ENQUEUED.name());
      stmt.setString(2, WorkflowState.DELAYED.name());
      stmt.setLong(3, System.currentTimeMillis());
      ctx.bindAppScope(stmt, 4);

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
      sqlBuilder.append(", ").append(inputsColumn(ctx.schema()));
    }
    if (loadOutput) {
      sqlBuilder.append(", ").append(outputColumns(ctx.schema()));
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
    // Null and empty alike mean "not keyed by ID": a Conductor request carries an omitted list
    // as JSON null.
    var idKeyed = input.workflowIds() != null && !input.workflowIds().isEmpty();
    addAppScope(ctx, whereConditions, parameters, input.applicationName(), idKeyed);

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
    if (input.groupByApplicationName())
      dims.add(new GroupDim("application_name", "application_name"));
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
    addAppScope(ctx, whereConditions, parameters, input.applicationName(), false);

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
    addAppScope(ctx, whereConditions, parameters, input.applicationName(), false);

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
    String serializedError = loadOutput ? SystemDatabase.errorOrNull(rs.getString("error")) : null;
    String serialization = loadInput || loadOutput ? rs.getString("serialization") : null;
    // A status read reaches other applications' rows on purpose, and wants their metadata; an
    // unreadable payload comes back null rather than failing the read.
    boolean readable = SerializationUtil.canDeserialize(serialization, serializer);
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
            loadInput && readable
                ? SerializationUtil.deserializePositionalArgs(
                    serializedInput, serialization, serializer)
                : null,
            loadOutput && readable
                ? SerializationUtil.deserializeValue(serializedOutput, serialization, serializer)
                : null,
            loadOutput && readable
                ? ErrorResult.deserialize(serializedError, serialization, serializer)
                : null,
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
            rs.getString("schedule_name"),
            rs.getString("application_name"));
    return info;
  }

  /**
   * Poll the workflow's row until it reaches a terminal state, then return the recorded outcome.
   *
   * <p>A missing row normally means the workflow just hasn't been inserted yet (an unchecked
   * retrieve, or a debounced workflow whose row appears only after the debounce period), so polling
   * is correct. Callers that know the row must already exist (a run parking on an outcome it just
   * failed to write) pass {@code failIfMissing} to fail fast with {@link
   * DBOSNonExistentWorkflowException} instead of polling forever.
   */
  @SuppressWarnings("unchecked")
  public static <T> Result<T> awaitWorkflowResult(
      DbContext ctx, Duration dbPollingInterval, String workflowId, boolean failIfMissing)
      throws SQLException {

    DBOSSerializer serializer = ctx.serializer();
    final String sql =
        """
          SELECT status, %s, serialization, recovery_attempts
          FROM "%s".workflow_status
          WHERE workflow_uuid = ?
        """
            .formatted(outputColumns(ctx.schema()), ctx.schema());

    while (true) {
      ctx.checkClosed();
      try (var permit = ctx.acquirePollPermit();
          Connection connection = ctx.getConnection();
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
                String error = SystemDatabase.errorOrNull(rs.getString("error"));
                Throwable t = SerializationUtil.deserializeError(error, serialization, serializer);
                return Result.failure(t);
              }
              case CANCELLED -> throw new DBOSAwaitedWorkflowCancelledException(workflowId);

              case MAX_RECOVERY_ATTEMPTS_EXCEEDED -> {
                // A workflow is dead-lettered by the attempt that pushes recovery_attempts
                // past maxRetries+1, so a dead-lettered row carries maxRetries+2 attempts.
                int maxRetries = Math.max(0, rs.getInt("recovery_attempts") - 2);
                throw new DBOSMaxRecoveryAttemptsExceededException(workflowId, maxRetries);
              }

              default -> {}
            }
            // Status is PENDING or other - continue polling
          } else if (failIfMissing) {
            // The caller knows the row must already exist, so a missing row means it was
            // deleted: fail fast instead of polling forever.
            throw new DBOSNonExistentWorkflowException(workflowId);
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
      StepsDAO.recordStepResult(ctx, conn, result, null, null);
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

    var ids = wfIdSet.toArray(String[]::new);
    try (var conn = ctx.getConnection()) {
      SqlTransaction.run(
          conn,
          c -> {
            try (var stmt = c.prepareStatement(sql)) {
              var array = c.createArrayOf("text", ids);
              try {
                stmt.setArray(1, array);
                stmt.executeUpdate();
              } finally {
                array.free();
              }
            }
            deleteWorkflowChildRows(c, ctx.schema(), ids);
          });
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
    final var forkQueueName = Objects.requireNonNullElse(queueName, Constants.DBOS_INTERNAL_QUEUE);

    try (var txConn = ctx.getConnection()) {
      SqlTransaction.run(
          txConn,
          conn -> {
            var wfDataMap = fetchForkWorkflowData(conn, ctx.schema(), workflowIds);
            for (String id : workflowIds) {
              if (!wfDataMap.containsKey(id)) {
                throw new DBOSNonExistentWorkflowException(id);
              }
            }
            var dataList = workflowIds.stream().map(wfDataMap::get).toList();

            // One app name per fork, shared by its status row and its copied steps: the source's,
            // or this application claiming an unclaimed one. Matches Python, TypeScript, and Go.
            List<@Nullable String> forkAppNames = new ArrayList<>(forkIds.size());
            for (var rd : dataList) {
              forkAppNames.add(rd.applicationName() != null ? rd.applicationName() : ctx.appName());
            }

            batchInsertForkedStatuses(
                conn,
                ctx.schema(),
                workflowIds,
                forkIds,
                dataList,
                applicationVersion,
                forkQueueName,
                queuePartitionKey,
                timeoutMs,
                forkAppNames);

            markWasForkedFrom(conn, ctx.schema(), workflowIds);

            List<String> copyOrigIds = new ArrayList<>();
            List<String> copyForkIds = new ArrayList<>();
            List<Integer> copyStartSteps = new ArrayList<>();
            List<@Nullable String> copyAppNames = new ArrayList<>();
            for (int i = 0; i < workflowIds.size(); i++) {
              if (startSteps.get(i) > 0) {
                copyOrigIds.add(workflowIds.get(i));
                copyForkIds.add(forkIds.get(i));
                copyStartSteps.add(startSteps.get(i));
                copyAppNames.add(forkAppNames.get(i));
              }
            }
            if (!copyOrigIds.isEmpty()) {
              batchCopyWorkflowData(
                  conn, ctx.schema(), copyOrigIds, copyForkIds, copyStartSteps, copyAppNames);
            }
          });
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
      String attributes,
      @Nullable String applicationName) {}

  private static Map<String, ForkWorkflowData> fetchForkWorkflowData(
      Connection conn, String schema, List<String> workflowIds) throws SQLException {
    String sql =
        """
          SELECT workflow_uuid, name, class_name, config_name, application_version,
                 application_id, authenticated_user, authenticated_roles, assumed_role,
                 %s, serialization, attributes, application_name
          FROM "%s".workflow_status
          WHERE workflow_uuid = ANY(?)
        """
            .formatted(inputsColumn(schema), schema);

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
                    rs.getString("attributes"),
                    rs.getString("application_name")));
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
      @Nullable Long timeoutMs,
      List<@Nullable String> forkAppNames)
      throws SQLException {

    StringBuilder sql =
        new StringBuilder(
            """
              INSERT INTO "%s".workflow_status (
                workflow_uuid, status, name, class_name, config_name,
                application_version, application_id, authenticated_user,
                authenticated_roles, assumed_role, queue_name, queue_partition_key,
                inputs, workflow_timeout_ms, forked_from, serialization, attributes,
                application_name
              ) VALUES\s
            """
                .formatted(schema));

    StringJoiner rows = new StringJoiner(", ");
    for (int i = 0; i < origIds.size(); i++) {
      rows.add("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)");
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
        stmt.setString(p++, forkAppNames.get(i));
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

  /**
   * Claims {@code workflowId} for {@code executorId}, skipping the write when the workflow is
   * already stamped with it. No-ops when {@code executorId} is null, as it is for contexts that
   * have no executor identity of their own (such as {@link dev.dbos.transact.DBOSClient}).
   *
   * <p>Runs on the caller's connection so it joins the caller's transaction.
   */
  static void restampExecutorId(
      Connection conn, String schema, String workflowId, @Nullable String executorId)
      throws SQLException {
    if (executorId == null) {
      return;
    }
    String sql =
        """
            UPDATE "%s".workflow_status SET executor_id = ? WHERE workflow_uuid = ? AND executor_id IS DISTINCT FROM ?
          """
            .formatted(schema);
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, executorId);
      stmt.setString(2, workflowId);
      stmt.setString(3, executorId);
      stmt.executeUpdate();
    }
  }

  private static void batchCopyWorkflowData(
      Connection conn,
      String schema,
      List<String> origIds,
      List<String> forkIds,
      List<Integer> startSteps,
      List<@Nullable String> forkAppNames)
      throws SQLException {

    StringJoiner valueRows = new StringJoiner(", ");
    for (int i = 0; i < origIds.size(); i++) {
      valueRows.add("(?::text, ?::text, ?::int, ?::text)");
    }
    String mappingCTE =
        "WITH mapping(orig_id, fork_id, start_step, app_name) AS (VALUES " + valueRows + ")\n";

    String ooSql =
        mappingCTE
            + """
              INSERT INTO "%1$s".operation_outputs
                (workflow_uuid, function_id, output, error, function_name,
                 child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization,
                 application_name)
              SELECT m.fork_id, oo.function_id, oo.output, oo.error, oo.function_name,
                     oo.child_workflow_id, oo.started_at_epoch_ms, oo.completed_at_epoch_ms,
                     oo.serialization, m.app_name
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
          // The copied steps take the same app_name as the forked workflow row itself.
          stmt.setString(p++, forkAppNames.get(i));
        }
        stmt.executeUpdate();
      }
    }
  }

  // Deleting a status row cannot be relied on to cascade its steps away: shared migration 112
  // drops the operation_outputs -> workflow_status foreign key, and any SDK sharing this system
  // database may already have applied it even though this ladder stops at 111. workflow_input and
  // workflow_output (migration 109) never had a foreign key at all. Every delete path clears all
  // three by ID instead.
  private static void deleteWorkflowChildRows(Connection conn, String schema, String[] workflowIds)
      throws SQLException {
    if (workflowIds.length == 0) {
      return;
    }
    for (var table : List.of("operation_outputs", "workflow_input", "workflow_output")) {
      var sql = "DELETE FROM \"%s\".%s WHERE workflow_uuid = ANY(?)".formatted(schema, table);
      try (var stmt = conn.prepareStatement(sql)) {
        var array = conn.createArrayOf("text", workflowIds);
        try {
          stmt.setArray(1, array);
          stmt.executeUpdate();
        } finally {
          array.free();
        }
      }
    }
  }

  private static Instant getRowsCutoff(DbContext ctx, Connection conn, long rowsThreshold)
      throws SQLException {
    String sql =
        """
          SELECT completed_at FROM "%s".workflow_status
          WHERE completed_at IS NOT NULL
          ORDER BY completed_at DESC OFFSET ? LIMIT 1
        """
            .formatted(ctx.schema());
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setLong(1, rowsThreshold - 1);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return Instant.ofEpochMilli(rs.getLong("completed_at"));
        }
      }
    }

    return null;
  }

  /** The child tables a retention round reclaims, in the order Python sweeps them. */
  private static final List<String> PAYLOAD_TABLES =
      List.of("workflow_input", "workflow_output", "operation_outputs");

  /**
   * The status rows a retention round is allowed to take, minus its watermark bounds.
   *
   * <p>completed_at is set on every terminal transition and cleared on resume, so one predicate
   * covers eligibility: in-flight rows hold NULL and never compare true. The round is system-wide,
   * not scoped to this application: retention policies apply to the whole system database even when
   * several applications share it.
   */
  private static final String STATUS_GC_FILTER = "completed_at < ?";

  /** Binds {@link #STATUS_GC_FILTER}'s parameters from index 1, returning the next free index. */
  private static int bindStatusGcFilter(PreparedStatement stmt, long deadline) throws SQLException {
    stmt.setLong(1, deadline);
    return 2;
  }

  /**
   * Runs one retention round: the status sweep, then the payload sweep that reclaims what it
   * orphaned. Does nothing when another round already holds the lock.
   */
  public static void runRetentionRound(
      DbContext ctx, Instant cutoff, Long rowsThreshold, int batchSize) throws SQLException {
    try (var lock = acquireRetentionLock(ctx)) {
      if (lock == null) {
        logger.warn(
            "Skipping retention: another round is already running against this system database.");
        return;
      }
      var used = garbageCollect(ctx, cutoff, rowsThreshold, batchSize);
      if (used == null) {
        return;
      }
      // Strictly after the status sweep: the payload sweep only takes orphans, so this round's
      // are only visible to it once that sweep has committed.
      garbageCollectPayloads(ctx, used, batchSize);
    }
  }

  /**
   * The advisory lock key guarding one schema's retention rounds. Every SDK derives it this way --
   * the leading 8 bytes of SHA-256, big-endian signed -- so rounds in different languages against
   * one system database contend for the same lock.
   */
  public static long retentionLockKey(String schema) {
    try {
      var digest =
          MessageDigest.getInstance("SHA-256")
              .digest(("dbos.retention." + schema).getBytes(StandardCharsets.UTF_8));
      return ByteBuffer.wrap(digest, 0, Long.BYTES).getLong();
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("SHA-256 is unavailable", e);
    }
  }

  /** A held retention lock. Closing it releases the lock and the session holding it. */
  public static final class RetentionLock implements AutoCloseable {
    private final String schema;
    private final @Nullable Connection conn;

    private RetentionLock(String schema, @Nullable Connection conn) {
      this.schema = schema;
      this.conn = conn;
    }

    @Override
    public void close() throws SQLException {
      if (conn == null) {
        return;
      }
      try (conn) {
        // Explicit, since closing only returns the session to the pool.
        try (var stmt = conn.prepareStatement("SELECT pg_advisory_unlock(?)")) {
          stmt.setLong(1, retentionLockKey(schema));
          try (var rs = stmt.executeQuery()) {
            if (rs.next() && !rs.getBoolean(1)) {
              // False means this session no longer holds it, which a transaction-pooling proxy
              // causes by switching backends.
              logger.warn(
                  "Could not release the retention lock: this session no longer holds it. Retention"
                      + " will not proceed until the lock is released, which happens when the"
                      + " holding backend closes. A transaction-pooling proxy in front of Postgres"
                      + " causes this; run DBOS through a session-pooled or direct connection.");
            }
          }
        }
      }
    }
  }

  /**
   * Takes a database-wide lock for one retention round, or returns null when another round already
   * holds it. The lock is session-scoped, so a round that crashes releases it. CockroachDB has no
   * advisory locks and always takes it, so it collects unprotected rather than not at all.
   */
  public static @Nullable RetentionLock acquireRetentionLock(DbContext ctx) throws SQLException {
    // The round holds this connection until it ends: returning it would drop the lock.
    var conn = ctx.getConnection();
    try {
      if (SystemDatabase.isCockroach(conn)) {
        conn.close();
        return new RetentionLock(ctx.schema(), null);
      }
      // Autocommit keeps the session clear of idle-in-transaction timeouts.
      conn.setAutoCommit(true);
      try (var stmt = conn.prepareStatement("SELECT pg_try_advisory_lock(?)")) {
        stmt.setLong(1, retentionLockKey(ctx.schema()));
        try (var rs = stmt.executeQuery()) {
          if (!rs.next() || !rs.getBoolean(1)) {
            conn.close();
            return null;
          }
        }
      }
    } catch (SQLException e) {
      try {
        conn.close();
      } catch (SQLException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw e;
    }
    return new RetentionLock(ctx.schema(), conn);
  }

  /**
   * Deletes old terminal workflows throughout the system database, returning the cutoff actually
   * used, or null when there is nothing to collect.
   *
   * <p>The sweep advances a {@code completed_at} watermark, committing one batch per transaction;
   * it never materializes workflow ids, so its memory cost is flat however much it collects. Call
   * {@link #garbageCollectPayloads} afterwards to reclaim the rows it orphaned.
   */
  public static @Nullable Instant garbageCollect(
      DbContext ctx, Instant cutoff, Long rowsThreshold, int batchSize) throws SQLException {
    if (batchSize < 1) {
      throw new IllegalArgumentException("batchSize must be a positive integer, got " + batchSize);
    }

    try (var conn = ctx.getConnection()) {
      if (rowsThreshold != null) {
        var rowsCutoff = getRowsCutoff(ctx, conn, rowsThreshold);
        if (rowsCutoff != null) {
          if (cutoff == null || rowsCutoff.isAfter(cutoff)) {
            cutoff = rowsCutoff;
          }
        }
      }

      if (cutoff == null) {
        return null;
      }

      sweepWorkflowStatus(ctx, conn, cutoff.toEpochMilli(), batchSize);
      return cutoff;
    }
  }

  /** Deletes eligible status rows in batches, seeded from the oldest one in range. */
  private static void sweepWorkflowStatus(
      DbContext ctx, Connection conn, long deadline, int batchSize) throws SQLException {
    var seedSql =
        "SELECT completed_at FROM \"%s\".workflow_status WHERE %s ORDER BY completed_at LIMIT 1"
            .formatted(ctx.schema(), STATUS_GC_FILTER);

    Long oldest;
    try (var stmt = conn.prepareStatement(seedSql)) {
      bindStatusGcFilter(stmt, deadline);
      try (var rs = stmt.executeQuery()) {
        oldest = rs.next() ? rs.getLong(1) : null;
      }
    }
    if (oldest == null) {
      return;
    }

    var watermark = oldest - 1;
    while (true) {
      var next = deleteStatusBatch(ctx, conn, deadline, watermark, batchSize);
      if (next == null) {
        return;
      }
      watermark = next;
    }
  }

  /**
   * Deletes one batch of status rows in its own transaction.
   *
   * @return the watermark to resume from, or null when the sweep is done
   */
  private static @Nullable Long deleteStatusBatch(
      DbContext ctx, Connection conn, long deadline, long watermark, int batchSize)
      throws SQLException {
    var stepSql =
        ("SELECT completed_at FROM \"%s\".workflow_status WHERE %s AND completed_at > ?"
                + " ORDER BY completed_at LIMIT 1 OFFSET ?")
            .formatted(ctx.schema(), STATUS_GC_FILTER);
    var boundedSql =
        "DELETE FROM \"%s\".workflow_status WHERE %s AND completed_at > ? AND completed_at <= ?"
            .formatted(ctx.schema(), STATUS_GC_FILTER);
    var remainderSql =
        "DELETE FROM \"%s\".workflow_status WHERE %s".formatted(ctx.schema(), STATUS_GC_FILTER);

    return SqlTransaction.call(
        conn,
        c -> {
          // The completed_at of the batchSize-th oldest eligible row above the watermark.
          Long step = null;
          try (var stmt = c.prepareStatement(stepSql)) {
            var next = bindStatusGcFilter(stmt, deadline);
            stmt.setLong(next, watermark);
            stmt.setInt(next + 1, batchSize - 1);
            try (var rs = stmt.executeQuery()) {
              if (rs.next()) {
                step = rs.getLong(1);
              }
            }
          }

          if (step == null) {
            // Unbounded, and deliberately not limited to rows above the watermark: an import can
            // land a completed_at below it mid-pass.
            try (var stmt = c.prepareStatement(remainderSql)) {
              bindStatusGcFilter(stmt, deadline);
              stmt.executeUpdate();
            }
            return null;
          }

          try (var stmt = c.prepareStatement(boundedSql)) {
            var next = bindStatusGcFilter(stmt, deadline);
            stmt.setLong(next, watermark);
            // completed_at ties may push the batch slightly over batchSize.
            stmt.setLong(next + 1, step);
            stmt.executeUpdate();
          }
          return step;
        });
  }

  /**
   * Deletes payload and step rows below the cutoff whose workflow is gone, returning the count
   * removed from each table in {@link #PAYLOAD_TABLES} order. Runs after the status sweep, whose
   * orphans all fall in range: every payload is stamped no later than the completion that made its
   * workflow collectable.
   */
  public static long[] garbageCollectPayloads(DbContext ctx, Instant cutoff, int batchSize)
      throws SQLException {
    if (batchSize < 1) {
      throw new IllegalArgumentException("batchSize must be a positive integer, got " + batchSize);
    }
    var deadline = cutoff.toEpochMilli();

    // To optimize performance, vacuum payload tables both before and after collecting them.
    var toVacuum = new ArrayList<String>();
    toVacuum.add("workflow_status");
    toVacuum.addAll(PAYLOAD_TABLES);
    vacuumTables(ctx, toVacuum);

    var deleted = new long[PAYLOAD_TABLES.size()];
    var failures = new ArrayList<SQLException>();
    sweepPayloadsConcurrently(ctx, deadline, batchSize, deleted, failures);

    // Only the first can be thrown, so the rest would otherwise be lost.
    for (var extra : failures.subList(Math.min(1, failures.size()), failures.size())) {
      logger.warn("Payload retention sweep also failed", extra);
    }
    if (!failures.isEmpty()) {
      throw failures.get(0);
    }

    vacuumTables(ctx, PAYLOAD_TABLES);
    logger.debug(
        "Payload retention deleted {} inputs, {} outputs, and {} steps",
        deleted[0],
        deleted[1],
        deleted[2]);
    return deleted;
  }

  /**
   * Sweeps the payload tables in parallel, one connection per sweep.
   *
   * <p>A pool too small for all three runs them sequentially rather than leaving sweeps waiting on
   * a connection that only the round itself would free: the retention lock already holds one for
   * the round's whole duration.
   */
  private static void sweepPayloadsConcurrently(
      DbContext ctx, long deadline, int batchSize, long[] deleted, List<SQLException> failures) {
    var poolMax =
        ctx.dataSource() instanceof HikariDataSource hikari
            ? hikari.getMaximumPoolSize()
            : PAYLOAD_TABLES.size() + 1;
    var concurrency = Math.max(1, Math.min(PAYLOAD_TABLES.size(), poolMax - 1));

    var pool =
        Executors.newFixedThreadPool(
            concurrency,
            runnable -> {
              var thread = new Thread(runnable, "dbos-gc-payload");
              thread.setDaemon(true);
              return thread;
            });
    try {
      var futures = new ArrayList<Future<Long>>(PAYLOAD_TABLES.size());
      for (var table : PAYLOAD_TABLES) {
        futures.add(
            pool.submit(
                () -> {
                  try (var conn = ctx.getConnection()) {
                    return sweepPayloadTable(ctx, conn, table, deadline, batchSize);
                  }
                }));
      }
      for (int i = 0; i < futures.size(); i++) {
        try {
          deleted[i] = futures.get(i).get();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          failures.add(new SQLException("Payload retention sweep interrupted", e));
        } catch (ExecutionException e) {
          var cause = e.getCause();
          failures.add(
              cause instanceof SQLException sqlCause
                  ? sqlCause
                  : new SQLException("Payload retention sweep failed", cause));
        }
      }
    } finally {
      pool.shutdownNow();
    }
  }

  /**
   * VACUUMs the tables a sweep is about to dirty, or just dirtied. No-op on CockroachDB, where
   * there is no autovacuum to outrun.
   */
  private static void vacuumTables(DbContext ctx, List<String> tables) throws SQLException {
    try (var conn = ctx.getConnection()) {
      if (SystemDatabase.isCockroach(conn)) {
        return;
      }
      // VACUUM cannot run inside a transaction block.
      conn.setAutoCommit(true);
      for (var table : tables) {
        // Per table, so one refusal does not skip the rest.
        try (var stmt = conn.createStatement()) {
          stmt.execute(
              "VACUUM (INDEX_CLEANUP ON, TRUNCATE OFF, ANALYZE) \"%s\".%s"
                  .formatted(ctx.schema(), table));
          // A refused or stalled VACUUM does not raise, it says so in a warning; a successful one
          // is silent, so anything here is worth surfacing.
          for (var w = stmt.getWarnings(); w != null; w = w.getNextWarning()) {
            logger.warn("Payload retention vacuuming {}: {}", table, w.getMessage());
          }
        } catch (SQLException e) {
          logger.warn("Payload retention could not vacuum {}: {}", table, e.getMessage());
        }
      }
    }
  }

  /** Deletes one payload table's orphans below the cutoff, returning how many it removed. */
  private static long sweepPayloadTable(
      DbContext ctx, Connection conn, String table, long deadline, int batchSize)
      throws SQLException {
    // A payload below the cutoff belongs to a workflow created before it, so the status side of
    // this anti-join is the few such rows still present, not the whole table.
    var orphaned =
        (" AND NOT EXISTS (SELECT 1 FROM \"%s\".workflow_status ws"
                + " WHERE ws.workflow_uuid = %s.workflow_uuid AND ws.created_at < ?)")
            .formatted(ctx.schema(), table);
    var seedSql =
        ("SELECT retention_timestamp FROM \"%s\".%s WHERE retention_timestamp < ?"
                + " ORDER BY retention_timestamp LIMIT 1")
            .formatted(ctx.schema(), table);

    Long oldest;
    try (var stmt = conn.prepareStatement(seedSql)) {
      stmt.setLong(1, deadline);
      try (var rs = stmt.executeQuery()) {
        oldest = rs.next() ? rs.getLong(1) : null;
      }
    }
    if (oldest == null) {
      return 0;
    }

    var stepSql =
        ("SELECT retention_timestamp FROM \"%s\".%s"
                + " WHERE retention_timestamp < ? AND retention_timestamp > ?"
                + " ORDER BY retention_timestamp LIMIT 1 OFFSET ?")
            .formatted(ctx.schema(), table);
    var deleteSql =
        "DELETE FROM \"%s\".%s WHERE retention_timestamp < ? AND retention_timestamp > ?"
            .formatted(ctx.schema(), table);

    var deleted = 0L;
    var watermark = oldest - 1;
    while (true) {
      final long from = watermark;
      var batch =
          SqlTransaction.<PayloadBatch>call(
              conn,
              c -> {
                // Batches are cut by candidate count, so rows spared by the anti-join only thin
                // one out; they are re-checked on the next round.
                Long step = null;
                try (var stmt = c.prepareStatement(stepSql)) {
                  stmt.setLong(1, deadline);
                  stmt.setLong(2, from);
                  stmt.setInt(3, batchSize - 1);
                  try (var rs = stmt.executeQuery()) {
                    if (rs.next()) {
                      step = rs.getLong(1);
                    }
                  }
                }

                // retention_timestamp ties may push the batch slightly over batchSize.
                var bounded = step != null ? " AND retention_timestamp <= ?" : "";
                try (var stmt = c.prepareStatement(deleteSql + bounded + orphaned)) {
                  stmt.setLong(1, deadline);
                  stmt.setLong(2, from);
                  var index = 3;
                  if (step != null) {
                    stmt.setLong(index++, step);
                  }
                  stmt.setLong(index, deadline);
                  return new PayloadBatch(step, stmt.executeUpdate());
                }
              });
      deleted += batch.deleted();
      if (batch.step() == null) {
        return deleted;
      }
      watermark = batch.step();
    }
  }

  /** One payload batch's outcome: the watermark to resume from, and how many rows it took. */
  private record PayloadBatch(@Nullable Long step, int deleted) {}

  /**
   * @param applicationName count only workflows and steps owned by these applications, plus
   *     unclaimed ones. Null counts this application's own; an explicitly empty list covers every
   *     application's.
   */
  public static List<MetricData> getMetrics(
      DbContext ctx, Instant startTime, Instant endTime, @Nullable List<String> applicationName)
      throws SQLException {
    final var start = Objects.requireNonNull(startTime).toEpochMilli();
    final var end = Objects.requireNonNull(endTime).toEpochMilli();
    logger.debug("getMetrics {} {}", start, end);
    List<MetricData> metrics = new ArrayList<>();
    final var names = ctx.scopeNames(applicationName);
    final var scope =
        names == null ? "" : " AND (application_name = ANY(?) OR application_name IS NULL)";
    final var wfSQL =
        """
          SELECT name, COUNT(workflow_uuid) as count
          FROM "%s".workflow_status
          WHERE created_at >= ? AND created_at < ?
        """
                .formatted(ctx.schema())
            + scope
            + " GROUP BY name";
    final var stepSQL =
        """
          SELECT function_name, COUNT(*) as count
          FROM "%s".operation_outputs
          WHERE completed_at_epoch_ms >= ? AND completed_at_epoch_ms < ?
        """
                .formatted(ctx.schema())
            + scope
            + " GROUP BY function_name";

    try (var conn = ctx.getConnection();
        var ps1 = conn.prepareStatement(wfSQL);
        var ps2 = conn.prepareStatement(stepSQL)) {

      Array namesArray = names == null ? null : conn.createArrayOf("text", names.toArray());

      ps1.setLong(1, start);
      ps1.setLong(2, end);
      if (namesArray != null) {
        ps1.setArray(3, namesArray);
      }

      try (var rs = ps1.executeQuery()) {
        while (rs.next()) {
          var name = rs.getString("name");
          var count = rs.getInt("count");
          metrics.add(new MetricData("workflow_count", name, count));
        }
      }

      ps2.setLong(1, start);
      ps2.setLong(2, end);
      if (namesArray != null) {
        ps2.setArray(3, namesArray);
      }

      try (var rs = ps2.executeQuery()) {
        while (rs.next()) {
          var name = rs.getString("function_name");
          var count = rs.getInt("count");
          metrics.add(new MetricData("step_count", name, count));
        }
      } finally {
        if (namesArray != null) {
          namesArray.free();
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

  /**
   * Refuse to move a workflow whose payloads this runtime cannot handle.
   *
   * <p>Export and import round-trip the payloads through this runtime's serializers, so neither can
   * settle for the null a status read reports: a payload dropped on the way through restores a
   * workflow that never had it.
   */
  private static void requireSerializerFor(
      String action,
      String workflowId,
      String workflowSerialization,
      List<StepInfo> steps,
      DBOSSerializer serializer) {
    var formats = new LinkedHashSet<String>();
    if (!SerializationUtil.canDeserialize(workflowSerialization, serializer)) {
      formats.add(workflowSerialization);
    }
    for (var step : steps) {
      if (!SerializationUtil.canDeserialize(step.serialization(), serializer)) {
        formats.add(step.serialization());
      }
    }
    if (!formats.isEmpty()) {
      throw new IllegalStateException(
          "Cannot %s workflow %s: it is serialized as %s, which this application has no serializer for"
              .formatted(action, workflowId, String.join(", ", formats)));
    }
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
        if (status != null) {
          requireSerializerFor("export", wfid, status.serialization(), steps, ctx.serializer());
        }
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
    // The whole batch, before anything is written: export and import are a single-SDK affair but
    // not a single-configuration one, and a payload we cannot re-serialize imports empty.
    for (var workflow : workflows) {
      var s = workflow.status();
      requireSerializerFor(
          "import", s.workflowId(), s.serialization(), workflow.steps(), serializer);
    }

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
          delay_until_epoch_ms, completed_at, application_name
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        )
        """
            .formatted(ctx.schema());

    var stepSQL =
        """
        INSERT INTO "%s".operation_outputs (
          workflow_uuid, function_id, function_name,
          output, error, child_workflow_id,
          started_at_epoch_ms, completed_at_epoch_ms,
          serialization, application_name
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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

    try (var txConn = ctx.getConnection()) {
      SqlTransaction.run(
          txConn,
          conn -> {
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
                wfStmt.setString(30, status.applicationName());
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
                  stepStmt.setString(
                      5, step.error() == null ? null : step.error().serializedError());
                  stepStmt.setString(6, step.childWorkflowId());
                  stepStmt.setObject(7, step.startedAtEpochMs());
                  stepStmt.setObject(8, step.completedAtEpochMs());
                  stepStmt.setString(9, step.serialization());
                  // A step keeps exactly the app_name it was exported with. An export that predates
                  // the
                  // column carries no app_name, so its steps import unclaimed rather than
                  // inheriting a
                  // guess from the workflow -- the same choice Python and TypeScript make.
                  stepStmt.setString(10, step.applicationName());
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
            }
          });
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
