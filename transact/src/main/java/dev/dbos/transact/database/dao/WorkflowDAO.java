package dev.dbos.transact.database.dao;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.DatabaseTime;
import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.database.DebounceCaller;
import dev.dbos.transact.database.MetricData;
import dev.dbos.transact.database.Result;
import dev.dbos.transact.database.SqlTransaction;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.database.TimedWorkflowStatus;
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
import dev.dbos.transact.workflow.DebounceResult;
import dev.dbos.transact.workflow.Debouncer.DebounceIds;
import dev.dbos.transact.workflow.DeduplicationHolder;
import dev.dbos.transact.workflow.ErrorResult;
import dev.dbos.transact.workflow.ExportedWorkflow;
import dev.dbos.transact.workflow.ForkFromFailureOptions;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.GetStepAggregatesInput;
import dev.dbos.transact.workflow.GetWorkflowAggregatesInput;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.RewindOptions;
import dev.dbos.transact.workflow.StepAggregateRow;
import dev.dbos.transact.workflow.WorkflowAggregateRow;
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
import java.sql.Types;
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
        workflow_status.workflow_uuid, status,
        name, class_name, config_name,
        queue_name, deduplication_id, priority, queue_partition_key, delay_until_epoch_ms,
        executor_id, application_version, application_id,
        authenticated_user, assumed_role, authenticated_roles,
        created_at, updated_at, completed_at, started_at_epoch_ms,
        recovery_attempts, workflow_timeout_ms, workflow_deadline_epoch_ms,
        forked_from, parent_workflow_id, was_forked_from, attributes, schedule_name,
        application_name, is_debounced, debounce_deadline_epoch_ms
      """;

  // Payloads moved off workflow_status in migration 109, so a status update no longer rewrites a
  // large payload. Every SDK writes workflow_input and workflow_output and leaves the legacy
  // columns null, but rows written before the move still carry them, so every read prefers the new
  // table and falls back to the column.
  //
  // One LEFT JOIN per payload table, as in Python and TypeScript: the join is on the payload
  // table's primary key, so it probes once per row however many of that table's columns the
  // COALESCE list reads. The joins make workflow_uuid ambiguous, so every query below that takes
  // one qualifies its own references.
  private static final String INPUTS_COLUMN =
      "COALESCE(wi.inputs, workflow_status.inputs) AS inputs";

  private static String inputsJoin(String schema) {
    return "LEFT JOIN \"%s\".workflow_input wi ON wi.workflow_uuid = workflow_status.workflow_uuid"
        .formatted(schema);
  }

  private static final String OUTPUT_COLUMNS =
      "COALESCE(wo.output, workflow_status.output) AS output,"
          + " COALESCE(wo.error, workflow_status.error) AS error";

  private static String outputJoin(String schema) {
    return "LEFT JOIN \"%s\".workflow_output wo ON wo.workflow_uuid = workflow_status.workflow_uuid"
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
      @Nullable Long delayUntilEpochMs,
      @Nullable Integer maxRetries,
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
                delayUntilEpochMs,
                ownerXid,
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

        // Only the first writer of a row owns its execution. A caller that finds someone else's
        // row polls for the outcome instead, and one that finds a dead-lettered row is told so.
        if (!ownerXid.equals(resRow.ownerXid)) {
          if (resRow.status == WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED) {
            throw new DBOSMaxRecoveryAttemptsExceededException(
                initStatus.workflowId(),
                Objects.requireNonNullElse(maxRetries, Constants.DEFAULT_MAX_RECOVERY_ATTEMPTS));
          }
          return new WorkflowInitResult(
              state, resRow.deadline(), false, resRow.serialization(), resRow.clock());
        }

        shouldCommit = true;

        return new WorkflowInitResult(
            state, resRow.deadline(), true, resRow.serialization(), resRow.clock());

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

  /**
   * Moves claimed workflows that have exhausted their attempts off the queue.
   *
   * <p>Guarded on PENDING like every other claim-owning write, and on the attempt count the
   * decision was read from: a row another executor has already moved on, or one given a fresh
   * budget by resume, is left alone.
   */
  public static void deadLetterWorkflows(
      DbContext ctx, List<String> workflowIds, int minRecoveryAttempts) throws SQLException {

    final String sql =
        """
          UPDATE "%1$s".workflow_status
          SET status = ?, deduplication_id = NULL, started_at_epoch_ms = NULL, queue_name = NULL,
              updated_at = %2$s, completed_at = %2$s
          WHERE workflow_uuid = ANY(?) AND status = ? AND recovery_attempts >= ?
        """
            .formatted(ctx.schema(), SystemDatabase.NOW_EPOCH_MS);

    try (Connection conn = ctx.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED.name());
      stmt.setArray(2, conn.createArrayOf("text", workflowIds.toArray()));
      stmt.setString(3, WorkflowState.PENDING.name());
      stmt.setInt(4, minRecoveryAttempts);
      stmt.executeUpdate();
    }
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
      String ownerXid,
      DatabaseTime clock) {}

  /**
   * Insert into the workflow_status table
   *
   * <p>A workflow started directly with a timeout and no deadline gets its deadline that long after
   * the insert, on the database's clock. A queued workflow's timeout starts at its claim instead.
   *
   * @param status WorkflowStatusInternal holds the data for a workflow_status row
   * @param delayUntilEpochMs the absolute end of the status's delay, resolved by the caller, or
   *     null when it has none
   * @return InsertWorkflowResult some of the column inserted, and the database's clock
   * @throws SQLException
   */
  static InsertWorkflowResult insertWorkflowStatus(
      Connection conn,
      String schema,
      WorkflowStatusInternal status,
      @Nullable Long delayUntilEpochMs,
      String ownerXid,
      @Nullable String appName)
      throws SQLException {

    logger.debug("insertWorkflowStatus workflowId {}", status.workflowId());

    String insertSQL =
        """
          INSERT INTO "%1$s".workflow_status (
            workflow_uuid, status,
            name, class_name, config_name,
            queue_name, deduplication_id, priority, queue_partition_key, delay_until_epoch_ms,
            authenticated_user, assumed_role, authenticated_roles,
            executor_id, application_version, application_id,
            created_at, updated_at, recovery_attempts,
            workflow_timeout_ms, workflow_deadline_epoch_ms,
            parent_workflow_id, owner_xid, serialization, attributes, schedule_name,
            application_name, is_debounced, debounce_deadline_epoch_ms
          ) VALUES (
            ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            %2$s, %2$s, ?,
            ?, COALESCE(?::bigint, %2$s + ?::bigint),
            ?, ?, ?, ?::jsonb, ?,
            ?, ?, ?
          )
          ON CONFLICT (workflow_uuid)
            DO UPDATE SET
              -- recovery_attempts is absent by design: only the queue's claim counts a dispatch.
              updated_at = EXCLUDED.updated_at,
              executor_id = CASE
                  WHEN EXCLUDED.status != 'ENQUEUED' AND EXCLUDED.status != 'DELAYED'
                  THEN EXCLUDED.executor_id
                  ELSE workflow_status.executor_id
              END,
              application_name = COALESCE(workflow_status.application_name, EXCLUDED.application_name)
          RETURNING recovery_attempts, status, name, class_name, config_name, queue_name, workflow_deadline_epoch_ms, owner_xid, serialization, %2$s AS db_now
        """
            .formatted(schema, SystemDatabase.NOW_EPOCH_MS);

    Objects.requireNonNull(status, "status must not be null");
    Objects.requireNonNull(status.workflowId(), "workflowId must not be null");
    var state =
        status.queueName() == null
            ? WorkflowState.PENDING
            : delayUntilEpochMs == null ? WorkflowState.ENQUEUED : WorkflowState.DELAYED;
    var recoveryAttempts =
        state == WorkflowState.ENQUEUED || state == WorkflowState.DELAYED ? 0 : 1;

    var authenticatedRolesJson =
        status.authenticatedRoles() != null
            ? JsonUtility.toJson(status.authenticatedRoles())
            : null;
    var attributesJson = attributesToJson(status.attributes());
    // Only a directly started workflow's timeout runs from the insert.
    Long deadlineTimeoutMs =
        status.queueName() == null && status.deadlineEpochMs() == null ? status.timeoutMs() : null;
    try (var stmt = conn.prepareStatement(insertSQL)) {

      stmt.setString(1, status.workflowId());
      stmt.setString(2, state.name());
      stmt.setString(3, status.workflowName());
      stmt.setString(4, status.className());
      stmt.setString(5, status.instanceName());

      stmt.setString(6, status.queueName());
      stmt.setString(7, status.deduplicationId());
      stmt.setInt(8, Objects.requireNonNullElse(status.priority(), 0));
      stmt.setString(9, status.queuePartitionKey());
      stmt.setObject(10, delayUntilEpochMs);

      stmt.setString(11, status.authenticatedUser());
      stmt.setString(12, status.assumedRole());
      stmt.setString(13, authenticatedRolesJson);

      stmt.setString(14, status.executorId());
      stmt.setString(15, status.appVersion());
      stmt.setString(16, status.appId());

      stmt.setInt(17, recoveryAttempts);

      stmt.setObject(18, status.timeoutMs(), Types.BIGINT);
      stmt.setObject(19, status.deadlineEpochMs(), Types.BIGINT);
      stmt.setObject(20, deadlineTimeoutMs, Types.BIGINT);
      stmt.setString(21, status.parentWorkflowId());

      stmt.setObject(22, ownerXid);
      stmt.setString(23, status.serialization());
      stmt.setString(24, attributesJson);
      stmt.setString(25, status.scheduleName());
      stmt.setString(26, appName);
      stmt.setBoolean(27, status.isDebounced());
      stmt.setObject(28, status.debounceDeadlineEpochMs(), Types.BIGINT);

      InsertWorkflowResult result;
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          result =
              new InsertWorkflowResult(
                  rs.getInt("recovery_attempts"),
                  WorkflowState.valueOf(rs.getString("status")),
                  rs.getString("name"),
                  rs.getString("class_name"),
                  rs.getString("config_name"),
                  rs.getString("queue_name"),
                  SystemDatabase.toInstant(rs.getObject("workflow_deadline_epoch_ms", Long.class)),
                  rs.getString("serialization"),
                  rs.getString("owner_xid"),
                  DatabaseTime.read(rs, "db_now"));
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

      // Only the call that created the status row writes its input. A row that was already there
      // keeps the input it was written with, including one that keeps it in workflow_status.inputs,
      // which a payload row here would override on every read. Checked here rather than left to the
      // callers' rollback: not every caller rolls back (recordErrorForUnstartedWorkflow commits
      // regardless), and a repeat start then skips a statement it would only have discarded.
      if (!ownerXid.equals(result.ownerXid())) {
        return result;
      }

      // Two statements rather than one data-modifying CTE: at scale the CTE costs more than the
      // round trip it saves.
      //
      // An upsert, because this call created the status row, so any input already filed under the
      // ID is not this workflow's. Retention deletes status rows before it sweeps their payloads,
      // so a workflow started under a reused ID in between would otherwise run with the previous
      // workflow's input, and then lose it to the payload sweep. A retried commit that did land
      // the first time rewrites the same input, stamped later than the created_at it kept.
      //
      // retention_timestamp takes the column default, the same now() as the status row's
      // created_at: both statements share a transaction. The payload sweep treats a payload below
      // the cutoff as an orphan unless its status row was also created before it, which holds only
      // if the input is never stamped earlier than created_at.
      var inputSQL =
          """
            INSERT INTO "%s".workflow_input (workflow_uuid, inputs)
            VALUES (?, ?)
            ON CONFLICT (workflow_uuid)
              DO UPDATE SET inputs = EXCLUDED.inputs, retention_timestamp = EXCLUDED.retention_timestamp
          """
              .formatted(schema);
      try (var inputStmt = conn.prepareStatement(inputSQL)) {
        inputStmt.setString(1, status.workflowId());
        inputStmt.setString(2, status.inputs());
        inputStmt.executeUpdate();
      }

      return result;
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
  private static boolean updateWorkflowOutcome(
      Connection conn,
      String schema,
      String workflowId,
      WorkflowState status,
      String output,
      String error)
      throws SQLException {

    logger.debug("updateWorkflowOutcome wfid {} status {}", workflowId, status);

    // Only the outcomes that carry a payload. Cancellation goes through its own paths, and here it
    // would write an empty workflow_output row.
    if (status != WorkflowState.SUCCESS && status != WorkflowState.ERROR) {
      throw new IllegalArgumentException(
          "updateWorkflowOutcome records SUCCESS or ERROR, not " + status);
    }

    var sql =
        """
          UPDATE "%1$s".workflow_status
          SET status = ?, updated_at = %2$s, completed_at = %2$s, deduplication_id = NULL
          WHERE workflow_uuid = ? AND status = ?
        """
            .formatted(schema, SystemDatabase.NOW_EPOCH_MS);

    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, status.name());
      stmt.setString(2, workflowId);
      stmt.setString(3, WorkflowState.PENDING.name());

      if (stmt.executeUpdate() == 0) {
        // The outcome was not ours to write, so leave no orphan payload.
        return false;
      }
    }

    // The payload follows the status transition it belongs to, so both must land together: the
    // caller runs them in one transaction. A row already here belongs to an earlier workflow under
    // the same ID whose payloads retention had not yet swept, so its retention_timestamp is reset
    // along with the payload; kept, it would get this workflow's output swept as an orphan.
    var outputSQL =
        """
          INSERT INTO "%s".workflow_output (workflow_uuid, output, error)
          VALUES (?, ?, ?)
          ON CONFLICT (workflow_uuid)
            DO UPDATE SET output = EXCLUDED.output, error = EXCLUDED.error,
                          retention_timestamp = EXCLUDED.retention_timestamp
        """
            .formatted(schema);
    try (var stmt = conn.prepareStatement(outputSQL)) {
      stmt.setString(1, workflowId);
      stmt.setString(2, output);
      stmt.setString(3, error);
      stmt.executeUpdate();
    }
    return true;
  }

  private static boolean updateWorkflowOutcome(
      DbContext ctx, String workflowId, WorkflowState state, String output, String error)
      throws SQLException {

    try (var conn = ctx.getConnection()) {
      return SqlTransaction.call(
          conn, c -> updateWorkflowOutcome(c, ctx.schema(), workflowId, state, output, error));
    }
  }

  /**
   * Store the result to workflow_output, marking the workflow SUCCESS
   *
   * @param workflowId id of the workflow
   * @param result output serialized as json
   * @return true if the outcome was recorded, false if the row is no longer PENDING
   */
  public static boolean recordWorkflowOutput(DbContext ctx, String workflowId, String result)
      throws SQLException {

    return updateWorkflowOutcome(ctx, workflowId, WorkflowState.SUCCESS, result, null);
  }

  /**
   * Store the error to workflow_output, marking the workflow ERROR
   *
   * @param workflowId id of the workflow
   * @param error output serialized as json
   * @return true if the outcome was recorded, false if the row is no longer PENDING
   */
  public static boolean recordWorkflowError(DbContext ctx, String workflowId, String error)
      throws SQLException {

    return updateWorkflowOutcome(ctx, workflowId, WorkflowState.ERROR, null, error);
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
      DbContext ctx,
      WorkflowStatusInternal initStatus,
      @Nullable Long delayUntilEpochMs,
      String error)
      throws SQLException {

    // One transaction: the outcome payload is written only when the status transition lands, so
    // a crash between the two would leave an ERROR row with no error, which replaying the durable
    // debouncer workflow cannot repair -- the retry no longer finds the row PENDING.
    try (var conn = ctx.getConnection()) {
      SqlTransaction.run(
          conn,
          c -> {
            insertWorkflowStatus(
                c,
                ctx.schema(),
                initStatus,
                delayUntilEpochMs,
                UUID.randomUUID().toString(),
                owner(ctx, initStatus));
            updateWorkflowOutcome(
                c, ctx.schema(), initStatus.workflowId(), WorkflowState.ERROR, null, error);
          });
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
    var timedStatus = getTimedWorkflowStatus(conn, schema, serializer, workflowId);
    return timedStatus == null ? null : timedStatus.status();
  }

  public static @Nullable TimedWorkflowStatus getTimedWorkflowStatus(
      DbContext ctx, String workflowId) throws SQLException {
    try (var conn = ctx.getConnection()) {
      return getTimedWorkflowStatus(conn, ctx.schema(), ctx.serializer(), workflowId);
    }
  }

  private static @Nullable TimedWorkflowStatus getTimedWorkflowStatus(
      Connection conn, String schema, DBOSSerializer serializer, String workflowId)
      throws SQLException {
    if (Objects.requireNonNull(workflowId, "workflowId must not be null").isEmpty()) {
      throw new IllegalArgumentException("workflowId must not be empty");
    }

    try (var stmt = conn.prepareStatement(workflowStatusByIdSql(schema))) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          // Read the clock before deserializing the row, so the reading isn't stamped late by
          // the time that takes.
          var readAt = DatabaseTime.read(rs, "db_now");
          return new TimedWorkflowStatus(
              resultsToWorkflowStatus(rs, true, true, serializer), readAt);
        }
      }
    }

    return null;
  }

  /**
   * One workflow's status row with its payloads, read through both payload shapes, and the
   * database's clock as {@code db_now}.
   */
  private static String workflowStatusByIdSql(String schema) {
    return ("SELECT "
            + WORKFLOW_STATUS_COLUMNS
            + ", "
            + INPUTS_COLUMN
            + ", "
            + OUTPUT_COLUMNS
            + ", serialization, "
            + SystemDatabase.NOW_EPOCH_MS
            + " AS db_now")
        + " FROM \"%s\".workflow_status ".formatted(schema)
        + inputsJoin(schema)
        + " "
        + outputJoin(schema)
        + " WHERE workflow_status.workflow_uuid = ?";
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
   * application that owns it and what kind of workflow it is, or {@code null} if the pair is
   * unheld. Uses the UNIQUE index on that pair for O(1) lookup.
   *
   * <p>That index is global across the applications sharing the system database, so the holder is
   * not necessarily ours. The read is deliberately unscoped: a caller cannot steer around a holder
   * it cannot see, and the debouncer needs the owner precisely so it can refuse.
   */
  public static @Nullable DeduplicationHolder findDeduplicationHolder(
      DbContext ctx, String queueName, String deduplicationId) throws SQLException {
    try (var conn = ctx.getConnection()) {
      return findDeduplicationHolder(conn, ctx.schema(), queueName, deduplicationId);
    }
  }

  private static @Nullable DeduplicationHolder findDeduplicationHolder(
      Connection conn, String schema, String queueName, String deduplicationId)
      throws SQLException {
    var sql =
        """
          SELECT workflow_uuid, application_name, name, class_name, config_name, status,
                 is_debounced
            FROM "%s".workflow_status
           WHERE queue_name = ?
             AND deduplication_id = ?
           LIMIT 1
        """
            .formatted(schema);
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, queueName);
      stmt.setString(2, deduplicationId);
      try (var rs = stmt.executeQuery()) {
        return rs.next()
            ? new DeduplicationHolder(
                rs.getString("workflow_uuid"),
                rs.getString("application_name"),
                rs.getString("name"),
                rs.getString("class_name"),
                rs.getString("config_name"),
                WorkflowState.valueOf(rs.getString("status")),
                rs.getBoolean("is_debounced"))
            : null;
      }
    }
  }

  /**
   * Takes over a debouncer workflow that stopped answering, in one transaction: cancels it, which
   * frees its debounce key, and creates the user workflow it promised as {@code promised}, a
   * debounced workflow holding that key. The key passes straight from one to the other, so no other
   * call can take it in between and leave the promised workflow uncreated, which would strand every
   * handle to it. A call that tries waits on the transaction, then extends the promised workflow.
   *
   * <p>The debouncer workflow is locked first. One that is gone or no longer active is left alone:
   * it finished on its own, or another call took it over first. An active one is cancelled. The
   * promised workflow is then created unless it already exists -- the debouncer workflow started
   * it, before it went silent or, on a node that was only slow, while this call waited -- or {@code
   * promised} is null because its inputs named none.
   *
   * <p>Returns the promised workflow's id if this call created it, otherwise null. With a {@code
   * caller}, it is that caller's step: if the step already ran, what it recorded is returned and
   * nothing is touched; otherwise the takeover and its checkpoint commit together. The return is
   * {@code Object}, as a replay hands back whatever the serializer preserved.
   */
  public static Object takeOverDebouncerWorkflow(
      DbContext ctx,
      String debouncerWorkflowId,
      @Nullable WorkflowStatusInternal promised,
      @Nullable Long delayUntilEpochMs,
      String ownerXid,
      @Nullable DebounceCaller caller)
      throws SQLException {
    long startTime = System.currentTimeMillis();
    try (var conn = ctx.getConnection()) {
      return SqlTransaction.call(
          conn,
          c -> {
            if (caller != null) {
              var prev =
                  StepsDAO.checkStepResult(
                      c, ctx.schema(), caller.workflowId(), caller.stepId(), caller.stepName());
              if (prev != null) {
                return prev.toResult(ctx.serializer());
              }
            }
            String created =
                takeOver(ctx, c, debouncerWorkflowId, promised, delayUntilEpochMs, ownerXid);
            if (caller != null) {
              var serialized = SerializationUtil.serializeValue(created, null, ctx.serializer());
              StepsDAO.recordStepResult(
                  ctx,
                  c,
                  new StepResult(
                      caller.workflowId(),
                      caller.stepId(),
                      caller.stepName(),
                      serialized.serializedValue(),
                      null,
                      created,
                      serialized.serialization()),
                  startTime,
                  System.currentTimeMillis());
            }
            return created;
          });
    }
  }

  private static @Nullable String takeOver(
      DbContext ctx,
      Connection conn,
      String debouncerWorkflowId,
      @Nullable WorkflowStatusInternal promised,
      @Nullable Long delayUntilEpochMs,
      String ownerXid)
      throws SQLException {
    var lockSql =
        """
          SELECT status FROM "%s".workflow_status WHERE workflow_uuid = ? FOR UPDATE
        """
            .formatted(ctx.schema());
    WorkflowState status;
    try (var stmt = conn.prepareStatement(lockSql)) {
      stmt.setString(1, debouncerWorkflowId);
      try (var rs = stmt.executeQuery()) {
        if (!rs.next()) {
          return null;
        }
        status = WorkflowState.valueOf(rs.getString("status"));
      }
    }
    if (!status.isActive()) {
      return null;
    }
    // As cancelWorkflows does, without cascading: a child the debouncer workflow already started
    // is the promised workflow, and it runs.
    var cancelSql =
        """
          UPDATE "%1$s".workflow_status
          SET status = ?,
              queue_name = NULL,
              deduplication_id = NULL,
              started_at_epoch_ms = NULL,
              updated_at = %2$s,
              completed_at = %2$s
          WHERE workflow_uuid = ?
        """
            .formatted(ctx.schema(), SystemDatabase.NOW_EPOCH_MS);
    try (var stmt = conn.prepareStatement(cancelSql)) {
      stmt.setString(1, WorkflowState.CANCELLED.name());
      stmt.setString(2, debouncerWorkflowId);
      stmt.executeUpdate();
    }
    DebugTriggers.debugTriggerPoint(DebugTriggers.DEBUG_TRIGGER_DEBOUNCE_TAKEOVER);
    if (promised == null) {
      logger.warn(
          "Cancelled debouncer workflow {}, which stopped acknowledging calls and names no user"
              + " workflow",
          debouncerWorkflowId);
      return null;
    }
    // A row already there -- the promised workflow the debouncer workflow started -- keeps its
    // inputs: the insert writes them only for a row it creates.
    var inserted =
        insertWorkflowStatus(
            conn, ctx.schema(), promised, delayUntilEpochMs, ownerXid, owner(ctx, promised));
    if (!ownerXid.equals(inserted.ownerXid())) {
      logger.warn(
          "Cancelled debouncer workflow {}, which stopped acknowledging calls after starting its"
              + " user workflow {}",
          debouncerWorkflowId,
          promised.workflowId());
      return null;
    }
    logger.warn(
        "Cancelled debouncer workflow {}, which stopped acknowledging calls, and created its user"
            + " workflow {}",
        debouncerWorkflowId,
        promised.workflowId());
    return promised.workflowId();
  }

  /**
   * Extends a debounced DELAYED workflow's delay and replaces its inputs, in one transaction.
   *
   * <p>A debounced workflow holds its debounce key as its deduplication ID while it is DELAYED;
   * this is the bounce that keeps it waiting. The new delay is capped at the workflow's {@code
   * debounce_deadline_epoch_ms}, if one is set. The match covers the workflow's name, class and
   * instance so a debounce-key collision between different workflows -- {@code "a" + "b-c"} against
   * {@code "a-b" + "c"} -- never overwrites another workflow's inputs, and is application-scoped so
   * a peer's row is never extended.
   *
   * <p>If nothing matched, the result carries the current holder of the pair (or none), so the
   * caller can decide whether to start fresh, coordinate with an older holder, or surface a
   * conflict.
   *
   * <p>The new inputs are {@code args} serialized in {@code serializationFormat}, the workflow's
   * registered format (null for the default), with this database's serializer.
   *
   * <p>With a {@code caller}, the bounce is that caller's step: if the step already ran, what it
   * recorded is returned and nothing is touched; otherwise the bounce and its checkpoint commit
   * together, so a crash can never leave the row extended but the step unrecorded, which on replay
   * would bounce again.
   *
   * <p>With {@code ids}, the bounce is the debouncer's first step, which records the ids it assigns
   * with the outcome, as that step has always recorded them. When nothing was extended, the same
   * transaction also looks for a debouncer workflow of this application holding the key on the
   * internal queue -- what a debouncer from before debounced workflows leaves there -- so the
   * caller can forward to it rather than create a second workflow beside it.
   *
   * <p>The return is what the step records -- the {@link DebounceResult}, or the ids completed with
   * it -- as {@code Object}, since a replay hands back whatever the serializer preserved.
   */
  public static Object debounceDelayedWorkflow(
      DbContext ctx,
      String workflowName,
      String className,
      @Nullable String instanceName,
      String queueName,
      String deduplicationId,
      long delayUntilEpochMs,
      Object[] args,
      @Nullable String serializationFormat,
      @Nullable DebounceIds ids,
      @Nullable DebounceCaller caller)
      throws SQLException {
    long startTime = System.currentTimeMillis();
    try (var conn = ctx.getConnection()) {
      return SqlTransaction.call(
          conn,
          c -> {
            if (caller != null) {
              var prev =
                  StepsDAO.checkStepResult(
                      c, ctx.schema(), caller.workflowId(), caller.stepId(), caller.stepName());
              if (prev != null) {
                return prev.toResult(ctx.serializer());
              }
            }
            // Serialize after the replay check, so a step that already ran never serializes
            // arguments it will not use.
            var serializedArgs =
                SerializationUtil.serializeArgs(args, null, serializationFormat, ctx.serializer());
            var result =
                bounce(
                    ctx,
                    c,
                    workflowName,
                    className,
                    instanceName,
                    queueName,
                    deduplicationId,
                    delayUntilEpochMs,
                    serializedArgs.serializedValue(),
                    serializedArgs.serialization());
            Object recorded =
                ids == null
                    ? result
                    : ids.withBounced(
                        result, findDebouncerWorkflow(ctx, c, queueName, deduplicationId, result));
            if (caller == null) {
              return recorded;
            }
            var serialized = SerializationUtil.serializeValue(recorded, null, ctx.serializer());
            StepsDAO.recordStepResult(
                ctx,
                c,
                new StepResult(
                    caller.workflowId(),
                    caller.stepId(),
                    caller.stepName(),
                    serialized.serializedValue(),
                    null,
                    null,
                    serialized.serialization()),
                startTime,
                System.currentTimeMillis());
            return recorded;
          });
    }
  }

  /**
   * The debouncer workflow of this application holding {@code deduplicationId} on the internal
   * queue, if nothing was bounced and one does; otherwise null. When the bounce itself ran on the
   * internal queue, the holder it reported is that one.
   */
  private static @Nullable String findDebouncerWorkflow(
      DbContext ctx,
      Connection conn,
      String queueName,
      String deduplicationId,
      DebounceResult result)
      throws SQLException {
    if (result instanceof DebounceResult.NotBounced notBounced) {
      var holder =
          Constants.DBOS_INTERNAL_QUEUE.equals(queueName)
              ? notBounced.holder()
              : findDeduplicationHolder(
                  conn, ctx.schema(), Constants.DBOS_INTERNAL_QUEUE, deduplicationId);
      if (holder != null && holder.isDebouncerWorkflow() && !holder.isForeignTo(ctx.appName())) {
        return holder.workflowId();
      }
    }
    return null;
  }

  private static DebounceResult bounce(
      DbContext ctx,
      Connection conn,
      String workflowName,
      String className,
      @Nullable String instanceName,
      String queueName,
      String deduplicationId,
      long delayUntilEpochMs,
      String inputs,
      @Nullable String serialization)
      throws SQLException {
    // CASE rather than LEAST, for portability across the databases the DAO supports. An unclaimed
    // row is claimed for this application, as its dequeue would: left unclaimed, every peer
    // coalesces onto the one workflow and the last inputs win.
    var claimAppName =
        ctx.appName() == null ? "" : ",\n application_name = COALESCE(application_name, ?)";
    var sql =
        """
          UPDATE "%1$s".workflow_status
             SET delay_until_epoch_ms = CASE
                   WHEN debounce_deadline_epoch_ms IS NOT NULL AND debounce_deadline_epoch_ms < ?
                   THEN debounce_deadline_epoch_ms
                   ELSE ?
                 END,
                 serialization = ?,
                 updated_at = %2$s%3$s
           WHERE name = ?
             AND class_name = ?
             AND config_name IS NOT DISTINCT FROM ?
             AND queue_name = ?
             AND deduplication_id = ?
             AND status = ?
             AND is_debounced = TRUE
        """
                .formatted(ctx.schema(), SystemDatabase.NOW_EPOCH_MS, claimAppName)
            + ctx.andAppScope()
            + " RETURNING workflow_uuid";
    // The latest call's inputs win, in the payload table, in the same transaction. An upsert
    // rather than an update: a row enqueued by a release that still wrote the status row's inputs
    // has no payload row yet, and the one created here takes precedence over the stale column on
    // every read. retention_timestamp takes the column default on insert and is left alone on
    // conflict, as in Python and TypeScript.
    var inputsSql =
        """
          INSERT INTO "%s".workflow_input (workflow_uuid, inputs)
          VALUES (?, ?)
          ON CONFLICT (workflow_uuid) DO UPDATE SET inputs = EXCLUDED.inputs
        """
            .formatted(ctx.schema());
    String workflowId = null;
    try (var stmt = conn.prepareStatement(sql)) {
      int i = 1;
      stmt.setLong(i++, delayUntilEpochMs);
      stmt.setLong(i++, delayUntilEpochMs);
      stmt.setString(i++, serialization);
      if (ctx.appName() != null) {
        stmt.setString(i++, ctx.appName());
      }
      stmt.setString(i++, workflowName);
      stmt.setString(i++, className);
      stmt.setString(i++, instanceName);
      stmt.setString(i++, queueName);
      stmt.setString(i++, deduplicationId);
      stmt.setString(i++, WorkflowState.DELAYED.name());
      ctx.bindAppScope(stmt, i);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          workflowId = rs.getString("workflow_uuid");
        }
      }
    }
    if (workflowId != null) {
      try (var stmt = conn.prepareStatement(inputsSql)) {
        stmt.setString(1, workflowId);
        stmt.setString(2, inputs);
        stmt.executeUpdate();
      }
      return new DebounceResult.Bounced(workflowId);
    }
    // No match: the key is unheld, or held by something this bounce must not extend. Read the
    // holder in the same transaction, so it is the holder the match failed against.
    return new DebounceResult.NotBounced(
        findDeduplicationHolder(conn, ctx.schema(), queueName, deduplicationId));
  }

  public static void setWorkflowDelay(DbContext ctx, String workflowId, long delayUntilEpochMs)
      throws SQLException {
    Objects.requireNonNull(workflowId, "workflowId must not be null");

    var sql =
        """
          UPDATE "%1$s".workflow_status
             SET delay_until_epoch_ms = ?,
                 updated_at = %2$s
           WHERE workflow_uuid = ?
             AND status = ?
        """
            .formatted(ctx.schema(), SystemDatabase.NOW_EPOCH_MS);
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setLong(1, delayUntilEpochMs);
      stmt.setString(2, workflowId);
      stmt.setString(3, WorkflowState.DELAYED.name());

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
          UPDATE "%1$s".workflow_status
             SET attributes = ?::jsonb,
                 updated_at = %2$s
           WHERE workflow_uuid = ?
        """
            .formatted(ctx.schema(), SystemDatabase.NOW_EPOCH_MS);
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, attributesJson);
      stmt.setString(2, workflowId);
      stmt.executeUpdate();
    }
  }

  /**
   * Transitions DELAYED workflows whose delay has expired to ENQUEUED.
   *
   * <p>A debounced workflow's deduplication ID is its debounce key, held only while it is DELAYED,
   * so the same update clears it: a later debounce on that key starts a fresh workflow instead of
   * bouncing one that is now committed to running. Every version sharing a fleet must clear it,
   * whether or not it writes debounced rows itself -- a sweep that flips the row and leaves the key
   * behind makes every later bounce on it retry until the workflow completes.
   */
  public static void transitionDelayedWorkflows(DbContext ctx) throws SQLException {
    var sql =
        """
          UPDATE "%s".workflow_status
             SET status = ?,
                 deduplication_id = CASE WHEN is_debounced THEN NULL ELSE deduplication_id END
           WHERE status = ?
             AND delay_until_epoch_ms <= ?
        """
                .formatted(ctx.schema())
            + ctx.andAppScope();

    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, WorkflowState.ENQUEUED.name());
      stmt.setString(2, WorkflowState.DELAYED.name());
      // This JVM's clock, the one delays are counted from, as in Python and TypeScript.
      stmt.setLong(3, System.currentTimeMillis());
      ctx.bindAppScope(stmt, 4);

      stmt.executeUpdate();
    }
  }

  /**
   * Cancels up to {@code limit} of this application's active workflows whose deadline has passed on
   * the database's clock, oldest deadline first, whichever executor they belong to and whether or
   * not anything is running them.
   *
   * <p>A row that a dequeue or a peer's sweep holds is skipped and left for the next sweep. So, on
   * CockroachDB, is a row that a transaction which just committed wrote or locked, until its locks
   * are cleaned up a moment later. The status values are literals rather than parameters, so the
   * planner can match idx_workflow_status_deadline's predicate under a generic plan.
   *
   * @return the IDs of the workflows cancelled
   */
  public static List<String> cancelTimedOutWorkflows(DbContext ctx, int limit) throws SQLException {
    var sql =
        """
          UPDATE "%1$s".workflow_status
             SET status = ?,
                 queue_name = NULL,
                 deduplication_id = NULL,
                 started_at_epoch_ms = NULL,
                 updated_at = %2$s,
                 completed_at = %2$s
           WHERE workflow_uuid IN (
               SELECT workflow_uuid FROM "%1$s".workflow_status
                WHERE status IN ('%3$s', '%4$s', '%5$s')
                  AND workflow_deadline_epoch_ms IS NOT NULL
                  AND workflow_deadline_epoch_ms <= %2$s%6$s
                ORDER BY workflow_deadline_epoch_ms
                LIMIT ?
                FOR UPDATE SKIP LOCKED)
           RETURNING workflow_uuid
        """
            .formatted(
                ctx.schema(),
                SystemDatabase.NOW_EPOCH_MS,
                WorkflowState.ENQUEUED.name(),
                WorkflowState.PENDING.name(),
                WorkflowState.DELAYED.name(),
                ctx.andAppScope());

    var cancelled = new ArrayList<String>();
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      int i = 1;
      stmt.setString(i++, WorkflowState.CANCELLED.name());
      i = ctx.bindAppScope(stmt, i);
      stmt.setInt(i, limit);
      try (var rs = stmt.executeQuery()) {
        while (rs.next()) {
          cancelled.add(rs.getString(1));
        }
      }
    }
    return cancelled;
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
      sqlBuilder.append(", ").append(INPUTS_COLUMN);
    }
    if (loadOutput) {
      sqlBuilder.append(", ").append(OUTPUT_COLUMNS);
    }
    if (loadInput || loadOutput) {
      sqlBuilder.append(", serialization");
    }

    sqlBuilder.append(" FROM \"%s\".workflow_status ".formatted(ctx.schema()));

    // Only join the payload table the caller actually asked for.
    if (loadInput) {
      sqlBuilder.append(inputsJoin(ctx.schema())).append(" ");
    }
    if (loadOutput) {
      sqlBuilder.append(outputJoin(ctx.schema())).append(" ");
    }

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
    if (input.isFork() != null) {
      if (input.isFork()) {
        whereConditions.add("forked_from IS NOT NULL");
      } else {
        whereConditions.add("forked_from IS NULL");
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
        prefixConditions.add("workflow_status.workflow_uuid LIKE ?");
        parameters.add(prefix + "%");
      }
      whereConditions.add(prefixConditions.toString());
    }
    if (input.workflowIds() != null && !input.workflowIds().isEmpty()) {
      whereConditions.add("workflow_status.workflow_uuid = ANY(?)");
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
            rs.getString("application_name"),
            rs.getBoolean("is_debounced"),
            SystemDatabase.toInstant(rs.getObject("debounce_deadline_epoch_ms", Long.class)));
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
          SELECT status, %1$s, serialization, recovery_attempts
          FROM "%2$s".workflow_status
          %3$s
          WHERE workflow_status.workflow_uuid = ?
        """
            .formatted(OUTPUT_COLUMNS, ctx.schema(), outputJoin(ctx.schema()));

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
          UPDATE "%1$s".workflow_status
          SET status = ?,
              queue_name = NULL,
              deduplication_id = NULL,
              started_at_epoch_ms = NULL,
              updated_at = %2$s,
              completed_at = %2$s
          WHERE workflow_uuid = ANY(?)
            AND status NOT IN (?, ?)
        """
            .formatted(ctx.schema(), SystemDatabase.NOW_EPOCH_MS);

    try (Connection conn = ctx.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      Array array = conn.createArrayOf("text", workflowIds.toArray(String[]::new));
      try {
        stmt.setString(1, WorkflowState.CANCELLED.name());
        stmt.setArray(2, array);
        stmt.setString(3, WorkflowState.SUCCESS.name());
        stmt.setString(4, WorkflowState.ERROR.name());
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
          UPDATE "%1$s".workflow_status
          SET status = ?,
              queue_name = ?,
              recovery_attempts = 0,
              workflow_deadline_epoch_ms = NULL,
              deduplication_id = NULL,
              started_at_epoch_ms = NULL,
              completed_at = NULL,
              updated_at = %2$s
          WHERE workflow_uuid = ANY(?)
            AND status NOT IN (?, ?)
        """
            .formatted(ctx.schema(), SystemDatabase.NOW_EPOCH_MS);

    try (Connection conn = ctx.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      Array array = conn.createArrayOf("text", filtered.toArray(String[]::new));
      try {
        stmt.setString(1, WorkflowState.ENQUEUED.name());
        stmt.setString(2, Objects.requireNonNullElse(queueName, Constants.DBOS_INTERNAL_QUEUE));
        stmt.setArray(3, array);
        stmt.setString(4, WorkflowState.SUCCESS.name());
        stmt.setString(5, WorkflowState.ERROR.name());
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

  /**
   * Drops a terminal workflow's history from {@code startStep} on and re-enqueues it under the same
   * ID, so a replay re-executes everything from that step. Unlike a fork, this writes no new
   * workflow: peers keep addressing the same ID.
   *
   * <p>In one transaction, from {@code startStep} on, it:
   *
   * <ul>
   *   <li>rolls each event published past the cut back to its last value from before the cut, or
   *       deletes it if it had none;
   *   <li>deletes the close sentinels of the streams, so the replay can append again. The stream
   *       entries themselves stay: their offsets are addresses peers read by;
   *   <li>deletes the step checkpoints and the event history;
   *   <li>deletes the messages the discarded steps consumed, and any still unconsumed;
   *   <li>deletes the workflow's outcome;
   *   <li>re-enqueues the workflow.
   * </ul>
   *
   * @throws DBOSNonExistentWorkflowException if the workflow does not exist
   * @throws IllegalStateException if the workflow is not in a terminal state, or changed status
   *     while being rewound
   */
  public static void rewindWorkflow(
      DbContext ctx, String workflowId, int startStep, RewindOptions options) throws SQLException {
    // Function IDs start at 0, so 0 is the whole history.
    if (startStep < 0) {
      throw new IllegalArgumentException("startStep must be >= 0, got " + startStep);
    }
    Objects.requireNonNull(options, "RewindOptions must not be null");
    var schema = ctx.schema();

    try (var txConn = ctx.getConnection()) {
      SqlTransaction.run(
          txConn,
          conn -> {
            var state = readRewindableState(conn, schema, workflowId);

            // Whether a key was published at or past the cut. %2$s is the key column the
            // correlated subquery compares against.
            var publishedPastCut =
                """
                  EXISTS (
                    SELECT 1 FROM "%1$s".workflow_events_history discarded
                    WHERE discarded.workflow_uuid = ? AND discarded.key = %2$s
                      AND discarded.function_id >= ?
                  )
                """;

            // workflow_events_history is the undo log for workflow_events, so the events are
            // rolled back before the history past the cut is deleted. First unpublish every key
            // the discarded steps published...
            var unpublishSql =
                ("""
                  DELETE FROM "%1$s".workflow_events
                  WHERE workflow_uuid = ? AND """
                        + publishedPastCut)
                    .formatted(schema, "\"%s\".workflow_events.key".formatted(schema));
            try (var stmt = conn.prepareStatement(unpublishSql)) {
              stmt.setString(1, workflowId);
              stmt.setString(2, workflowId);
              stmt.setInt(3, startStep);
              stmt.executeUpdate();
            }

            // ...then restore those keys to the last value published before the cut, if any.
            var restoreSql =
                ("""
                  INSERT INTO "%1$s".workflow_events (workflow_uuid, key, value, serialization)
                  SELECT surviving.workflow_uuid, surviving.key, surviving.value,
                         surviving.serialization
                  FROM (
                    SELECT weh.workflow_uuid, weh.key, weh.value, weh.serialization,
                           ROW_NUMBER() OVER (PARTITION BY weh.key ORDER BY weh.function_id DESC)
                             AS rn
                    FROM "%1$s".workflow_events_history weh
                    WHERE weh.workflow_uuid = ? AND weh.function_id < ? AND """
                        + publishedPastCut
                        + """
                  ) surviving
                  WHERE surviving.rn = 1
                """)
                    .formatted(schema, "weh.key");
            try (var stmt = conn.prepareStatement(restoreSql)) {
              stmt.setString(1, workflowId);
              stmt.setInt(2, startStep);
              stmt.setString(3, workflowId);
              stmt.setInt(4, startStep);
              stmt.executeUpdate();
            }

            StreamsDAO.deleteCloseSentinels(conn, schema, workflowId, startStep);

            for (var table : List.of("operation_outputs", "workflow_events_history")) {
              var deleteSql =
                  """
                    DELETE FROM "%s".%s WHERE workflow_uuid = ? AND function_id >= ?
                  """
                      .formatted(schema, table);
              try (var stmt = conn.prepareStatement(deleteSql)) {
                stmt.setString(1, workflowId);
                stmt.setInt(2, startStep);
                stmt.executeUpdate();
              }
            }

            var notificationsSql =
                """
                  DELETE FROM "%s".notifications
                  WHERE destination_uuid = ?
                    AND (consumed_by_function_id >= ? OR consumed = FALSE)
                """
                    .formatted(schema);
            try (var stmt = conn.prepareStatement(notificationsSql)) {
              stmt.setString(1, workflowId);
              stmt.setInt(2, startStep);
              stmt.executeUpdate();
            }

            var outputSql =
                """
                  DELETE FROM "%s".workflow_output WHERE workflow_uuid = ?
                """
                    .formatted(schema);
            try (var stmt = conn.prepareStatement(outputSql)) {
              stmt.setString(1, workflowId);
              stmt.executeUpdate();
            }

            // Re-enqueue. The legacy output and error columns are cleared too: reads fall back to
            // them when there is no workflow_output row, so a value left there would be returned
            // as the rewound workflow's result. Re-asserting the status read above keeps a
            // workflow that moved on underneath this transaction from being resurrected.
            var setVersion =
                options.applicationVersion() != null ? ", application_version = ?" : "";
            var enqueueSql =
                """
                  UPDATE "%1$s".workflow_status
                  SET status = ?, owner_xid = NULL, queue_name = ?, queue_partition_key = ?,
                      recovery_attempts = 0, workflow_deadline_epoch_ms = NULL,
                      deduplication_id = NULL, started_at_epoch_ms = NULL, completed_at = NULL,
                      output = NULL, error = NULL, updated_at = %2$s%3$s
                  WHERE workflow_uuid = ? AND status = ?
                """
                    .formatted(schema, SystemDatabase.NOW_EPOCH_MS, setVersion);
            try (var stmt = conn.prepareStatement(enqueueSql)) {
              int i = 1;
              stmt.setString(i++, WorkflowState.ENQUEUED.name());
              stmt.setString(
                  i++,
                  Objects.requireNonNullElse(options.queueName(), Constants.DBOS_INTERNAL_QUEUE));
              stmt.setString(i++, options.queuePartitionKey());
              if (options.applicationVersion() != null) {
                stmt.setString(i++, options.applicationVersion());
              }
              stmt.setString(i++, workflowId);
              stmt.setString(i++, state.name());
              if (stmt.executeUpdate() != 1) {
                throw new IllegalStateException(
                    "Workflow %s changed status while being rewound; retry the rewind"
                        .formatted(workflowId));
              }
            }
          });
    }
  }

  /**
   * Checks that a workflow can be rewound, without changing anything: it must exist and be in a
   * terminal state. A rewind repeats this check in its own transaction; this one lets a caller
   * refuse before touching anything outside the system database.
   */
  public static void checkRewindable(DbContext ctx, String workflowId, int startStep)
      throws SQLException {
    if (startStep < 0) {
      throw new IllegalArgumentException("startStep must be >= 0, got " + startStep);
    }
    try (var conn = ctx.getConnection()) {
      readRewindableState(conn, ctx.schema(), workflowId);
    }
  }

  private static WorkflowState readRewindableState(
      Connection conn, String schema, String workflowId) throws SQLException {
    var state = getWorkflowState(conn, schema, workflowId);
    if (state == null) {
      throw new DBOSNonExistentWorkflowException(workflowId);
    }
    if (state.isActive()) {
      throw new IllegalStateException(
          ("Cannot rewind %s (%s): only a workflow in a terminal state can be rewound, so cancel it"
                  + " first")
              .formatted(workflowId, state));
    }
    return state;
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
          SELECT workflow_status.workflow_uuid, name, class_name, config_name,
                 application_version, application_id, authenticated_user, authenticated_roles,
                 assumed_role, %1$s, serialization, attributes, application_name
          FROM "%2$s".workflow_status
          %3$s
          WHERE workflow_status.workflow_uuid = ANY(?)
        """
            .formatted(INPUTS_COLUMN, schema, inputsJoin(schema));

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
                workflow_timeout_ms, forked_from, serialization, attributes,
                application_name
              ) VALUES\s
            """
                .formatted(schema));

    StringJoiner rows = new StringJoiner(", ");
    for (int i = 0; i < origIds.size(); i++) {
      rows.add("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)");
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
        stmt.setObject(p++, timeoutMs);
        stmt.setString(p++, origIds.get(i));
        stmt.setString(p++, rd.serialization());
        stmt.setString(p++, rd.attributes());
        stmt.setString(p++, forkAppNames.get(i));
      }
      stmt.executeUpdate();
    }

    // The fork's input is a copy of the original's. The status insert above just claimed each new
    // ID, so an input already filed under one is a leftover retention has not yet swept, and is
    // replaced. retention_timestamp defaults to now, the same reading as the fork's created_at.
    StringBuilder inputSQL =
        new StringBuilder(
            """
              INSERT INTO "%s".workflow_input (workflow_uuid, inputs) VALUES\s
            """
                .formatted(schema));
    StringJoiner inputRows = new StringJoiner(", ");
    for (int i = 0; i < forkIds.size(); i++) {
      inputRows.add("(?, ?)");
    }
    inputSQL.append(inputRows);
    inputSQL.append(
        " ON CONFLICT (workflow_uuid)"
            + " DO UPDATE SET inputs = EXCLUDED.inputs,"
            + " retention_timestamp = EXCLUDED.retention_timestamp");

    try (var stmt = conn.prepareStatement(inputSQL.toString())) {
      int p = 1;
      for (int i = 0; i < forkIds.size(); i++) {
        stmt.setString(p++, forkIds.get(i));
        stmt.setString(p++, dataList.get(i).inputs());
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

  // workflow_input and workflow_output (migration 109) have no foreign key, so a status delete
  // was never going to take them. Migration 112 then drops the operation_outputs cascade, leaving
  // all three the same: nothing follows a status row out on its own. Every delete path clears all
  // three by ID.
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
        var rowsCutoff =
            SystemDatabase.retryOnSerializationError(
                "retention rows-threshold probe", () -> getRowsCutoff(ctx, conn, rowsThreshold));
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

    Long oldest =
        SystemDatabase.retryOnSerializationError(
            "retention status seed",
            () -> {
              try (var stmt = conn.prepareStatement(seedSql)) {
                bindStatusGcFilter(stmt, deadline);
                try (var rs = stmt.executeQuery()) {
                  return rs.next() ? rs.getLong(1) : null;
                }
              }
            });
    if (oldest == null) {
      return;
    }

    var watermark = oldest - 1;
    while (true) {
      final long from = watermark;
      var next =
          SystemDatabase.retryOnSerializationError(
              "retention status batch",
              () -> deleteStatusBatch(ctx, conn, deadline, from, batchSize));
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
   * removed from each table in {@link #PAYLOAD_TABLES} order. Runs after the status sweep, most of
   * whose orphans fall in range: a payload written by this SDK is stamped no later than the
   * completion that made its workflow collectable.
   *
   * <p>The exception is a step row that predates migration 110, which stamped every existing one
   * with the migration's own clock. That can sit well above its workflow's completed_at, so the
   * status sweep can collect the workflow in a round that leaves the step rows behind. They are
   * deferred rather than stranded: a later round, once its cutoff passes the migration, finds no
   * status row for them and collects them. Until migration 112 drops the cascade, the foreign key
   * takes them with the status row anyway.
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

    Long oldest =
        SystemDatabase.retryOnSerializationError(
            "retention payload seed",
            () -> {
              try (var stmt = conn.prepareStatement(seedSql)) {
                stmt.setLong(1, deadline);
                try (var rs = stmt.executeQuery()) {
                  return rs.next() ? rs.getLong(1) : null;
                }
              }
            });
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
          SystemDatabase.retryOnSerializationError(
              "retention payload batch",
              () ->
                  SqlTransaction.<PayloadBatch>call(
                      conn,
                      c -> {
                        // Batches are cut by candidate count, so rows spared by the anti-join
                        // only thin one out; they are re-checked on the next round.
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
                      }));
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
   * Refuse to import a workflow with a payload this runtime would have to re-serialize but cannot.
   *
   * <p>Payloads carried as stored strings are written back unchanged and need no serializer. Only
   * an export written before payloads travelled that way carries them deserialized, in the
   * workflow's status and its steps, and re-serializing those needs the format that wrote them. A
   * payload dropped on the way through would restore a workflow that never had it.
   */
  private static void requireSerializerFor(ExportedWorkflow workflow, DBOSSerializer serializer) {
    var status = workflow.status();
    var payloads = workflow.payloads();
    var formats = new LinkedHashSet<String>();
    if (payloads == null && !SerializationUtil.canDeserialize(status.serialization(), serializer)) {
      formats.add(status.serialization());
    }
    var storedSteps = storedStepsById(payloads);
    for (var step : workflow.steps()) {
      if (!storedSteps.containsKey(step.functionId())
          && !SerializationUtil.canDeserialize(step.serialization(), serializer)) {
        formats.add(step.serialization());
      }
    }
    if (!formats.isEmpty()) {
      throw new IllegalStateException(
          "Cannot import workflow %s: it is serialized as %s, which this application has no serializer for"
              .formatted(status.workflowId(), String.join(", ", formats)));
    }
  }

  private static Map<Integer, ExportedWorkflow.SerializedStep> storedStepsById(
      ExportedWorkflow.@Nullable SerializedPayloads payloads) {
    if (payloads == null) {
      return Map.of();
    }
    return payloads.steps().stream()
        .collect(Collectors.toMap(ExportedWorkflow.SerializedStep::stepId, s -> s));
  }

  /**
   * Exports a workflow, and its children when asked, with every payload exactly as stored.
   *
   * <p>Nothing is deserialized: the payloads travel as stored strings in {@link
   * ExportedWorkflow#payloads()}, so an export keeps the payloads' types however the export is
   * encoded, and needs no serializer for them. A workflow written in a format this application
   * cannot read exports like any other.
   */
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
        WorkflowStatus status = null;
        String serialization = null;
        String inputs = null;
        String output = null;
        String error = null;
        try (var stmt = conn.prepareStatement(workflowStatusByIdSql(ctx.schema()))) {
          stmt.setString(1, wfid);
          try (var rs = stmt.executeQuery()) {
            if (rs.next()) {
              // The metadata only: the payloads are taken below as the strings they are stored as.
              status = resultsToWorkflowStatus(rs, false, false, ctx.serializer());
              serialization = rs.getString("serialization");
              inputs = rs.getString("inputs");
              output = rs.getString("output");
              error = rs.getString("error");
            }
          }
        }
        var steps = StepsDAO.exportWorkflowSteps(conn, ctx.schema(), wfid);
        var payloads =
            status == null
                ? null
                : new ExportedWorkflow.SerializedPayloads(
                    serialization, inputs, output, error, steps.stored());
        var events = listWorkflowEvents(conn, ctx.schema(), wfid);
        var eventHistory = listWorkflowEventHistory(conn, ctx.schema(), wfid);
        var streams = listWorkflowStreams(conn, ctx.schema(), wfid);
        workflows.add(
            new ExportedWorkflow(status, steps.steps(), events, eventHistory, streams, payloads));
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
      requireSerializerFor(workflow, serializer);
    }

    var wfSQL =
        """
        INSERT INTO "%s".workflow_status (
          workflow_uuid, status,
          name, class_name, config_name,
          authenticated_user, assumed_role, authenticated_roles,
          executor_id, application_version, application_id,
          created_at, updated_at, started_at_epoch_ms,
          queue_name, deduplication_id, priority, queue_partition_key,
          workflow_timeout_ms, workflow_deadline_epoch_ms,
          recovery_attempts, forked_from, parent_workflow_id, serialization,
          delay_until_epoch_ms, completed_at, was_forked_from, attributes, schedule_name,
          application_name, is_debounced, debounce_deadline_epoch_ms
        ) VALUES (
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?,
          ?, ?
        )
        """
            .formatted(ctx.schema());

    // retention_timestamp takes the column default, so retention starts at import. The export
    // carries no retention timestamp to restore, and the original ones would be long past the
    // cutoff, getting the payloads collected immediately.
    var wfInputSQL =
        """
        INSERT INTO "%s".workflow_input (workflow_uuid, inputs) VALUES (?, ?)
        """
            .formatted(ctx.schema());

    var wfOutputSQL =
        """
        INSERT INTO "%s".workflow_output (workflow_uuid, output, error) VALUES (?, ?, ?)
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
                var wfInputStmt = conn.prepareStatement(wfInputSQL);
                var wfOutputStmt = conn.prepareStatement(wfOutputSQL);
                var stepStmt = conn.prepareStatement(stepSQL);
                var eventStmt = conn.prepareStatement(eventSQL);
                var eventHistoryStmt = conn.prepareStatement(eventHistorySQL);
                var streamsStmt = conn.prepareStatement(streamsSQL)) {

              for (var workflow : workflows) {
                var status = workflow.status();
                var payloads = workflow.payloads();
                // Stored strings are written back unchanged. Only an export that predates them
                // carries its payloads deserialized, and those are re-serialized here.
                var serialization =
                    payloads != null ? payloads.serialization() : status.serialization();
                var storedSteps = storedStepsById(payloads);

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
                String inputs;
                String output;
                String error;
                if (payloads != null) {
                  inputs = payloads.inputs();
                  output = payloads.output();
                  error = payloads.error();
                } else {
                  inputs =
                      status.input() == null
                          ? null
                          : SerializationUtil.serializeArgs(
                                  status.input(), null, status.serialization(), serializer)
                              .serializedValue();
                  output =
                      status.output() == null
                          ? null
                          : SerializationUtil.serializeValue(
                                  status.output(), status.serialization(), serializer)
                              .serializedValue();
                  error =
                      status.error() == null
                          ? null
                          : SerializationUtil.serializeError(
                                  status.error().throwable(), status.serialization(), serializer)
                              .serializedValue();
                }

                wfStmt.setString(9, status.executorId());
                wfStmt.setString(10, status.appVersion());
                wfStmt.setString(11, status.appId());
                wfStmt.setObject(12, status.createdAtEpochMs());
                wfStmt.setObject(13, status.updatedAtEpochMs());
                wfStmt.setObject(14, status.startedAtEpochMs());
                wfStmt.setString(15, status.queueName());
                wfStmt.setString(16, status.deduplicationId());
                wfStmt.setObject(17, status.priority());
                wfStmt.setString(18, status.queuePartitionKey());
                wfStmt.setObject(19, status.timeoutMs());
                wfStmt.setObject(20, status.deadlineEpochMs());
                wfStmt.setObject(21, status.recoveryAttempts());
                wfStmt.setString(22, status.forkedFrom());
                wfStmt.setString(23, status.parentWorkflowId());
                wfStmt.setString(24, serialization);
                wfStmt.setObject(25, status.delayUntilEpochMs());
                wfStmt.setObject(26, status.completedAtEpochMs());
                // NOT NULL column: an export predating it carries no value, so fall back to false.
                wfStmt.setBoolean(27, Boolean.TRUE.equals(status.wasForkedFrom()));
                wfStmt.setString(28, attributesToJson(status.attributes()));
                wfStmt.setString(29, status.scheduleName());
                wfStmt.setString(30, status.applicationName());
                // NOT NULL column: an export predating it carries no value, so fall back to false.
                wfStmt.setBoolean(31, Boolean.TRUE.equals(status.isDebounced()));
                wfStmt.setObject(32, status.debounceDeadlineEpochMs());
                wfStmt.addBatch();

                // Every workflow has an input row, as in Python; an output row only once there is
                // an output or an error to hold.
                wfInputStmt.setString(1, status.workflowId());
                wfInputStmt.setString(2, inputs);
                wfInputStmt.addBatch();
                if (output != null || error != null) {
                  wfOutputStmt.setString(1, status.workflowId());
                  wfOutputStmt.setString(2, output);
                  wfOutputStmt.setString(3, error);
                  wfOutputStmt.addBatch();
                }

                for (var step : workflow.steps()) {
                  stepStmt.setString(1, status.workflowId());
                  stepStmt.setInt(2, step.functionId());
                  stepStmt.setString(3, step.functionName());
                  var stored = storedSteps.get(step.functionId());
                  if (stored != null) {
                    stepStmt.setString(4, stored.output());
                    stepStmt.setString(5, stored.error());
                  } else {
                    stepStmt.setString(
                        4,
                        step.output() == null
                            ? null
                            : SerializationUtil.serializeValue(
                                    step.output(), step.serialization(), serializer)
                                .serializedValue());
                    stepStmt.setString(
                        5, step.error() == null ? null : step.error().serializedError());
                  }
                  stepStmt.setString(6, step.childWorkflowId());
                  stepStmt.setObject(7, step.startedAtEpochMs());
                  stepStmt.setObject(8, step.completedAtEpochMs());
                  stepStmt.setString(9, step.serialization());
                  // A step keeps exactly the app_name it was exported with. An export that predates
                  // the column carries no app_name, so its steps import unclaimed rather than
                  // inheriting a guess from the workflow -- the same choice Python and TypeScript
                  // make.
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

              // The status inserts fail on an existing row, so any payload under these IDs is a
              // leftover retention has not swept yet. Clear it before writing this import's own.
              deleteWorkflowChildRows(
                  conn,
                  ctx.schema(),
                  workflows.stream().map(w -> w.status().workflowId()).toArray(String[]::new));

              wfInputStmt.executeBatch();
              wfOutputStmt.executeBatch();
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
