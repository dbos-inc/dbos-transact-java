package dev.dbos.transact.database.dao;

import dev.dbos.transact.Constants;
import dev.dbos.transact.conductor.WorkflowFork;
import dev.dbos.transact.database.ChildOf;
import dev.dbos.transact.database.DatabaseTime;
import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.database.DebounceCaller;
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
import dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException;
import dev.dbos.transact.exceptions.DBOSWorkflowIdInUseException;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.JsonUtility;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.DebounceResult;
import dev.dbos.transact.workflow.Debouncer.DebounceIds;
import dev.dbos.transact.workflow.DeduplicationHolder;
import dev.dbos.transact.workflow.ErrorResult;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.RewindOptions;
import dev.dbos.transact.workflow.WorkflowIdReusePolicy;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
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
      String creatorXid,
      @Nullable WorkflowIdReusePolicy workflowIdReusePolicy,
      @Nullable ChildOf parent)
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
                creatorXid,
                owner(ctx, initStatus));
        boolean created = creatorXid.equals(resRow.creatorXid);

        // A row carrying another creator token was already there; a retry whose first attempt
        // committed finds its own, and is not refused. Thrown before anything else is checked, and
        // rolled back, so a refused call writes nothing: the insert wrote no input for a row it
        // did not create, and its conflict arm changes nothing.
        if (!created && workflowIdReusePolicy == WorkflowIdReusePolicy.REJECT) {
          throw new DBOSWorkflowIdInUseException(
              initStatus.workflowId(), resRow.status(), resRow.workflowName());
        }

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

        // A row this call created can't be dead-lettered yet, so only a found row is checked.
        if (!created && resRow.status == WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED) {
          throw new DBOSMaxRecoveryAttemptsExceededException(
              initStatus.workflowId(),
              Objects.requireNonNullElse(maxRetries, Constants.DEFAULT_MAX_RECOVERY_ATTEMPTS));
        }

        // The parent records the child whether this call created it or found it, so the parent's
        // replay attaches to the same child. Recording it checks that the parent's execution still
        // owns the parent: one that does not rolls the child's row back with its step.
        if (parent != null) {
          var childStep =
              new StepResult(
                      parent.workflowId(),
                      parent.functionId(),
                      parent.functionName(),
                      null,
                      null,
                      null,
                      null)
                  .withChildWorkflowId(initStatus.workflowId());
          StepsDAO.recordStepResult(ctx, conn, childStep, null, null, parent.ownerXid());
          DebugTriggers.debugTriggerPoint(DebugTriggers.DEBUG_TRIGGER_CHILD_RECORDED);
        }

        // The insert's conflict arm changes nothing, so committing a row this call found is
        // harmless, and commits the parent's step.
        shouldCommit = true;

        // Only the call that created a row owns its execution. A caller that finds someone else's
        // row polls for the outcome instead. A retry whose first attempt committed but lost its
        // acknowledgement finds its own token.
        if (!created) {
          return new WorkflowInitResult(
              state, resRow.deadline(), false, resRow.serialization(), resRow.clock(), null);
        }

        // A row created PENDING is owned by its creator token. One created ENQUEUED or DELAYED is
        // owned by nobody until the queue claims it.
        return new WorkflowInitResult(
            state,
            resRow.deadline(),
            true,
            resRow.serialization(),
            resRow.clock(),
            state == WorkflowState.PENDING ? creatorXid : null);

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
              owner_xid = NULL, updated_at = %2$s, completed_at = %2$s
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
      String creatorXid,
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
      String creatorXid,
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
            parent_workflow_id, owner_xid, creator_xid, serialization, attributes, schedule_name,
            application_name, is_debounced, debounce_deadline_epoch_ms
          ) VALUES (
            ?, ?,
            ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            %2$s, %2$s, ?,
            ?, COALESCE(?::bigint, (SELECT %4$s FROM (VALUES (?::bigint)) AS t(timeout_ms))),
            ?, ?, ?, ?, ?::jsonb, ?,
            ?, ?, ?
          )
          ON CONFLICT (workflow_uuid)
            -- A no-op, so RETURNING hands back the row already there, unchanged: a duplicate start
            -- takes nothing from it, not its executor, its owner or its application.
            DO UPDATE SET creator_xid = workflow_status.creator_xid
          RETURNING recovery_attempts, status, name, class_name, config_name, queue_name, workflow_deadline_epoch_ms, creator_xid, serialization, %3$s AS db_now
        """
            .formatted(
                schema,
                SystemDatabase.NOW_EPOCH_MS,
                SystemDatabase.CLOCK_TIMESTAMP_EPOCH_MS,
                // Wrapped so the timeout parameter is bound once though the expression reads it
                // twice.
                SystemDatabase.deadlineEpochMs("t.timeout_ms"));

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

      // A direct start's creator token also owns the execution. A queued workflow is owned by
      // nobody until a claim installs its own token.
      stmt.setString(22, state == WorkflowState.PENDING ? creatorXid : null);
      stmt.setString(23, creatorXid);
      stmt.setString(24, status.serialization());
      stmt.setString(25, attributesJson);
      stmt.setString(26, status.scheduleName());
      stmt.setString(27, appName);
      stmt.setBoolean(28, status.isDebounced());
      stmt.setObject(29, status.debounceDeadlineEpochMs(), Types.BIGINT);

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
                  rs.getString("creator_xid"),
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
      if (!creatorXid.equals(result.creatorXid())) {
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
   * only to a PENDING row still owned by the execution writing it: a run owns its workflow's
   * outcome exactly as long as the row says that run is what the workflow is doing.
   *
   * <p>Returning false means the row was CANCELLED, dead-lettered, already terminal, handed to
   * another execution (ENQUEUED/DELAYED, e.g. by a concurrent resume, or PENDING under another
   * execution's token, once a claim has taken it), or gone entirely. Callers that need to
   * distinguish a deleted row do so when they wait for the recorded outcome (see {@link
   * #awaitWorkflowResult(DbContext, Duration, String, boolean)}).
   */
  private static boolean updateWorkflowOutcome(
      Connection conn,
      String schema,
      String workflowId,
      WorkflowState status,
      String output,
      String error,
      String ownerXid)
      throws SQLException {

    logger.debug("updateWorkflowOutcome wfid {} status {}", workflowId, status);

    // Only the outcomes that carry a payload. Cancellation goes through its own paths, and here it
    // would write an empty workflow_output row.
    if (status != WorkflowState.SUCCESS && status != WorkflowState.ERROR) {
      throw new IllegalArgumentException(
          "updateWorkflowOutcome records SUCCESS or ERROR, not " + status);
    }

    // The write is itself the fence: it lands only while this execution still owns the workflow,
    // and it releases the workflow, as every terminal transition does.
    var sql =
        """
          UPDATE "%1$s".workflow_status
          SET status = ?, updated_at = %2$s, completed_at = %2$s, deduplication_id = NULL,
              owner_xid = NULL
          WHERE workflow_uuid = ? AND status = ? AND owner_xid = ?
        """
            .formatted(schema, SystemDatabase.NOW_EPOCH_MS);

    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, status.name());
      stmt.setString(2, workflowId);
      stmt.setString(3, WorkflowState.PENDING.name());
      stmt.setString(4, Objects.requireNonNull(ownerXid, "ownerXid"));

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
      DbContext ctx,
      String workflowId,
      WorkflowState state,
      String output,
      String error,
      String ownerXid)
      throws SQLException {

    try (var conn = ctx.getConnection()) {
      return SqlTransaction.call(
          conn,
          c -> updateWorkflowOutcome(c, ctx.schema(), workflowId, state, output, error, ownerXid));
    }
  }

  /**
   * Store the result to workflow_output, marking the workflow SUCCESS
   *
   * @param workflowId id of the workflow
   * @param result output serialized as json
   * @param ownerXid the token of the execution recording it
   * @return true if the outcome was recorded, false if the row is no longer PENDING or no longer
   *     owned by the execution
   */
  public static boolean recordWorkflowOutput(
      DbContext ctx, String workflowId, String result, String ownerXid) throws SQLException {

    return updateWorkflowOutcome(ctx, workflowId, WorkflowState.SUCCESS, result, null, ownerXid);
  }

  /**
   * Store the error to workflow_output, marking the workflow ERROR
   *
   * @param workflowId id of the workflow
   * @param error output serialized as json
   * @param ownerXid the token of the execution recording it
   * @return true if the outcome was recorded, false if the row is no longer PENDING or no longer
   *     owned by the execution
   */
  public static boolean recordWorkflowError(
      DbContext ctx, String workflowId, String error, String ownerXid) throws SQLException {

    return updateWorkflowOutcome(ctx, workflowId, WorkflowState.ERROR, null, error, ownerXid);
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
            // The error is recorded only on a row this call creates, which it owns: a workflow
            // already there, started by someone else, keeps its own outcome.
            var creatorXid = UUID.randomUUID().toString();
            insertWorkflowStatus(
                c, ctx.schema(), initStatus, delayUntilEpochMs, creatorXid, owner(ctx, initStatus));
            updateWorkflowOutcome(
                c,
                ctx.schema(),
                initStatus.workflowId(),
                WorkflowState.ERROR,
                null,
                error,
                creatorXid);
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
            + SystemDatabase.CLOCK_TIMESTAMP_EPOCH_MS
            + " AS db_now")
        + " FROM \"%s\".workflow_status ".formatted(schema)
        + inputsJoin(schema)
        + " "
        + outputJoin(schema)
        + " WHERE workflow_status.workflow_uuid = ?";
  }

  public static void checkWorkflow(DbContext ctx, String workflowId, @Nullable String ownerXid)
      throws SQLException {
    try (var conn = ctx.getConnection()) {
      checkWorkflow(conn, ctx.schema(), workflowId, ownerXid);
    }
  }

  /**
   * Fails if the workflow does not exist or is cancelled and, given an {@code ownerXid}, with
   * {@link DBOSWorkflowExecutionConflictException} if that execution no longer owns it.
   *
   * <p>The read takes no lock, so it does not replace the {@link #checkOwner} a write makes. It
   * stops an execution that has lost the workflow before it runs a step or keeps waiting, rather
   * than at the write that follows.
   */
  public static void checkWorkflow(
      Connection conn, String schema, String workflowId, @Nullable String ownerXid)
      throws SQLException {
    var sql =
        """
        SELECT status, owner_xid FROM "%s".workflow_status WHERE workflow_uuid = ?
        """
            .formatted(Objects.requireNonNull(schema));
    WorkflowState workflowState = null;
    String currentOwner = null;
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, Objects.requireNonNull(workflowId));
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          workflowState = WorkflowState.valueOf(rs.getString("status"));
          currentOwner = rs.getString("owner_xid");
        }
      }
    }
    if (workflowState == null) {
      throw new DBOSNonExistentWorkflowException(workflowId);
    }

    if (workflowState == WorkflowState.CANCELLED) {
      throw new DBOSWorkflowCancelledException(workflowId);
    }

    if (ownerXid != null && !ownerXid.equals(currentOwner)) {
      throw DBOSWorkflowExecutionConflictException.notOwned(workflowId);
    }
  }

  /**
   * Fails with {@link DBOSWorkflowExecutionConflictException} unless the execution holding {@code
   * ownerXid} still owns the workflow. A NULL owner -- the workflow was cancelled, resumed,
   * recovered, timed out or finished -- fails, and so does a deleted row.
   *
   * <p>Takes the row with {@code FOR NO KEY UPDATE} and keeps it until the caller's transaction
   * ends, so no hand-off can land between this check and the write it guards: a cancel, resume or
   * claim waits for that write to commit. The caller must be inside a transaction, or the lock
   * would end with this statement. A write that changes other rows in the same transaction makes
   * them first and checks last. That does not rule out a deadlock: a cancel or delete of a tree
   * holds each target's row as it walks, so two workflows cancelling each other, or a delete racing
   * a fenced write from one of the tree's descendants, can each wait on a row the other holds. The
   * database aborts one with 40P01, and its retry runs as if the other had committed first.
   */
  public static void checkOwner(Connection conn, String schema, String workflowId, String ownerXid)
      throws SQLException {
    Objects.requireNonNull(ownerXid, "ownerXid");
    if (conn.getAutoCommit()) {
      throw new IllegalStateException("checkOwner must run inside a transaction");
    }
    var sql =
        """
        SELECT owner_xid FROM "%s".workflow_status WHERE workflow_uuid = ? FOR NO KEY UPDATE
        """
            .formatted(schema);
    String current = null;
    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          current = rs.getString("owner_xid");
        }
      }
    }
    if (!ownerXid.equals(current)) {
      throw DBOSWorkflowExecutionConflictException.notOwned(workflowId);
    }
  }

  /** The token of the execution that owns the workflow, read without a lock; null if none. */
  public static @Nullable String getWorkflowOwner(DbContext ctx, String workflowId)
      throws SQLException {
    var sql =
        """
        SELECT owner_xid FROM "%s".workflow_status WHERE workflow_uuid = ?
        """
            .formatted(ctx.schema());
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        return rs.next() ? rs.getString("owner_xid") : null;
      }
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
      String creatorXid,
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
                      c,
                      ctx.schema(),
                      caller.workflowId(),
                      caller.stepId(),
                      caller.stepName(),
                      caller.ownerXid());
              if (prev != null) {
                return prev.toResult(ctx.serializer());
              }
            }
            String created =
                takeOver(ctx, c, debouncerWorkflowId, promised, delayUntilEpochMs, creatorXid);
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
                  System.currentTimeMillis(),
                  caller.ownerXid());
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
      String creatorXid)
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
              owner_xid = NULL,
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
            conn, ctx.schema(), promised, delayUntilEpochMs, creatorXid, owner(ctx, promised));
    if (!creatorXid.equals(inserted.creatorXid())) {
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
                      c,
                      ctx.schema(),
                      caller.workflowId(),
                      caller.stepId(),
                      caller.stepName(),
                      caller.ownerXid());
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
                System.currentTimeMillis(),
                caller.ownerXid());
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
          UPDATE "%1$s".workflow_status
             SET status = ?,
                 deduplication_id = CASE WHEN is_debounced THEN NULL ELSE deduplication_id END,
                 updated_at = %2$s
           WHERE status = ?
             AND delay_until_epoch_ms <= %2$s
        """
                .formatted(ctx.schema(), SystemDatabase.NOW_EPOCH_MS)
            + ctx.andAppScope();

    // Released against the database's clock, which every executor shares, rather than this JVM's:
    // whichever executor runs the release, a delay ends at the same instant.
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, WorkflowState.ENQUEUED.name());
      stmt.setString(2, WorkflowState.DELAYED.name());
      ctx.bindAppScope(stmt, 3);

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
                 owner_xid = NULL,
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
   * is correct. Callers that know the row must already exist (a run waiting for an outcome it just
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

  /**
   * What a parent's step recorded for a child it started or enqueued: the child's ID, or the
   * deduplication collision its enqueue failed with. Empty when nothing is recorded at that step.
   */
  public static Optional<StepResult> checkChildWorkflow(
      DbContext ctx, String workflowUuid, int functionId) throws SQLException {

    final String sql =
        """
          SELECT function_name, child_workflow_id, error, serialization
          FROM "%s".operation_outputs WHERE workflow_uuid = ? AND function_id = ?
        """
            .formatted(ctx.schema());

    try (Connection connection = ctx.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {

      stmt.setString(1, workflowUuid);
      stmt.setInt(2, functionId);

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return Optional.of(
              new StepResult(
                  workflowUuid,
                  functionId,
                  rs.getString("function_name"),
                  null,
                  SystemDatabase.errorOrNull(rs.getString("error")),
                  rs.getString("child_workflow_id"),
                  rs.getString("serialization")));
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

  /**
   * Cancels the workflows and, when {@code cancelChildren}, their descendants, on the caller's
   * transaction.
   *
   * <p>Each level is cancelled before its children are read. A child whose insert holds its
   * parent's row lock commits before that level's update goes ahead, and is then read with the next
   * level; one inserted after it finds its parent no longer owned and is rolled back. Reading the
   * tree first would miss a child committed in between. Each read sees what committed before it
   * only under READ COMMITTED, so the transaction must run at that level.
   *
   * @param callerWorkflowId the workflow making the call, when it is made from inside one, which
   *     may not cancel itself or a tree it belongs to
   * @throws IllegalArgumentException if the caller is a target, before anything is cancelled, or is
   *     reached among a target's descendants, after which the caller's transaction must roll back
   *     the levels already cancelled
   */
  public static void cancelWorkflows(
      Connection conn,
      String schema,
      List<String> workflowIds,
      boolean cancelChildren,
      @Nullable String callerWorkflowId)
      throws SQLException {
    var roots = filterNullsAndBlanks(workflowIds);
    if (roots.isEmpty()) {
      return;
    }
    refuseSelfTarget(callerWorkflowId, roots, "cancel");

    if (!cancelChildren) {
      cancelBatch(conn, schema, roots);
      return;
    }

    walkTree(conn, schema, roots, callerWorkflowId, "cancel", WorkflowDAO::cancelBatch);
  }

  /** What a tree walk does to each level of the tree, on the walk's transaction. */
  @FunctionalInterface
  private interface LevelAction {
    void apply(Connection conn, String schema, Collection<String> level) throws SQLException;
  }

  /**
   * Applies {@code onLevel} to the roots and then to each level of their descendants in turn, each
   * level before its children are read, on the caller's transaction.
   *
   * <p>A walk that reaches the calling workflow is an {@code action} on a tree the caller is
   * inside, and is refused there: the levels above have been acted on by now, and the refusal rolls
   * them back with the transaction. Each workflow's root is carried down the walk, so the refusal
   * names the tree without reading rows the walk may already have changed.
   */
  private static void walkTree(
      Connection conn,
      String schema,
      Collection<String> roots,
      @Nullable String callerWorkflowId,
      String action,
      LevelAction onLevel)
      throws SQLException {
    var rootOf = new HashMap<String, String>();
    roots.forEach(root -> rootOf.put(root, root));
    List<String> frontier = new ArrayList<>(roots);
    while (!frontier.isEmpty()) {
      if (callerWorkflowId != null && frontier.contains(callerWorkflowId)) {
        throw new IllegalArgumentException(
            "workflow %s cannot %s the tree of %s, which contains it"
                .formatted(callerWorkflowId, action, rootOf.get(callerWorkflowId)));
      }
      onLevel.apply(conn, schema, frontier);
      var next = new ArrayList<String>();
      for (var child : getDirectChildren(conn, schema, frontier).entrySet()) {
        if (rootOf.putIfAbsent(child.getKey(), rootOf.get(child.getValue())) == null) {
          next.add(child.getKey());
        }
      }
      frontier = next;
    }
  }

  /**
   * Cancels {@code workflowId} only while the execution holding {@code ownerXid} still owns it. The
   * condition is the fence, so no lock is taken first. Returns whether it cancelled the workflow.
   */
  public static boolean cancelOwnedWorkflow(DbContext ctx, String workflowId, String ownerXid)
      throws SQLException {
    String sql =
        """
          UPDATE "%1$s".workflow_status
          SET status = ?,
              queue_name = NULL,
              deduplication_id = NULL,
              started_at_epoch_ms = NULL,
              owner_xid = NULL,
              updated_at = %2$s,
              completed_at = %2$s
          WHERE workflow_uuid = ?
            AND status NOT IN (?, ?)
            AND owner_xid = ?
        """
            .formatted(ctx.schema(), SystemDatabase.NOW_EPOCH_MS);

    try (Connection conn = ctx.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, WorkflowState.CANCELLED.name());
      stmt.setString(2, workflowId);
      stmt.setString(3, WorkflowState.SUCCESS.name());
      stmt.setString(4, WorkflowState.ERROR.name());
      stmt.setString(5, ownerXid);
      return stmt.executeUpdate() > 0;
    }
  }

  /** Cancels the given workflows in one statement, on the caller's transaction. */
  private static void cancelBatch(Connection conn, String schema, Collection<String> workflowIds)
      throws SQLException {
    String sql =
        """
          UPDATE "%1$s".workflow_status
          SET status = ?,
              queue_name = NULL,
              deduplication_id = NULL,
              started_at_epoch_ms = NULL,
              owner_xid = NULL,
              updated_at = %2$s,
              completed_at = %2$s
          WHERE workflow_uuid = ANY(?)
            AND status NOT IN (?, ?)
        """
            .formatted(schema, SystemDatabase.NOW_EPOCH_MS);

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
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

  /**
   * Resumes the workflows onto {@code queueName}, or the internal queue, on the caller's
   * transaction.
   *
   * @param callerWorkflowId the workflow making the call, when it is made from inside one, which
   *     may not resume itself
   * @throws IllegalArgumentException if the caller is a target, before anything is resumed
   */
  public static void resumeWorkflows(
      Connection conn,
      String schema,
      List<String> workflowIds,
      String queueName,
      @Nullable String callerWorkflowId)
      throws SQLException {
    var filtered = filterNullsAndBlanks(workflowIds);
    if (filtered.isEmpty()) {
      return;
    }
    refuseSelfTarget(callerWorkflowId, filtered, "resume");

    String sql =
        """
          UPDATE "%1$s".workflow_status
          SET status = ?,
              queue_name = ?,
              recovery_attempts = 0,
              workflow_deadline_epoch_ms = NULL,
              deduplication_id = NULL,
              started_at_epoch_ms = NULL,
              owner_xid = NULL,
              completed_at = NULL,
              updated_at = %2$s
          WHERE workflow_uuid = ANY(?)
            AND status NOT IN (?, ?)
        """
            .formatted(schema, SystemDatabase.NOW_EPOCH_MS);

    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
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

  /**
   * Deletes the workflows and, when {@code deleteChildren}, their descendants, on the caller's
   * transaction.
   *
   * @param callerWorkflowId the workflow making the call, when it is made from inside one, which
   *     may not delete itself or a tree it belongs to
   * @throws IllegalArgumentException if the caller is a target, before anything is deleted, or is
   *     reached among a target's descendants, after which the caller's transaction must roll back
   *     the levels already deleted
   */
  public static void deleteWorkflows(
      Connection conn,
      String schema,
      List<String> workflowIds,
      boolean deleteChildren,
      @Nullable String callerWorkflowId)
      throws SQLException {
    var filtered = filterNullsAndBlanks(workflowIds);
    if (filtered.isEmpty()) {
      return;
    }
    refuseSelfTarget(callerWorkflowId, filtered, "delete");

    if (!deleteChildren) {
      deleteBatch(conn, schema, filtered);
      return;
    }
    // Each level is deleted before its children are read, as a cancel's is, so a child whose
    // insert holds its parent's lock is read once it commits, and one inserted later finds its
    // parent gone. The children are still found by their own parent_workflow_id.
    walkTree(conn, schema, filtered, callerWorkflowId, "delete", WorkflowDAO::deleteBatch);
  }

  /**
   * Deletes the given workflows and their rows in every other table, on the caller's transaction.
   */
  private static void deleteBatch(Connection conn, String schema, Collection<String> workflowIds)
      throws SQLException {
    var sql =
        """
          DELETE FROM "%s".workflow_status
          WHERE workflow_uuid = ANY(?);
        """
            .formatted(schema);

    var ids = workflowIds.toArray(String[]::new);
    try (var stmt = conn.prepareStatement(sql)) {
      var array = conn.createArrayOf("text", ids);
      try {
        stmt.setArray(1, array);
        stmt.executeUpdate();
      } finally {
        array.free();
      }
    }
    deleteWorkflowChildRows(conn, schema, ids);
  }

  /** The direct children of the given workflows, each mapped to its parent. */
  private static Map<String, String> getDirectChildren(
      Connection conn, String schema, Collection<String> workflowIds) throws SQLException {
    if (workflowIds.isEmpty()) {
      return Map.of();
    }
    var sql =
        """
          SELECT workflow_uuid, parent_workflow_id
          FROM "%s".workflow_status
          WHERE parent_workflow_id = ANY(?)
        """
            .formatted(schema);

    try (var stmt = conn.prepareStatement(sql)) {
      var array = conn.createArrayOf("text", workflowIds.toArray(String[]::new));
      try {
        stmt.setArray(1, array);
        try (var rs = stmt.executeQuery()) {
          var result = new HashMap<String, String>();
          while (rs.next()) {
            result.put(rs.getString(1), rs.getString(2));
          }
          return result;
        }
      } finally {
        array.free();
      }
    }
  }

  /**
   * Refuses a cancel, resume or delete, made from inside a workflow, that targets that workflow.
   * Each clears or removes the workflow's row, so the call's own checkpoint, or the enclosing
   * step's, would then fail the ownership check, rolling the call back and leaving the workflow
   * waiting on an outcome nothing writes.
   */
  private static void refuseSelfTarget(
      @Nullable String callerWorkflowId, Collection<String> targets, String action) {
    if (callerWorkflowId != null && targets.contains(callerWorkflowId)) {
      throw new IllegalArgumentException(
          "workflow %s cannot %s itself".formatted(callerWorkflowId, action));
    }
  }

  public static Set<String> getWorkflowChildren(Connection conn, String schema, String workflowId)
      throws SQLException {
    var descendants = new HashSet<String>();
    List<String> frontier = List.of(workflowId);
    while (!frontier.isEmpty()) {
      var children = getDirectChildren(conn, schema, frontier).keySet();
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
        List.of(new WorkflowFork(workflowId, forkedWorkflowId, startStep)),
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

  /**
   * Forks workflows in one transaction, each under its fork's ID and from its fork's start step.
   * Fails, writing nothing, if any of them does not exist.
   */
  public static void forkWorkflows(
      DbContext ctx,
      List<WorkflowFork> forks,
      @Nullable String applicationVersion,
      @Nullable String queueName,
      @Nullable String queuePartitionKey,
      @Nullable Duration timeout)
      throws SQLException {

    if (forks.isEmpty()) {
      return;
    }
    var workflowIds = forks.stream().map(WorkflowFork::workflowId).toList();

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
            var rows =
                forks.stream()
                    .map(
                        fork -> {
                          var original = wfDataMap.get(fork.workflowId());
                          // One app name per fork, shared by its status row and its copied steps:
                          // the source's, or this application claiming an unclaimed one.
                          var appName =
                              original.applicationName() != null
                                  ? original.applicationName()
                                  : ctx.appName();
                          return new ForkRow(fork, original, appName);
                        })
                    .toList();

            batchInsertForkedStatuses(
                conn,
                ctx.schema(),
                rows,
                applicationVersion,
                forkQueueName,
                queuePartitionKey,
                timeoutMs);

            markWasForkedFrom(conn, ctx.schema(), workflowIds);

            // A fork from step 0 has nothing before its start step to copy.
            var rowsToCopy = rows.stream().filter(row -> row.fork().startStep() > 0).toList();
            if (!rowsToCopy.isEmpty()) {
              batchCopyWorkflowData(conn, ctx.schema(), rowsToCopy);
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

  /** A fork with what it takes from its original: the original's row, and the fork's app name. */
  private record ForkRow(WorkflowFork fork, ForkWorkflowData original, @Nullable String appName) {}

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
      List<ForkRow> rows,
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
                workflow_timeout_ms, forked_from, serialization, attributes,
                application_name
              ) VALUES\s
            """
                .formatted(schema));

    StringJoiner values = new StringJoiner(", ");
    for (int i = 0; i < rows.size(); i++) {
      values.add("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)");
    }
    sql.append(values);

    try (var stmt = conn.prepareStatement(sql.toString())) {
      int p = 1;
      for (var row : rows) {
        ForkWorkflowData rd = row.original();
        stmt.setString(p++, row.fork().forkedWorkflowId());
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
        stmt.setString(p++, row.fork().workflowId());
        stmt.setString(p++, rd.serialization());
        stmt.setString(p++, rd.attributes());
        stmt.setString(p++, row.appName());
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
    for (int i = 0; i < rows.size(); i++) {
      inputRows.add("(?, ?)");
    }
    inputSQL.append(inputRows);
    inputSQL.append(
        " ON CONFLICT (workflow_uuid)"
            + " DO UPDATE SET inputs = EXCLUDED.inputs,"
            + " retention_timestamp = EXCLUDED.retention_timestamp");

    try (var stmt = conn.prepareStatement(inputSQL.toString())) {
      int p = 1;
      for (var row : rows) {
        stmt.setString(p++, row.fork().forkedWorkflowId());
        stmt.setString(p++, row.original().inputs());
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

  private static void batchCopyWorkflowData(Connection conn, String schema, List<ForkRow> rows)
      throws SQLException {

    StringJoiner valueRows = new StringJoiner(", ");
    for (int i = 0; i < rows.size(); i++) {
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
        for (var row : rows) {
          stmt.setString(p++, row.fork().workflowId());
          stmt.setString(p++, row.fork().forkedWorkflowId());
          stmt.setInt(p++, row.fork().startStep());
          // The copied steps take the same app_name as the forked workflow row itself.
          stmt.setString(p++, row.appName());
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

  public static Map<String, Object> getAllEvents(DbContext ctx, String workflowId)
      throws SQLException {
    var sql =
        """
        SELECT key, value, serialization
        FROM "%s".workflow_events
        WHERE workflow_uuid = ?
        """
            .formatted(ctx.schema());

    var result = new LinkedHashMap<String, Object>();
    try (var conn = ctx.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        while (rs.next()) {
          result.put(
              rs.getString("key"),
              SerializationUtil.deserializeValue(
                  rs.getString("value"), rs.getString("serialization"), ctx.serializer()));
        }
      }
    }
    return result;
  }
}
