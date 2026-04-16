package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.exceptions.DBOSConflictingWorkflowException;
import dev.dbos.transact.exceptions.DBOSMaxRecoveryAttemptsExceededException;
import dev.dbos.transact.exceptions.DBOSNonExistentWorkflowException;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.ErrorResult;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.GetWorkflowAggregatesInput;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Timeout;
import dev.dbos.transact.workflow.WorkflowAggregateRow;
import dev.dbos.transact.workflow.WorkflowDelay;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.StringJoiner;
import java.util.UUID;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WorkflowDAO {

  private static final Logger logger = LoggerFactory.getLogger(WorkflowDAO.class);

  // All workflow_status columns except inputs/output/error/serialization, which are loaded
  // conditionally. Add new columns here so both getWorkflowStatus and listWorkflows stay in sync.
  private static final String WORKFLOW_STATUS_COLUMNS =
      """
        workflow_uuid, status,
        name, class_name, config_name,
        queue_name, deduplication_id, priority, queue_partition_key,
        executor_id, application_version, application_id,
        authenticated_user, assumed_role, authenticated_roles,
        created_at, updated_at, recovery_attempts, started_at_epoch_ms,
        workflow_timeout_ms, workflow_deadline_epoch_ms,
        forked_from, parent_workflow_id, was_forked_from, delay_until_epoch_ms
      """;

  private final DataSource dataSource;
  private final String schema;
  private final DBOSSerializer serializer;
  private long getResultPollingIntervalMs = 1000;

  WorkflowDAO(DataSource ds, String schema, DBOSSerializer serializer) {
    this.dataSource = ds;
    this.schema = Objects.requireNonNull(schema);
    this.serializer = serializer;
  }

  void speedUpPollingForTest() {
    getResultPollingIntervalMs = 100;
  }

  WorkflowInitResult initWorkflowStatus(
      WorkflowStatusInternal initStatus,
      Integer maxRetries,
      boolean isRecoveryRequest,
      boolean isDequeuedRequest,
      String ownerXid)
      throws SQLException {

    logger.debug("initWorkflowStatus workflowId {}", initStatus.workflowId());

    try (Connection connection = dataSource.getConnection()) {

      boolean shouldCommit = false;

      try {
        connection.setAutoCommit(false);
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        InsertWorkflowResult resRow =
            insertWorkflowStatus(
                connection, initStatus, ownerXid, isRecoveryRequest || isDequeuedRequest);

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

        var state = WorkflowState.valueOf(resRow.status);

        // If there is an existing DB record and we aren't here to recover it,
        //  leave it be.  Roll back the change to max recovery attempts.
        if (!ownerXid.equals(resRow.ownerXid) && !isRecoveryRequest && !isDequeuedRequest) {
          if (resRow.status.equals(WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED.name())) {
            throw new DBOSMaxRecoveryAttemptsExceededException(initStatus.workflowId(), maxRetries);
          }
          return new WorkflowInitResult(
              state, resRow.deadlineEpochMs(), false, resRow.serialization());
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
                  .formatted(this.schema);

          try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED.name());
            stmt.setString(2, initStatus.workflowId());
            stmt.setString(3, WorkflowState.PENDING.name());

            stmt.executeUpdate();
          }

          throw new DBOSMaxRecoveryAttemptsExceededException(initStatus.workflowId(), maxRetries);
        }

        return new WorkflowInitResult(
            state, resRow.deadlineEpochMs(), true, resRow.serialization());

      } finally {
        if (shouldCommit) {
          connection.commit();
        } else {
          connection.rollback();
        }
        DebugTriggers.debugTriggerPoint(DebugTriggers.DEBUG_TRIGGER_INITWF_COMMIT);
      }
    } // end try with resources connection closed
  }

  static record InsertWorkflowResult(
      int recoveryAttempts,
      String status,
      String workflowName,
      String className,
      String instanceName,
      String queueName,
      Long deadlineEpochMs,
      String serialization,
      String ownerXid) {}

  /**
   * Insert into the workflow_status table
   *
   * @param status WorkflowStatusInternal holds the data for a workflow_status row
   * @return InsertWorkflowResult some of the column inserted
   * @throws SQLException
   */
  InsertWorkflowResult insertWorkflowStatus(
      Connection connection,
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
            parent_workflow_id, owner_xid, serialization
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            .formatted(this.schema);

    Objects.requireNonNull(status, "status must not be null");
    Objects.requireNonNull(status.workflowId(), "workflowId must not be null");
    var state =
        status.queueName() == null
            ? WorkflowState.PENDING
            : status.delay() == null ? WorkflowState.ENQUEUED : WorkflowState.DELAYED;
    var recoveryAttempts =
        state == WorkflowState.ENQUEUED || state == WorkflowState.DELAYED ? 0 : 1;

    var authenticatedRolesJson =
        status.authenticatedRoles() != null ? JSONUtil.toJson(status.authenticatedRoles()) : null;
    try (PreparedStatement stmt = connection.prepareStatement(insertSQL)) {

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
      stmt.setInt(26, incrementAttempts ? 1 : 0);

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          InsertWorkflowResult result =
              new InsertWorkflowResult(
                  rs.getInt("recovery_attempts"),
                  rs.getString("status"),
                  rs.getString("name"),
                  rs.getString("class_name"),
                  rs.getString("config_name"),
                  rs.getString("queue_name"),
                  rs.getObject("workflow_deadline_epoch_ms", Long.class),
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

  void updateWorkflowOutcome(
      Connection connection, String workflowId, WorkflowState status, String output, String error)
      throws SQLException {

    logger.debug("updateWorkflowOutcome wfid {} status {}", workflowId, status);

    // Note that transitions from CANCELLED to SUCCESS or ERROR are forbidden
    var sql =
        """
          UPDATE "%s".workflow_status
          SET status = ?, output = ?, error = ?, updated_at = ?, deduplication_id = NULL
          WHERE workflow_uuid = ? AND NOT (status = ? AND ? in (?, ?))
        """
            .formatted(this.schema);

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, status.name());
      stmt.setString(2, output);
      stmt.setString(3, error);
      stmt.setLong(4, Instant.now().toEpochMilli());
      stmt.setString(5, workflowId);
      stmt.setString(6, WorkflowState.CANCELLED.name());
      stmt.setString(7, status.name());
      stmt.setString(8, WorkflowState.SUCCESS.name());
      stmt.setString(9, WorkflowState.ERROR.name());

      stmt.executeUpdate();
    }
  }

  /**
   * Store the result to workflow_status
   *
   * @param workflowId id of the workflow
   * @param result output serialized as json
   */
  void recordWorkflowOutput(String workflowId, String result) throws SQLException {

    try (Connection connection = dataSource.getConnection()) {
      updateWorkflowOutcome(connection, workflowId, WorkflowState.SUCCESS, result, null);
    }
  }

  /**
   * Store the error to workflow_status
   *
   * @param workflowId id of the workflow
   * @param error output serialized as json
   */
  void recordWorkflowError(String workflowId, String error) throws SQLException {

    try (Connection connection = dataSource.getConnection()) {
      updateWorkflowOutcome(connection, workflowId, WorkflowState.ERROR, null, error);
    }
  }

  String getWorkflowSerialization(String workflowId) throws SQLException {
    var sql =
        "SELECT serialization FROM \"%s\".workflow_status WHERE workflow_uuid = ?"
            .formatted(this.schema);
    try (var conn = dataSource.getConnection();
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

  WorkflowStatus getWorkflowStatus(String workflowId) throws SQLException {

    try (var conn = dataSource.getConnection()) {
      return getWorkflowStatus(conn, workflowId);
    }
  }

  WorkflowStatus getWorkflowStatus(Connection conn, String workflowId) throws SQLException {
    if (Objects.requireNonNull(workflowId, "workflowId must not be null").isEmpty()) {
      throw new IllegalArgumentException("workflowId must not be empty");
    }

    var sql =
        ("SELECT " + WORKFLOW_STATUS_COLUMNS + ", inputs, output, error, serialization")
            + " FROM \"%s\".workflow_status WHERE workflow_uuid = ?".formatted(this.schema);

    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return resultsToWorkflowStatus(rs, true, true, this.serializer);
        }
      }
    }

    return null;
  }

  void setWorkflowDelay(String workflowId, WorkflowDelay delay) throws SQLException {
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
                 updated_at = EXTRACT(epoch FROM NOW()) * 1000
           WHERE workflow_uuid = ?
             AND status = ?
        """
            .formatted(this.schema);
    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setLong(1, resolved.toEpochMilli());
      stmt.setString(2, workflowId);
      stmt.setString(3, WorkflowState.DELAYED.name());

      stmt.executeUpdate();
    }
  }

  void transitionDelayedWorkflows() throws SQLException {
    var sql =
        """
          UPDATE "%s".workflow_status
             SET status = ?
           WHERE status = ?
             AND delay_until_epoch_ms <= ?
        """
            .formatted(this.schema);

    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, WorkflowState.ENQUEUED.name());
      stmt.setString(2, WorkflowState.DELAYED.name());
      stmt.setLong(3, System.currentTimeMillis());

      stmt.executeUpdate();
    }
  }

  List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) throws SQLException {

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

    sqlBuilder.append(" FROM \"%s\".workflow_status ".formatted(this.schema));

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

    try (Connection connection = dataSource.getConnection();
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
            WorkflowStatus info =
                resultsToWorkflowStatus(rs, loadInput, loadOutput, this.serializer);
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

  List<WorkflowAggregateRow> getWorkflowAggregates(GetWorkflowAggregatesInput input)
      throws SQLException {
    if (input == null) {
      input = new GetWorkflowAggregatesInput();
    }

    // Determine which columns to group by (in a stable order)
    record GroupDim(String name, String column) {}
    var dims = new ArrayList<GroupDim>();
    if (input.groupByStatus()) dims.add(new GroupDim("status", "status"));
    if (input.groupByName()) dims.add(new GroupDim("name", "name"));
    if (input.groupByQueueName()) dims.add(new GroupDim("queue_name", "queue_name"));
    if (input.groupByExecutorId()) dims.add(new GroupDim("executor_id", "executor_id"));
    if (input.groupByApplicationVersion())
      dims.add(new GroupDim("application_version", "application_version"));

    if (dims.isEmpty()) {
      throw new IllegalArgumentException(
          "At least one groupBy flag must be set in GetWorkflowAggregatesInput");
    }

    List<Object> parameters = new ArrayList<>();
    StringBuilder sqlBuilder = new StringBuilder("SELECT ");

    StringJoiner selectCols = new StringJoiner(", ");
    for (var dim : dims) selectCols.add(dim.column());
    selectCols.add("COUNT(*) AS count");
    sqlBuilder.append(selectCols).append(" FROM \"%s\".workflow_status".formatted(this.schema));

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
    if (input.workflowIdPrefix() != null && !input.workflowIdPrefix().isEmpty()) {
      // Multiple prefixes are OR'd: (uuid LIKE 'a%' OR uuid LIKE 'b%' ...)
      StringJoiner prefixOr = new StringJoiner(" OR ", "(", ")");
      for (var prefix : input.workflowIdPrefix()) {
        prefixOr.add("workflow_uuid LIKE ?");
        parameters.add(prefix + "%");
      }
      whereConditions.add(prefixOr.toString());
    }

    if (whereConditions.length() > 0) {
      sqlBuilder.append(" WHERE ").append(whereConditions);
    }

    // --- GROUP BY ---
    StringJoiner groupByCols = new StringJoiner(", ");
    for (var dim : dims) groupByCols.add(dim.column());
    sqlBuilder.append(" GROUP BY ").append(groupByCols);
    sqlBuilder.append(" ORDER BY ").append(groupByCols);

    List<WorkflowAggregateRow> results = new ArrayList<>();
    try (Connection connection = dataSource.getConnection();
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
            var group = new LinkedHashMap<String, String>();
            for (var dim : dims) {
              group.put(dim.name(), rs.getString(dim.column()));
            }
            results.add(new WorkflowAggregateRow(group, rs.getLong("count")));
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
                ? JSONUtil.fromJson(authenticatedRolesJson, String[].class)
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
            serialization);
    return info;
  }

  List<WorkflowStatus> getPendingWorkflows(List<String> executorIds, String appVersion)
      throws SQLException {
    var input =
        new ListWorkflowsInput()
            .withStatus(WorkflowState.PENDING)
            .withExecutorIds(executorIds)
            .withApplicationVersion(appVersion);
    return listWorkflows(input);
  }

  @SuppressWarnings("unchecked")
  <T> Result<T> awaitWorkflowResult(String workflowId) throws SQLException {

    final String sql =
        """
          SELECT status, output, error, serialization
          FROM "%s".workflow_status
          WHERE workflow_uuid = ?
        """
            .formatted(this.schema);

    while (true) {
      try (Connection connection = dataSource.getConnection();
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
                    SerializationUtil.deserializeValue(output, serialization, this.serializer);
                return Result.success((T) outputValue);
              }

              case ERROR -> {
                String error = rs.getString("error");
                Throwable t =
                    SerializationUtil.deserializeError(error, serialization, this.serializer);
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
        Thread.sleep(getResultPollingIntervalMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Workflow polling interrupted for " + workflowId, e);
      }
    }
  }

  void recordChildWorkflow(
      String parentId,
      String childId, // workflowId of the child
      int functionId, // func id in the parent
      String functionName,
      long startTime)
      throws SQLException {

    var result =
        new StepResult(parentId, functionId, functionName, null, null, null, null)
            .withChildWorkflowId(childId);
    try (Connection connection = dataSource.getConnection()) {
      StepsDAO.recordStepResultTxn(result, null, null, connection, schema);
    }
  }

  Optional<String> checkChildWorkflow(String workflowUuid, int functionId) throws SQLException {

    final String sql =
        """
          SELECT child_workflow_id FROM "%s".operation_outputs WHERE workflow_uuid = ? AND function_id = ?
        """
            .formatted(this.schema);

    try (Connection connection = dataSource.getConnection();
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

  private List<String> filterNullsAndBlanks(List<String> workflowIds) {
    if (workflowIds == null) {
      return List.of();
    }
    return workflowIds.stream().filter(id -> id != null && !id.isBlank()).toList();
  }

  void cancelWorkflows(List<String> workflowIds) throws SQLException {
    List<String> filtered = filterNullsAndBlanks(workflowIds);
    if (filtered.isEmpty()) {
      return;
    }
    String sql =
        """
          UPDATE "%s".workflow_status
          SET status = ?,
              queue_name = NULL,
              deduplication_id = NULL,
              started_at_epoch_ms = NULL,
              updated_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint
          WHERE workflow_uuid = ANY(?)
            AND status NOT IN (?, ?)
        """
            .formatted(this.schema);

    try (Connection conn = dataSource.getConnection();
        PreparedStatement stmt = conn.prepareStatement(sql)) {
      Array array = conn.createArrayOf("text", filtered.toArray(String[]::new));
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

  void resumeWorkflows(List<String> workflowIds, String queueName) throws SQLException {
    List<String> filtered = filterNullsAndBlanks(workflowIds);
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
              updated_at = (EXTRACT(EPOCH FROM now()) * 1000)::bigint
          WHERE workflow_uuid = ANY(?)
            AND status NOT IN (?, ?)
        """
            .formatted(this.schema);

    try (Connection conn = dataSource.getConnection();
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

  void deleteWorkflows(List<String> workflowIds, boolean deleteChildren) throws SQLException {
    List<String> filtered = filterNullsAndBlanks(workflowIds);
    if (filtered.isEmpty()) {
      return;
    }

    var wfIdSet = new HashSet<String>(filtered);
    if (deleteChildren) {
      for (var wfid : filtered) {
        var children = getWorkflowChildren(wfid);
        wfIdSet.addAll(children);
      }
    }

    var sql =
        """
          DELETE FROM "%s".workflow_status
          WHERE workflow_uuid = ANY(?);
        """
            .formatted(this.schema);

    try (var conn = dataSource.getConnection();
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

  Set<String> getWorkflowChildren(String workflowId) throws SQLException {
    var children = new HashSet<String>();
    var toProcess = new ArrayDeque<String>();
    toProcess.add(workflowId);

    var sql =
        """
          SELECT child_workflow_id
          FROM "%s".operation_outputs
          WHERE workflow_uuid = ? AND child_workflow_id IS NOT NULL
        """
            .formatted(this.schema);

    try (var conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      while (!toProcess.isEmpty()) {
        var wfid = toProcess.poll();
        stmt.setString(1, wfid);

        try (var rs = stmt.executeQuery()) {
          while (rs.next()) {
            var childWorkflowId = rs.getString(1);
            if (!children.contains(childWorkflowId)) {
              children.add(childWorkflowId);
              toProcess.add(childWorkflowId);
            }
          }
        }
      }
    }
    return children;
  }

  String forkWorkflow(String originalWorkflowId, int startStep, ForkOptions options)
      throws SQLException {

    options = Objects.requireNonNullElseGet(options, ForkOptions::new);

    var status = getWorkflowStatus(originalWorkflowId);
    if (status == null) {
      throw new DBOSNonExistentWorkflowException(originalWorkflowId);
    }

    String forkedWorkflowId =
        Objects.requireNonNullElseGet(
            options.forkedWorkflowId(), () -> UUID.randomUUID().toString());

    logger.debug("forkWorkflow Original id {} forked id {}", originalWorkflowId, forkedWorkflowId);

    var timeout = Objects.requireNonNullElseGet(options.timeout(), Timeout::inherit);
    Long timeoutMS = null;
    if (timeout instanceof Timeout.Inherit) {
      timeoutMS = status.timeoutMs();
    } else if (timeout instanceof Timeout.Explicit explicit) {
      timeoutMS = explicit.value().toMillis();
    }

    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);

      try {
        // Create entry for forked workflow
        insertForkedWorkflowStatus(
            connection,
            originalWorkflowId,
            forkedWorkflowId,
            status,
            options.applicationVersion(),
            timeoutMS,
            options.queueName(),
            options.queuePartitionKey(),
            this.schema,
            this.serializer);

        // Copy operation outputs if starting from step > 0
        if (startStep > 0) {
          copyOperationOutputs(
              connection, originalWorkflowId, forkedWorkflowId, startStep, this.schema);
        }

        // Mark the original workflow as having been forked
        markWasForkedFrom(connection, originalWorkflowId, this.schema);

        connection.commit();
        return forkedWorkflowId;

      } catch (SQLException e) {
        connection.rollback();
        throw e;
      }
    }
  }

  private static void insertForkedWorkflowStatus(
      Connection connection,
      String originalWorkflowId,
      String forkedWorkflowId,
      WorkflowStatus originalStatus,
      String applicationVersion,
      Long timeoutMS,
      String queueName,
      String queuePartitionKey,
      String schema,
      DBOSSerializer serializer)
      throws SQLException {
    Objects.requireNonNull(schema);

    String sql =
        """
          INSERT INTO "%s".workflow_status (
            workflow_uuid, status, name, class_name, config_name, application_version, application_id,
            authenticated_user, authenticated_roles, assumed_role, queue_name, queue_partition_key, inputs,
            workflow_timeout_ms, forked_from, serialization
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
            .formatted(schema);

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, forkedWorkflowId);
      stmt.setString(2, WorkflowState.ENQUEUED.name());
      stmt.setString(3, originalStatus.workflowName());
      stmt.setString(4, originalStatus.className());
      stmt.setString(5, originalStatus.instanceName());
      stmt.setString(6, applicationVersion);
      stmt.setString(7, originalStatus.appId());
      stmt.setString(8, originalStatus.authenticatedUser());
      stmt.setString(
          9,
          originalStatus.authenticatedRoles() == null
              ? null
              : JSONUtil.toJson(originalStatus.authenticatedRoles()));
      stmt.setString(10, originalStatus.assumedRole());
      stmt.setString(11, Objects.requireNonNullElse(queueName, Constants.DBOS_INTERNAL_QUEUE));
      stmt.setString(12, queuePartitionKey);
      stmt.setString(
          13,
          SerializationUtil.serializeArgs(
                  originalStatus.input(), null, originalStatus.serialization(), serializer)
              .serializedValue());
      stmt.setObject(14, timeoutMS);
      stmt.setString(15, originalWorkflowId);
      stmt.setString(16, originalStatus.serialization());

      stmt.executeUpdate();
    }
  }

  private static void markWasForkedFrom(Connection connection, String workflowId, String schema)
      throws SQLException {
    String sql =
        """
          UPDATE "%s".workflow_status
          SET was_forked_from = TRUE
          WHERE workflow_uuid = ?
        """
            .formatted(schema);
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.executeUpdate();
    }
  }

  private static void copyOperationOutputs(
      Connection connection,
      String originalWorkflowId,
      String forkedWorkflowId,
      int startStep,
      String schema)
      throws SQLException {

    String stepOutputsSql =
        """
          INSERT INTO "%1$s".operation_outputs
              (workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization)
          SELECT ? as workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms, serialization
              FROM "%1$s".operation_outputs
              WHERE workflow_uuid = ? AND function_id < ?
        """
            .formatted(schema);
    try (PreparedStatement stmt = connection.prepareStatement(stepOutputsSql)) {
      stmt.setString(1, forkedWorkflowId);
      stmt.setString(2, originalWorkflowId);
      stmt.setInt(3, startStep);

      int rowsCopied = stmt.executeUpdate();
      logger.debug("Copied " + rowsCopied + " operation outputs to forked workflow");
    }

    var eventHistorySql =
        """
          INSERT INTO "%1$s".workflow_events_history
            (workflow_uuid, function_id, key, value, serialization)
          SELECT ? as workflow_uuid, function_id, key, value, serialization
            FROM "%1$s".workflow_events_history
            WHERE workflow_uuid = ? AND function_id < ?
        """
            .formatted(schema);
    try (PreparedStatement stmt = connection.prepareStatement(eventHistorySql)) {
      stmt.setString(1, forkedWorkflowId);
      stmt.setString(2, originalWorkflowId);
      stmt.setInt(3, startStep);

      int rowsCopied = stmt.executeUpdate();
      logger.debug("Copied " + rowsCopied + " workflow_events_history to forked workflow");
    }

    var eventSql =
        """
          INSERT INTO "%1$s".workflow_events
            (workflow_uuid, key, value, serialization)
          SELECT ?, weh1.key, weh1.value, weh1.serialization
            FROM "%1$s".workflow_events_history weh1
            WHERE weh1.workflow_uuid = ?
              AND weh1.function_id = (
                SELECT MAX(weh2.function_id)
                  FROM "%1$s".workflow_events_history weh2
                  WHERE weh2.workflow_uuid = ?
                    AND weh2.key = weh1.key
                    AND weh2.function_id < ?
              )
        """
            .formatted(schema);

    try (PreparedStatement stmt = connection.prepareStatement(eventSql)) {
      stmt.setString(1, forkedWorkflowId);
      stmt.setString(2, originalWorkflowId);
      stmt.setString(3, originalWorkflowId);
      stmt.setInt(4, startStep);

      int rowsCopied = stmt.executeUpdate();
      logger.debug("Copied " + rowsCopied + " workflow_events to forked workflow");
    }

    var streamsSql =
        """
          INSERT INTO "%1$s".streams
            (workflow_uuid, function_id, key, value, "offset", serialization)
          SELECT ? as workflow_uuid, function_id, key, value, "offset", serialization
            FROM "%1$s".streams
            WHERE workflow_uuid = ? AND function_id < ?
        """
            .formatted(schema);
    try (PreparedStatement stmt = connection.prepareStatement(streamsSql)) {
      stmt.setString(1, forkedWorkflowId);
      stmt.setString(2, originalWorkflowId);
      stmt.setInt(3, startStep);

      int rowsCopied = stmt.executeUpdate();
      logger.debug("Copied " + rowsCopied + " streams to forked workflow");
    }
  }

  private static Instant getRowsCutoff(Connection connection, long rowsThreshold, String schema)
      throws SQLException {
    String sql =
        """
          SELECT created_at FROM "%s".workflow_status ORDER BY created_at DESC OFFSET ? LIMIT 1
        """
            .formatted(schema);
    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setLong(1, rowsThreshold - 1);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return Instant.ofEpochMilli(rs.getLong("created_at"));
        }
      }
    }

    return null;
  }

  void garbageCollect(Instant cutoff, Long rowsThreshold) throws SQLException {

    try (Connection connection = dataSource.getConnection()) {
      if (rowsThreshold != null) {
        var rowsCutoff = getRowsCutoff(connection, rowsThreshold, this.schema);
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
                .formatted(this.schema);
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
          stmt.setLong(1, cutoff.toEpochMilli());
          stmt.setString(2, WorkflowState.PENDING.name());
          stmt.setString(3, WorkflowState.ENQUEUED.name());
          stmt.setString(4, WorkflowState.DELAYED.name());

          stmt.executeUpdate();
        }
      }
    }
  }
}
