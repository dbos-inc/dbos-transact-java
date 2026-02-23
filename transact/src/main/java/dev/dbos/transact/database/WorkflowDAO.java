package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.ErrorResult;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.Timeout;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;
import dev.dbos.transact.workflow.internal.StepResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.*;
import java.time.Instant;
import java.util.*;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class WorkflowDAO {

  private static final Logger logger = LoggerFactory.getLogger(WorkflowDAO.class);

  private final DataSource dataSource;
  private final String schema;
  private long getResultPollingIntervalMs = 1000;

  WorkflowDAO(DataSource ds, String schema) {
    this.dataSource = ds;
    this.schema = Objects.requireNonNull(schema);
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

        if (!Objects.equals(resRow.name(), initStatus.name())) {
          String msg =
              String.format(
                  "Workflow already exists with a different function name: %s, but the provided function name is: %s",
                  resRow.name(), initStatus.name());
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

        // If there is an existing DB record and we aren't here to recover it,
        //  leave it be.  Roll back the change to max recovery attempts.
        if (!ownerXid.equals(resRow.ownerXid) && !isRecoveryRequest && !isDequeuedRequest) {
          if (resRow.status.equals(WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED.toString())) {
            throw new DBOSMaxRecoveryAttemptsExceededException(initStatus.workflowId(), maxRetries);
          }
          return new WorkflowInitResult(
              initStatus.workflowId(), resRow.status(), resRow.deadlineEpochMs(), false);
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
            stmt.setString(1, WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED.toString());
            stmt.setString(2, initStatus.workflowId());
            stmt.setString(3, WorkflowState.PENDING.toString());

            stmt.executeUpdate();
          }

          throw new DBOSMaxRecoveryAttemptsExceededException(initStatus.workflowId(), maxRetries);
        }

        return new WorkflowInitResult(
            initStatus.workflowId(), resRow.status(), resRow.deadlineEpochMs(), true);

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
      String name,
      String className,
      String instanceName,
      String queueName,
      Long timeoutMs,
      Long deadlineEpochMs,
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
            queue_name, deduplication_id, priority, queue_partition_key,
            authenticated_user, assumed_role, authenticated_roles,
            executor_id, application_version, application_id,
            created_at, updated_at, recovery_attempts,
            workflow_timeout_ms, workflow_deadline_epoch_ms,
            parent_workflow_id, owner_xid
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT (workflow_uuid)
            DO UPDATE SET
              recovery_attempts = CASE
                  WHEN EXCLUDED.status != 'ENQUEUED'
                    THEN workflow_status.recovery_attempts + ?
                    ELSE workflow_status.recovery_attempts
              END,
              updated_at = EXCLUDED.updated_at,
              executor_id = CASE
                  WHEN EXCLUDED.status = 'ENQUEUED'
                    THEN workflow_status.executor_id
                    ELSE EXCLUDED.executor_id
              END
          RETURNING recovery_attempts, status, name, class_name, config_name, queue_name, workflow_timeout_ms, workflow_deadline_epoch_ms, owner_xid
        """
            .formatted(this.schema);

    try (PreparedStatement stmt = connection.prepareStatement(insertSQL)) {

      var now = Instant.now().toEpochMilli();
      var recoveryAttempts = status.status() == WorkflowState.ENQUEUED ? 0 : 1;
      int priority = status.priority() == null ? 0 : status.priority();

      stmt.setString(1, status.workflowId());
      stmt.setString(2, status.status().toString());
      stmt.setString(3, status.inputs());

      stmt.setString(4, status.name());
      stmt.setString(5, status.className());
      stmt.setString(6, status.instanceName());

      stmt.setString(7, status.queueName());
      stmt.setString(8, status.deduplicationId());
      stmt.setInt(9, priority);
      stmt.setString(10, status.queuePartitionKey());

      stmt.setString(11, status.authenticatedUser());
      stmt.setString(12, status.assumedRole());
      stmt.setString(13, status.authenticatedRoles());

      stmt.setString(14, status.executorId());
      stmt.setString(15, status.appVersion());
      stmt.setString(16, status.appId());

      stmt.setLong(17, now); // created_at
      stmt.setLong(18, now); // updated_at
      stmt.setInt(19, recoveryAttempts);

      stmt.setObject(20, status.timeoutMs());
      stmt.setObject(21, status.deadlineEpochMs());
      stmt.setString(22, status.parentWorkflowId());

      stmt.setObject(23, ownerXid);
      stmt.setInt(24, incrementAttempts ? 1 : 0);

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
                  rs.getObject("workflow_timeout_ms", Long.class),
                  rs.getObject("workflow_deadline_epoch_ms", Long.class),
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
      stmt.setString(1, status.toString());
      stmt.setString(2, output);
      stmt.setString(3, error);
      stmt.setLong(4, Instant.now().toEpochMilli());
      stmt.setString(5, workflowId);
      stmt.setString(6, WorkflowState.CANCELLED.toString());
      stmt.setString(7, status.toString());
      stmt.setString(8, WorkflowState.SUCCESS.toString());
      stmt.setString(9, WorkflowState.ERROR.toString());

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

  WorkflowStatus getWorkflowStatus(String workflowId) throws SQLException {

    try (var conn = dataSource.getConnection()) {
      return getWorkflowStatus(conn, workflowId);
    }
  }

  WorkflowStatus getWorkflowStatus(Connection conn, String workflowId) throws SQLException {
    var sql =
        """
          SELECT
            workflow_uuid, status,
            name, class_name, config_name,
            inputs, output, error,
            queue_name, deduplication_id, priority, queue_partition_key,
            executor_id, application_version, application_id,
            authenticated_user, assumed_role, authenticated_roles,
            created_at, updated_at, recovery_attempts, started_at_epoch_ms,
            workflow_timeout_ms, workflow_deadline_epoch_ms,
            forked_from, parent_workflow_id
            FROM "%s".workflow_status
            WHERE workflow_uuid = ?
        """
            .formatted(this.schema);

    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        if (rs.next()) {
          return resultsToWorkflowStatus(rs, true, true);
        }
      }
    }

    return null;
  }

  List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) throws SQLException {

    if (input == null) {
      input = new ListWorkflowsInput();
    }

    List<WorkflowStatus> workflows = new ArrayList<>();

    StringBuilder sqlBuilder = new StringBuilder();
    List<Object> parameters = new ArrayList<>();

    // Start building the SELECT clause. The order of columns here is critical
    // for mapping to the WorkflowStatus fields by index later in the ResultSet.
    sqlBuilder.append(
        """
          SELECT
            workflow_uuid, status,
            name, class_name, config_name,
            queue_name, deduplication_id, priority, queue_partition_key,
            executor_id, application_version, application_id,
            authenticated_user, assumed_role, authenticated_roles,
            created_at, updated_at, recovery_attempts, started_at_epoch_ms,
            workflow_timeout_ms, workflow_deadline_epoch_ms,
            forked_from, parent_workflow_id
        """);

    var loadInput = input.loadInput() == null || input.loadInput();
    var loadOutput = input.loadOutput() == null || input.loadOutput();
    if (loadInput) {
      sqlBuilder.append(", inputs");
    }
    if (loadOutput) {
      sqlBuilder.append(", output, error");
    }

    sqlBuilder.append(" FROM \"%s\".workflow_status ".formatted(this.schema));

    // --- WHERE Clauses ---
    StringJoiner whereConditions = new StringJoiner(" AND ");

    if (input.workflowName() != null) {
      whereConditions.add("name = ?");
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
    if (input.queueName() != null) {
      whereConditions.add("queue_name = ?");
      parameters.add(input.queueName());
    }
    if (input.queuesOnly() != null && input.queuesOnly()) {
      whereConditions.add("queue_name IS NOT NULL");
    }
    if (input.forkedFrom() != null) {
      whereConditions.add("forked_from = ?");
      parameters.add(input.forkedFrom());
    }
    if (input.parentWorkflowId() != null) {
      whereConditions.add("parent_workflow_id = ?");
      parameters.add(input.parentWorkflowId());
    }
    if (input.workflowIdPrefix() != null) {
      whereConditions.add("workflow_uuid LIKE ?");
      // Append wildcard directly to the parameter value
      parameters.add(input.workflowIdPrefix() + "%");
    }
    if (input.workflowIds() != null) {
      whereConditions.add("workflow_uuid = ANY(?)");
      parameters.add(input.workflowIds().toArray());
    }
    if (input.authenticatedUser() != null) {
      whereConditions.add("authenticated_user = ?");
      parameters.add(input.authenticatedUser());
    }
    if (input.startTime() != null) {
      whereConditions.add("created_at >= ?");
      // Convert OffsetDateTime to epoch milliseconds for comparison with DB column
      parameters.add(input.startTime().toInstant().toEpochMilli());
    }
    if (input.endTime() != null) {
      whereConditions.add("created_at <= ?");
      // Convert OffsetDateTime to epoch milliseconds for comparison with DB column
      parameters.add(input.endTime().toInstant().toEpochMilli());
    }
    if (input.status() != null) {
      whereConditions.add("status = ANY(?)");
      parameters.add(input.status().toArray());
    }
    if (input.applicationVersion() != null) {
      whereConditions.add("application_version = ?");
      parameters.add(input.applicationVersion());
    }
    if (input.executorIds() != null) {
      whereConditions.add("executor_id = ANY(?)");
      parameters.add(input.executorIds().toArray());
    }

    // Only append WHERE keyword if there are actual conditions
    if (whereConditions.length() > 0) {
      sqlBuilder.append(" WHERE ").append(whereConditions.toString());
    }

    // --- ORDER BY Clause ---
    sqlBuilder.append(" ORDER BY created_at ");
    if (input != null && input.sortDesc() != null && input.sortDesc()) {
      sqlBuilder.append("DESC");
    } else {
      sqlBuilder.append("ASC");
    }

    // --- LIMIT and OFFSET Clauses ---
    if (input != null) {
      if (input.limit() != null) {
        sqlBuilder.append(" LIMIT ?");
        parameters.add(input.limit());
      }
      if (input.offset() != null) {
        sqlBuilder.append(" OFFSET ?");
        parameters.add(input.offset());
      }
    }

    try (Connection connection = dataSource.getConnection();
        PreparedStatement pstmt = connection.prepareStatement(sqlBuilder.toString())) {

      for (int i = 0; i < parameters.size(); i++) {

        Object param = parameters.get(i);
        if (param instanceof String v) {
          pstmt.setString(i + 1, v);
        } else if (param instanceof Long v) {
          pstmt.setLong(i + 1, v);
        } else if (param instanceof Integer v) {
          pstmt.setInt(i + 1, v);
        } else if (param instanceof Object[] v) {
          Array sqlArray = connection.createArrayOf("text", v);
          pstmt.setArray(i + 1, sqlArray);
        } else {
          // Fallback for other types, or if OffsetDateTime was directly added
          // to
          // parameters list
          pstmt.setObject(i + 1, param);
        }
      }

      try (ResultSet rs = pstmt.executeQuery()) {
        while (rs.next()) {
          WorkflowStatus info = resultsToWorkflowStatus(rs, loadInput, loadOutput);
          workflows.add(info);
        }
      }
    }

    return workflows;
  }

  private static WorkflowStatus resultsToWorkflowStatus(
      ResultSet rs, boolean loadInput, boolean loadOutput) throws SQLException {
    var workflow_uuid = rs.getString("workflow_uuid");
    String authenticatedRolesJson = rs.getString("authenticated_roles");
    String serializedInput = loadInput ? rs.getString("inputs") : null;
    String serializedOutput = loadOutput ? rs.getString("output") : null;
    String serializedError = loadOutput ? rs.getString("error") : null;
    ErrorResult err = ErrorResult.deserialize(serializedError);
    WorkflowStatus info =
        new WorkflowStatus(
            workflow_uuid,
            rs.getString("status"),
            rs.getString("name"),
            rs.getString("class_name"),
            rs.getString("config_name"),
            rs.getString("authenticated_user"),
            rs.getString("assumed_role"),
            (authenticatedRolesJson != null)
                ? (String[]) JSONUtil.deserializeToArray(authenticatedRolesJson)
                : null,
            (serializedInput != null) ? JSONUtil.deserializeToArray(serializedInput) : null,
            (serializedOutput != null) ? JSONUtil.deserializeToArray(serializedOutput)[0] : null,
            err,
            rs.getString("executor_id"),
            rs.getObject("created_at", Long.class),
            rs.getObject("updated_at", Long.class),
            rs.getString("application_version"),
            rs.getString("application_id"),
            rs.getInt("recovery_attempts"),
            rs.getString("queue_name"),
            rs.getObject("workflow_timeout_ms", Long.class),
            rs.getObject("workflow_deadline_epoch_ms", Long.class),
            rs.getObject("started_at_epoch_ms", Long.class),
            rs.getString("deduplication_id"),
            rs.getObject("priority", Integer.class),
            rs.getString("queue_partition_key"),
            rs.getString("forked_from"),
            rs.getString("parent_workflow_id"));
    return info;
  }

  List<GetPendingWorkflowsOutput> getPendingWorkflows(String executorId, String appVersion)
      throws SQLException {

    final String sql =
        """
          SELECT workflow_uuid, queue_name
          FROM "%s".workflow_status
          WHERE status = ?
            AND executor_id = ?
            AND application_version = ?
        """
            .formatted(this.schema);

    List<GetPendingWorkflowsOutput> results = new ArrayList<>();

    try (Connection connection = dataSource.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {

      stmt.setString(1, WorkflowState.PENDING.name());
      stmt.setString(2, executorId);
      stmt.setString(3, appVersion);

      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          results.add(
              new GetPendingWorkflowsOutput(
                  rs.getString("workflow_uuid"), rs.getString("queue_name")));
        }
      }
    }

    return results;
  }

  @SuppressWarnings("unchecked")
  <T> Result<T> awaitWorkflowResult(String workflowId) throws SQLException {

    final String sql =
        """
          SELECT status, output, error
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

            switch (WorkflowState.valueOf(status.toUpperCase())) {
              case SUCCESS:
                String output = rs.getString("output");
                Object[] oArray = JSONUtil.deserializeToArray(output);
                return Result.success((T) oArray[0]);

              case ERROR:
                String error = rs.getString("error");
                Throwable t = JSONUtil.deserializeAppException(error);
                return Result.failure(t);
              case CANCELLED:
                throw new DBOSAwaitedWorkflowCancelledException(workflowId);

              default:
                // Status is PENDING or other - continue polling
                break;
            }
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

    var result = new StepResult(parentId, functionId, functionName).withChildWorkflowId(childId);
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

  void cancelWorkflow(String workflowId) throws SQLException {

    try (Connection conn = dataSource.getConnection()) {

      // Check the status of the workflow. If it is complete, do nothing.
      String checkStatusSql =
          """
            SELECT status FROM "%s".workflow_status WHERE workflow_uuid = ?
          """
              .formatted(this.schema);

      String currentStatus = null;
      try (PreparedStatement stmt = conn.prepareStatement(checkStatusSql)) {
        stmt.setString(1, workflowId);
        try (ResultSet rs = stmt.executeQuery()) {
          if (rs.next()) {
            currentStatus = rs.getString("status");
          }
        }
      }

      // If workflow doesn't exist or is already complete, do nothing
      if (currentStatus == null
          || WorkflowState.SUCCESS.name().equals(currentStatus)
          || WorkflowState.ERROR.name().equals(currentStatus)) {
        logger.debug("Workflow {} already complete, aborting cancelWorkflow", workflowId);
        return;
      }

      // Set the workflow's status to CANCELLED and remove it from any queue it is
      // on
      String updateSql =
          """
            UPDATE "%s".workflow_status
            SET status = ?,
                queue_name = NULL,
                deduplication_id = NULL,
                started_at_epoch_ms = NULL
            WHERE workflow_uuid = ?
          """
              .formatted(this.schema);

      try (PreparedStatement stmt = conn.prepareStatement(updateSql)) {
        stmt.setString(1, WorkflowState.CANCELLED.name());
        stmt.setString(2, workflowId);
        stmt.executeUpdate();
      }
    }
  }

  void resumeWorkflow(String workflowId) throws SQLException {

    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      try {
        String currentStatus = getWorkflowStatus(connection, workflowId, this.schema);

        if (currentStatus == null) {
          connection.rollback();
          throw new DBOSNonExistentWorkflowException(workflowId);
        }

        // If workflow is already complete, do nothing
        if (WorkflowState.SUCCESS.name().equals(currentStatus)
            || WorkflowState.ERROR.name().equals(currentStatus)) {
          connection.rollback();
          return;
        }

        // Set the workflow's status to ENQUEUED and clear recovery fields
        updateWorkflowToEnqueued(connection, workflowId, this.schema);

        connection.commit();

      } catch (SQLException e) {
        connection.rollback();
        throw e;
      }
    }
  }

  String forkWorkflow(String originalWorkflowId, int startStep, ForkOptions options)
      throws SQLException {

    Objects.requireNonNull(options);

    var status = getWorkflowStatus(originalWorkflowId);
    if (status == null) {
      throw new DBOSNonExistentWorkflowException(originalWorkflowId);
    }

    String forkedWorkflowId =
        options.forkedWorkflowId() == null
            ? UUID.randomUUID().toString()
            : options.forkedWorkflowId();

    logger.debug("forkWorkflow Original id {} forked id {}", originalWorkflowId, forkedWorkflowId);

    String applicationVersion = options.applicationVersion();

    var timeout = Objects.requireNonNullElse(options.timeout(), Timeout.inherit());
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
            applicationVersion,
            timeoutMS,
            this.schema);

        // Copy operation outputs if starting from step > 0
        if (startStep > 0) {
          copyOperationOutputs(
              connection, originalWorkflowId, forkedWorkflowId, startStep, this.schema);
        }

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
      String schema)
      throws SQLException {
    Objects.requireNonNull(schema);

    String sql =
        """
          INSERT INTO "%s".workflow_status (
            workflow_uuid, status, name, class_name, config_name, application_version, application_id,
            authenticated_user, authenticated_roles, assumed_role, queue_name, inputs, workflow_timeout_ms, forked_from
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
            .formatted(schema);

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, forkedWorkflowId);
      stmt.setString(2, WorkflowState.ENQUEUED.name());
      stmt.setString(3, originalStatus.name());
      stmt.setString(4, originalStatus.className());
      stmt.setString(5, originalStatus.instanceName());
      stmt.setString(6, applicationVersion);
      stmt.setString(7, originalStatus.appId());
      stmt.setString(8, originalStatus.authenticatedUser());
      stmt.setString(9, JSONUtil.serializeArray(originalStatus.authenticatedRoles()));
      stmt.setString(10, originalStatus.assumedRole());
      stmt.setString(11, Constants.DBOS_INTERNAL_QUEUE);
      stmt.setString(12, JSONUtil.serializeArray(originalStatus.input()));
      stmt.setObject(13, timeoutMS);
      stmt.setString(14, originalWorkflowId);

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
              (workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms)
          SELECT ? as workflow_uuid, function_id, output, error, function_name, child_workflow_id, started_at_epoch_ms, completed_at_epoch_ms
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
            (workflow_uuid, function_id, key, value)
          SELECT ? as workflow_uuid, function_id, key, value
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
            (workflow_uuid, key, value)
          SELECT ?, weh1.key, weh1.value
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
  }

  private static String getWorkflowStatus(Connection connection, String workflowId, String schema)
      throws SQLException {
    String sql =
        """
          SELECT status FROM "%s".workflow_status WHERE workflow_uuid = ?
        """
            .formatted(schema);

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, workflowId);

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return rs.getString("status");
        }
        return null;
      }
    }
  }

  private static void updateWorkflowToEnqueued(
      Connection connection, String workflowId, String schema) throws SQLException {
    String sql =
        """
          UPDATE "%s".workflow_status
          SET status = ?, queue_name = ?, recovery_attempts = ?, workflow_deadline_epoch_ms = 0, deduplication_id = NULL,  started_at_epoch_ms = NULL
          WHERE workflow_uuid = ?
        """
            .formatted(schema);

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, WorkflowState.ENQUEUED.name());
      stmt.setString(2, Constants.DBOS_INTERNAL_QUEUE);
      stmt.setInt(3, 0); // recovery_attempts = 0
      stmt.setString(4, workflowId);

      stmt.executeUpdate();
    }
  }

  private static Long getRowsCutoff(Connection connection, long rowsThreshold, String schema)
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
          return rs.getLong("created_at");
        }
      }
    }

    return null;
  }

  void garbageCollect(Long cutoffEpochTimestampMs, Long rowsThreshold) throws SQLException {

    try (Connection connection = dataSource.getConnection()) {
      if (rowsThreshold != null) {
        Long rowsCutoff = getRowsCutoff(connection, rowsThreshold, this.schema);
        if (rowsCutoff != null) {
          if (cutoffEpochTimestampMs == null || rowsCutoff > cutoffEpochTimestampMs) {
            cutoffEpochTimestampMs = rowsCutoff;
          }
        }
      }

      if (cutoffEpochTimestampMs != null) {
        String sql =
            """
              DELETE FROM "%s".workflow_status WHERE created_at < ? AND status NOT IN (?, ?)
            """
                .formatted(this.schema);
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
          stmt.setLong(1, cutoffEpochTimestampMs);
          stmt.setString(2, WorkflowState.PENDING.toString());
          stmt.setString(3, WorkflowState.ENQUEUED.toString());

          stmt.executeUpdate();
        }
      }
    }
  }
}
