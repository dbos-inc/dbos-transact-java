package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.ErrorResult;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;
import dev.dbos.transact.workflow.internal.InsertWorkflowResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.*;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowDAO {

  private final HikariDataSource dataSource;
  private static final Logger logger = LoggerFactory.getLogger(WorkflowDAO.class);

  WorkflowDAO(HikariDataSource ds) {
    dataSource = ds;
  }

  public Optional<String> getWorkflowResult(String workflowId) throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    String sql =
        "SELECT status, output, error FROM %s.workflow_status WHERE workflow_uuid = ?;"
            .formatted(Constants.DB_SCHEMA);

    try (Connection connection = dataSource.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {

      stmt.setString(1, workflowId);

      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          String status = rs.getString("status");

          if (WorkflowState.SUCCESS.toString().equals(status)) {
            String output = rs.getString("output");
            return Optional.ofNullable(output);

          } else if (WorkflowState.ERROR.toString().equals(status)) {
            String error = rs.getString("error");
            return Optional.ofNullable(error);
          }

          // For other statuses (PENDING, RUNNING, etc.), return empty
          return Optional.empty();
        }

        // No row found - return empty
        return Optional.empty();
      }

    } catch (SQLException e) {
      logger.error("Error getting workflow result", e);
      throw e;
    }
  }

  public WorkflowInitResult initWorkflowStatus(
      WorkflowStatusInternal initStatus, Integer maxRetries) throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    try (Connection connection = dataSource.getConnection()) {

      try {
        connection.setAutoCommit(false);
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

        InsertWorkflowResult resRow = insertWorkflowStatus(connection, initStatus);

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

        final int attempts = resRow.recoveryAttempts();
        if (maxRetries != null && attempts > maxRetries + 1) {

          var sql =
              """
              UPDATE %s.workflow_status
              SET status = ?, deduplication_id = NULL, started_at_epoch_ms = NULL, queue_name = NULL
              WHERE workflow_uuid = ? AND status = ?
              """
                  .formatted(Constants.DB_SCHEMA);

          try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, WorkflowState.MAX_RECOVERY_ATTEMPTS_EXCEEDED.toString());
            stmt.setString(2, initStatus.workflowId());
            stmt.setString(3, WorkflowState.PENDING.toString());

            stmt.executeUpdate();
          }

          throw new DBOSMaxRecoveryAttemptsExceededException(initStatus.workflowId(), maxRetries);
        }

        return new WorkflowInitResult(
            initStatus.workflowId(), resRow.status(), resRow.deadlineEpochMs());

      } finally {
        connection.commit();
      }
    } // end try with resources connection closed
  }

  /**
   * Insert into the workflow_status table
   *
   * @param status @WorkflowStatusInternal holds the data for a workflow_status row
   * @return @InsertWorkflowResult some of the column inserted
   * @throws SQLException
   */
  public InsertWorkflowResult insertWorkflowStatus(
      Connection connection, WorkflowStatusInternal status) throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    String insertSQL =
        """
        INSERT INTO %s.workflow_status (
        workflow_uuid, status, inputs,
        name, class_name, config_name,
        queue_name, deduplication_id, priority,
        authenticated_user, assumed_role, authenticated_roles,
        executor_id, application_version, application_id,
        created_at, updated_at, recovery_attempts,
        workflow_timeout_ms, workflow_deadline_epoch_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (workflow_uuid)
          DO UPDATE SET
            recovery_attempts = CASE
                WHEN EXCLUDED.status != 'ENQUEUED'
                  THEN workflow_status.recovery_attempts + 1
                  ELSE workflow_status.recovery_attempts
            END,
            updated_at = EXCLUDED.updated_at,
            executor_id = CASE
                WHEN EXCLUDED.status = 'ENQUEUED'
                  THEN workflow_status.executor_id
                  ELSE EXCLUDED.executor_id
            END
        RETURNING recovery_attempts, status, name, class_name, config_name, queue_name, workflow_timeout_ms, workflow_deadline_epoch_ms
        """
            .formatted(Constants.DB_SCHEMA);

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

      stmt.setString(10, status.authenticatedUser());
      stmt.setString(11, status.assumedRole());
      stmt.setString(12, status.authenticatedRoles());

      stmt.setString(13, status.executorId());
      stmt.setString(14, status.appVersion());
      stmt.setString(15, status.appId());

      stmt.setLong(16, now); // created_at
      stmt.setLong(17, now); // updated_at
      stmt.setInt(18, recoveryAttempts);

      stmt.setObject(19, status.timeoutMs());
      stmt.setObject(20, status.deadlineEpochMs());

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
                  rs.getObject("workflow_deadline_epoch_ms", Long.class));

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

  public void updateWorkflowOutcome(
      Connection connection, String workflowId, WorkflowState status, String output, String error)
      throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    // Note that transitions from CANCELLED to SUCCESS or ERROR are forbidden
    var sql =
        """
      UPDATE %s.workflow_status
      SET status = ?, output = ?, error = ?, updated_at = ?, deduplication_id = NULL
      WHERE workflow_uuid = ? AND NOT (status = ? AND ? in (?, ?))
      """
            .formatted(Constants.DB_SCHEMA);

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
  public void recordWorkflowOutput(String workflowId, String result) throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

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
  public void recordWorkflowError(String workflowId, String error) throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    try (Connection connection = dataSource.getConnection()) {
      updateWorkflowOutcome(connection, workflowId, WorkflowState.ERROR, null, error);
    }
  }

  public WorkflowStatus getWorkflowStatus(String workflowId) throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    var input = new ListWorkflowsInput().withWorkflowId(workflowId);
    List<WorkflowStatus> output = listWorkflows(input);
    if (output.size() > 0) {
      return output.get(0);
    }

    return null;
  }

  public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

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
          SELECT workflow_uuid, status, name, config_name, class_name,
          authenticated_user, assumed_role, authenticated_roles,
          executor_id, created_at, updated_at, application_version, application_id,
          recovery_attempts, queue_name, workflow_timeout_ms, workflow_deadline_epoch_ms,
          started_at_epoch_ms, deduplication_id, priority
        """);

    var loadInput = input.loadInput() == null || input.loadInput();
    var loadOutput = input.loadOutput() == null || input.loadOutput();
    if (loadInput) {
      sqlBuilder.append(", inputs");
    }
    if (loadOutput) {
      sqlBuilder.append(", output, error");
    }

    sqlBuilder.append(" FROM %s.workflow_status ".formatted(Constants.DB_SCHEMA));

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
          var workflow_uuid = rs.getString("workflow_uuid");
          String authenticatedRolesJson = rs.getString("authenticated_roles");
          String serializedInput = loadInput ? rs.getString("inputs") : null;
          String serializedOutput = loadOutput ? rs.getString("output") : null;
          String serializedError = loadOutput ? rs.getString("error") : null;
          ErrorResult err = null;
          if (serializedError != null) {
            var wrapper = JSONUtil.deserializeAppExceptionWrapper(serializedError);
            Throwable throwable = null;
            try {
              throwable = JSONUtil.deserializeAppException(serializedError);
            } catch (Exception e) {
              throw new RuntimeException(
                  "Failed to deserialize error for workflow " + workflow_uuid, e);
            }
            err = new ErrorResult(wrapper.type, wrapper.message, serializedError, throwable);
          }
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
                  (serializedOutput != null)
                      ? JSONUtil.deserializeToArray(serializedOutput)[0]
                      : null,
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
                  rs.getObject("priority", Integer.class));

          workflows.add(info);
        }
      }
    }

    return workflows;
  }

  public List<GetPendingWorkflowsOutput> getPendingWorkflows(String executorId, String appVersion)
      throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    final String sql =
        """
        SELECT workflow_uuid, queue_name
        FROM %s.workflow_status
        WHERE status = ?
          AND executor_id = ?
          AND application_version = ?
        """
            .formatted(Constants.DB_SCHEMA);

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

  public <T, E extends Exception> T awaitWorkflowResult(String workflowId) throws E {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    final String sql =
        """
        SELECT status, output, error
        FROM %s.workflow_status
        WHERE workflow_uuid = ?
        """
            .formatted(Constants.DB_SCHEMA);

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
                return (T) oArray[0];

              case ERROR:
                String error = rs.getString("error");
                Throwable t = JSONUtil.deserializeAppException(error);
                if (t instanceof Exception) {
                  throw (E) t;
                }
                throw new RuntimeException(t.getMessage(), t);
              case CANCELLED:
                throw new DBOSAwaitedWorkflowCancelledException(workflowId);

              default:
                // Status is PENDING or other - continue polling
                break;
            }
          }
          // Row not found - workflow hasn't appeared yet, continue polling
        }
      } catch (SQLException e) {
        logger.error("Database error while polling workflow {}", workflowId, e);
      }

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException("Workflow polling interrupted for " + workflowId, e);
      }
    }
  }

  public void recordChildWorkflow(
      String parentId,
      String childId, // workflowId of the
      // child
      int functionId, // func id in the parent
      String functionName)
      throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    String sql =
        "INSERT INTO %s.operation_outputs (workflow_uuid, function_id, function_name, child_workflow_id) VALUES (?, ?, ?, ?)"
            .formatted(Constants.DB_SCHEMA);

    try {
      try (Connection connection = dataSource.getConnection();
          PreparedStatement pStmt = connection.prepareStatement(sql)) {

        pStmt.setString(1, parentId);
        pStmt.setInt(2, functionId);
        pStmt.setString(3, functionName);
        pStmt.setString(4, childId);

        pStmt.executeUpdate();
      }
    } catch (SQLException sqe) {
      if ("23505".equals(sqe.getSQLState())) {
        throw new DBOSWorkflowExecutionConflictException(parentId);
      } else {
        throw sqe;
      }
    }
  }

  public Optional<String> checkChildWorkflow(String workflowUuid, int functionId)
      throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }
    final String sql =
        "SELECT child_workflow_id FROM %s.operation_outputs WHERE workflow_uuid = ? AND function_id = ? "
            .formatted(Constants.DB_SCHEMA);

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

  public void cancelWorkflow(String workflowId) throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    try (Connection conn = dataSource.getConnection()) {

      // Check the status of the workflow. If it is complete, do nothing.
      String checkStatusSql =
          " SELECT status FROM %s.workflow_status WHERE workflow_uuid = ? "
              .formatted(Constants.DB_SCHEMA);

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
          UPDATE %s.workflow_status
          SET status = ?,
              queue_name = NULL,
              deduplication_id = NULL,
              started_at_epoch_ms = NULL
          WHERE workflow_uuid = ?
          """
              .formatted(Constants.DB_SCHEMA);

      try (PreparedStatement stmt = conn.prepareStatement(updateSql)) {
        stmt.setString(1, WorkflowState.CANCELLED.name());
        stmt.setString(2, workflowId);
        stmt.executeUpdate();
      }
    }
  }

  public void resumeWorkflow(String workflowId) throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);
      connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

      try {
        String currentStatus = getWorkflowStatus(connection, workflowId);

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
        updateWorkflowToEnqueued(connection, workflowId);

        connection.commit();

      } catch (SQLException e) {
        connection.rollback();
        throw e;
      }
    }
  }

  public String forkWorkflow(String originalWorkflowId, int startStep, ForkOptions options)
      throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

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

    var timeout = options.timeout();
    if (timeout == null) {
      timeout = status.getTimeout();
    }
    if (timeout == null) {
      timeout = Duration.ZERO;
    }

    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);

      try {
        // Create entry for forked workflow
        insertForkedWorkflowStatus(
            connection, forkedWorkflowId, status, applicationVersion, timeout.toMillis());

        // Copy operation outputs if starting from step > 0
        if (startStep > 0) {
          copyOperationOutputs(connection, originalWorkflowId, forkedWorkflowId, startStep);
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
      String forkedWorkflowId,
      WorkflowStatus originalStatus,
      String applicationVersion,
      long timeoutMs)
      throws SQLException {

    long workflowDeadlineEpoch = 0;
    if (timeoutMs > 0) {
      workflowDeadlineEpoch = System.currentTimeMillis() + timeoutMs;
    }

    String sql =
        """
        INSERT INTO %s.workflow_status (
        workflow_uuid, status, name, class_name, config_name, application_version, application_id,
        authenticated_user, authenticated_roles, assumed_role, queue_name, inputs, workflow_deadline_epoch_ms, workflow_timeout_ms
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
            .formatted(Constants.DB_SCHEMA);

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, forkedWorkflowId);
      stmt.setString(2, WorkflowState.ENQUEUED.name());
      stmt.setString(3, originalStatus.name());
      stmt.setString(4, originalStatus.className());
      stmt.setString(5, originalStatus.instanceName());

      // Use provided application version or fall back to original
      String appVersion =
          applicationVersion != null ? applicationVersion : originalStatus.appVersion();
      stmt.setString(6, appVersion);

      stmt.setString(7, originalStatus.appId());
      stmt.setString(8, originalStatus.authenticatedUser());
      stmt.setString(9, JSONUtil.serializeArray(originalStatus.authenticatedRoles()));
      stmt.setString(10, originalStatus.assumedRole());
      stmt.setString(11, Constants.DBOS_INTERNAL_QUEUE);
      stmt.setString(12, JSONUtil.serializeArray(originalStatus.input()));
      stmt.setLong(13, workflowDeadlineEpoch);
      stmt.setObject(14, originalStatus.timeoutMs());

      stmt.executeUpdate();
    }
  }

  private static void copyOperationOutputs(
      Connection connection, String originalWorkflowId, String forkedWorkflowId, int startStep)
      throws SQLException {

    String sql =
        """
        INSERT INTO %1$s.operation_outputs
            (workflow_uuid, function_id, output, error, function_name, child_workflow_id )
        SELECT ? as workflow_uuid, function_id, output, error, function_name, child_workflow_id
            FROM %1$s.operation_outputs
            WHERE workflow_uuid = ? AND function_id < ?
        """
            .formatted(Constants.DB_SCHEMA);

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, forkedWorkflowId);
      stmt.setString(2, originalWorkflowId);
      stmt.setInt(3, startStep);

      int rowsCopied = stmt.executeUpdate();
      System.out.println("Copied " + rowsCopied + " operation outputs to forked workflow");
    }
  }

  /*
   * public String forkWorkflow(String originalWorkflowId, String
   * forkedWorkflowId, int startStep) throws SQLException { return
   * forkWorkflow(originalWorkflowId, forkedWorkflowId, startStep, null); }
   */

  private static String getWorkflowStatus(Connection connection, String workflowId)
      throws SQLException {
    String sql =
        "SELECT status FROM %s.workflow_status WHERE workflow_uuid = ?"
            .formatted(Constants.DB_SCHEMA);

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

  private static void updateWorkflowToEnqueued(Connection connection, String workflowId)
      throws SQLException {
    String sql =
        """
        UPDATE %s.workflow_status
        SET status = ?, queue_name = ?, recovery_attempts = ?, workflow_deadline_epoch_ms = 0, deduplication_id = NULL,  started_at_epoch_ms = NULL
        WHERE workflow_uuid = ?
        """
            .formatted(Constants.DB_SCHEMA);

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, WorkflowState.ENQUEUED.name());
      stmt.setString(2, Constants.DBOS_INTERNAL_QUEUE);
      stmt.setInt(3, 0); // recovery_attempts = 0
      stmt.setString(4, workflowId);

      stmt.executeUpdate();
    }
  }

  private static Long getRowsCutoff(Connection connection, long rowsThreshold) throws SQLException {
    String sql =
        "SELECT created_at FROM %s.workflow_status ORDER BY created_at DESC OFFSET ? LIMIT 1"
            .formatted(Constants.DB_SCHEMA);
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

  public void garbageCollect(Long cutoffEpochTimestampMs, Long rowsThreshold) throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    try (Connection connection = dataSource.getConnection()) {
      if (rowsThreshold != null) {
        Long rowsCutoff = getRowsCutoff(connection, rowsThreshold);
        if (rowsCutoff != null) {
          if (cutoffEpochTimestampMs == null || rowsCutoff > cutoffEpochTimestampMs) {
            cutoffEpochTimestampMs = rowsCutoff;
          }
        }
      }

      if (cutoffEpochTimestampMs != null) {
        String sql =
            "DELETE FROM %s.workflow_status WHERE created_at < ? AND status NOT IN (?, ?)"
                .formatted(Constants.DB_SCHEMA);
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
