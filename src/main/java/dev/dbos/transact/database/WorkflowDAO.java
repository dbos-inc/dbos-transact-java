package dev.dbos.transact.database;

import static dev.dbos.transact.exceptions.ErrorCode.UNEXPECTED;

import dev.dbos.transact.Constants;
import dev.dbos.transact.exceptions.*;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;
import dev.dbos.transact.workflow.internal.InsertWorkflowResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.*;
import java.time.Instant;
import java.util.*;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowDAO {

    private final DataSource dataSource;
    private final Logger logger = LoggerFactory.getLogger(WorkflowDAO.class);

    WorkflowDAO(DataSource ds) {
        dataSource = ds;
    }

    public Optional<String> getWorkflowResult(String workflowId) throws SQLException {
        final String sql = "SELECT status, output, error " + "FROM dbos.workflow_status "
                + "WHERE workflow_uuid = ?;";

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

    public WorkflowInitResult initWorkflowStatus(WorkflowStatusInternal initStatus,
            Integer maxRetries) throws SQLException {

        try (Connection connection = dataSource.getConnection()) {

            try {
                connection.setAutoCommit(false);
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

                InsertWorkflowResult resRow = insertWorkflowStatus(connection, initStatus);

                if (!Objects.equals(resRow.getName(), initStatus.getName())) {
                    String msg = String.format(
                            "Workflow already exists with a different function name: %s, but the provided function name is: %s",
                            resRow.getName(),
                            initStatus.getName());
                    throw new DBOSWorkflowConflictException(initStatus.getWorkflowUUID(), msg);
                } else if (!Objects.equals(resRow.getClassName(), initStatus.getClassName())) {
                    String msg = String.format(
                            "Workflow already exists with a different class name: %s, but the provided class name is: %s",
                            resRow.getClassName(),
                            initStatus.getClassName());
                    throw new DBOSWorkflowConflictException(initStatus.getWorkflowUUID(), msg);
                } else if (!Objects.equals(
                        resRow.getConfigName() != null ? resRow.getConfigName() : "",
                        initStatus.getConfigName() != null ? initStatus.getConfigName() : "")) {
                    String msg = String.format(
                            "Workflow already exists with a different class configuration: %s, but the provided class configuration is: %s",
                            resRow.getConfigName(),
                            initStatus.getConfigName());
                    throw new DBOSWorkflowConflictException(initStatus.getWorkflowUUID(), msg);
                }

                final int attempts = resRow.getRecoveryAttempts();
                if (maxRetries != null && attempts > maxRetries + 1) {

                    UpdateWorkflowOptions options = new UpdateWorkflowOptions();
                    options.setWhereStatus(WorkflowState.PENDING.toString());
                    options.setThrowOnFailure(false);

                    updateWorkflowStatus(connection,
                            initStatus.getWorkflowUUID(),
                            WorkflowState.RETRIES_EXCEEDED.toString(),
                            options);
                    throw new DBOSDeadLetterQueueException(initStatus.getWorkflowUUID(),
                            maxRetries);
                }

                connection.commit(); // Commit transaction on success
                return new WorkflowInitResult(initStatus.getWorkflowUUID(), resRow.getStatus(),
                        resRow.getWorkflowDeadlineEpochMs());

            } catch (SQLException e) {

                try {
                    connection.rollback();
                } catch (SQLException rollbackEx) {
                    logger.error("Rollback failed: " + rollbackEx.getMessage());
                }
                throw e; // Re-throw the original SQLException
            } catch (DBOSWorkflowConflictException | DBOSDeadLetterQueueException e) {
                // Rollback for custom business exceptions
                try {
                    connection.rollback();
                } catch (SQLException rollbackEx) {
                    logger.error("Rollback failed: " + rollbackEx.getMessage());
                }
                throw e; // Re-throw the custom exception
            }
        } // end try with resources connection closed
    }

    /**
     * Insert into the workflow_status table
     *
     * @param status
     * @WorkflowStatusInternal holds the data for a workflow_status row
     * @return @InsertWorkflowResult some of the column inserted
     * @throws SQLException
     */
    public InsertWorkflowResult insertWorkflowStatus(Connection connection,
            WorkflowStatusInternal status) throws SQLException {

        String insertSQL = "INSERT INTO dbos.workflow_status ("
                + "workflow_uuid, status, name, class_name, config_name, "
                + "output, error, executor_id, application_version, application_id, "
                + "authenticated_user, authenticated_roles, assumed_role, queue_name, "
                + "recovery_attempts, workflow_timeout_ms, workflow_deadline_epoch_ms, "
                + "deduplication_id, priority, inputs"
                + ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                + "ON CONFLICT (workflow_uuid) DO UPDATE "
                + "SET recovery_attempts = EXCLUDED.recovery_attempts + 1, "
                + "updated_at = EXCLUDED.updated_at, " + "executor_id = EXCLUDED.executor_id "
                + "RETURNING recovery_attempts, status, workflow_deadline_epoch_ms, name, class_name, config_name, queue_name";

        try (PreparedStatement stmt = connection.prepareStatement(insertSQL)) {

            stmt.setString(1, status.getWorkflowUUID());
            stmt.setString(2, status.getStatus().toString());
            stmt.setString(3, status.getName());
            stmt.setString(4, status.getClassName());
            stmt.setString(5, status.getConfigName());
            stmt.setString(6, status.getOutput());
            stmt.setString(7, status.getError());
            stmt.setString(8, status.getExecutorId());
            stmt.setString(9, status.getAppVersion());
            stmt.setString(10, status.getAppId());
            stmt.setString(11, status.getAuthenticatedUser());
            stmt.setString(12, status.getAuthenticatedRoles());
            stmt.setString(13, status.getAssumedRole());
            stmt.setString(14, status.getQueueName());
            stmt.setInt(15, status.getRecoveryAttempts());
            stmt.setObject(16, status.getWorkflowTimeoutMs());
            stmt.setObject(17, status.getWorkflowDeadlineEpochMs());
            stmt.setString(18, status.getDeduplicationId());
            stmt.setInt(19, status.getPriority());
            stmt.setString(20, status.getInputs());

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    InsertWorkflowResult result = new InsertWorkflowResult(
                            rs.getInt("recovery_attempts"), rs.getString("status"),
                            rs.getString("name"), rs.getString("class_name"),
                            rs.getString("config_name"), rs.getString("queue_name"),
                            rs.getObject("workflow_deadline_epoch_ms", Long.class));

                    return result;
                } else {
                    throw new RuntimeException("Attempt to insert workflow "
                            + status.getWorkflowUUID() + " failed: No rows returned.");
                }

            } catch (SQLException e) {

                if ("23505".equals(e.getSQLState())) {
                    throw new DBOSQueueDuplicatedException(status.getWorkflowUUID(),
                            status.getQueueName() != null ? status.getQueueName() : "",
                            status.getDeduplicationId() != null ? status.getDeduplicationId() : "");
                }
                // Re-throw other SQL exceptions
                throw e;
            }
        }
    }

    public void updateWorkflowStatus(Connection connection, String workflowID, String status,
            UpdateWorkflowOptions options) throws SQLException {

        StringBuilder setClauseBuilder = new StringBuilder("SET status = ?, updated_at = ?");
        StringBuilder whereClauseBuilder = new StringBuilder("WHERE workflow_uuid = ?");

        List<Object> finalOrderedArgs = new ArrayList<>();

        finalOrderedArgs.add(status);
        finalOrderedArgs.add(Instant.now().toEpochMilli());

        if (options.getOutput() != null) {
            setClauseBuilder.append(", output = ?");
            finalOrderedArgs.add(options.getOutput());
        }

        if (options.getError() != null) {
            setClauseBuilder.append(", error = ?");
            finalOrderedArgs.add(options.getError());
        }

        if (options.getResetRecoveryAttempts() != null && options.getResetRecoveryAttempts()) {
            setClauseBuilder.append(", recovery_attempts = 0");
        }

        if (options.getResetDeadline() != null && options.getResetDeadline()) {
            setClauseBuilder.append(", workflow_deadline_epoch_ms = NULL");
        }

        if (options.getQueueName() != null) {
            setClauseBuilder.append(", queue_name = ?");
            finalOrderedArgs.add(options.getQueueName()); // This handles both String and
                                                          // null
        }

        if (options.getResetDeduplicationID() != null && options.getResetDeduplicationID()) {
            setClauseBuilder.append(", deduplication_id = NULL");
        }

        if (options.getResetStartedAtEpochMs() != null && options.getResetStartedAtEpochMs()) {
            setClauseBuilder.append(", started_at_epoch_ms = NULL");
        }

        finalOrderedArgs.add(workflowID); // This must be the first parameter in the WHERE
                                          // clause part (for WHERE
        // workflow_uuid = ?)

        if (options.getWhereStatus() != null) {
            whereClauseBuilder.append(" AND status = ?");
            finalOrderedArgs.add(options.getWhereStatus());
        }

        // Construct the final SQL query
        String sql = String.format("UPDATE %s.workflow_status %s %s",
                Constants.DB_SCHEMA,
                setClauseBuilder.toString(),
                whereClauseBuilder.toString());

        int affectedRows;
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            // Bind all parameters in order
            for (int i = 0; i < finalOrderedArgs.size(); i++) {
                Object arg = finalOrderedArgs.get(i);
                if (arg == null) {
                    pstmt.setNull(i + 1, Types.VARCHAR); // Default to VARCHAR for null
                                                         // strings
                } else if (arg instanceof String) {
                    pstmt.setString(i + 1, (String) arg);
                } else if (arg instanceof Long) {
                    pstmt.setLong(i + 1, (Long) arg);
                } else if (arg instanceof Integer) {
                    pstmt.setInt(i + 1, (Integer) arg);
                } else {
                    pstmt.setObject(i + 1, arg); // Fallback for other types
                }
            }

            affectedRows = pstmt.executeUpdate();
        }

        if (options.getThrowOnFailure() && affectedRows != 1) {
            throw new DBOSWorkflowConflictException(workflowID, String.format(
                    "Attempt to record transition of nonexistent workflow %s (affected rows: %d)",
                    workflowID,
                    affectedRows));
        }
    }

    /**
     * Store the result to workflow_status
     *
     * @param workflowId
     *            id of the workflow
     * @param result
     *            output serialized as json
     */
    public void recordWorkflowOutput(String workflowId, String result) {

        try {
            try (Connection connection = dataSource.getConnection()) {

                UpdateWorkflowOptions options = new UpdateWorkflowOptions();
                options.setOutput(result);
                options.setResetDeduplicationID(true);

                updateWorkflowStatus(connection,
                        workflowId,
                        WorkflowState.SUCCESS.toString(),
                        options);
            }
        } catch (SQLException e) {
            throw new DBOSException(UNEXPECTED.getCode(), e.getMessage());
        }
    }

    /**
     * Store the error to workflow_status
     *
     * @param workflowId
     *            id of the workflow
     * @param error
     *            output serialized as json
     */
    public void recordWorkflowError(String workflowId, String error) {

        try {
            try (Connection connection = dataSource.getConnection()) {

                UpdateWorkflowOptions options = new UpdateWorkflowOptions();
                options.setError(error);
                options.setResetDeduplicationID(true);

                updateWorkflowStatus(connection, workflowId, WorkflowState.ERROR.toString(), options);
            }
        } catch (SQLException e) {
            throw new DBOSException(UNEXPECTED.getCode(), e.getMessage());
        }
    }

    public WorkflowStatus getWorkflowStatus(String workflowId) {

        try {
            ListWorkflowsInput input = new ListWorkflowsInput();
            input.setWorkflowIDs(Arrays.asList(workflowId));
            List<WorkflowStatus> output = listWorkflows(input);
            if (output.size() > 0) {
                return output.get(0);
            }
        } catch (SQLException e) {
            logger.error("Error retrieving workflow for " + workflowId, e);
        }

        throw new NonExistentWorkflowException(workflowId);
    }

    public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) throws SQLException {

        List<WorkflowStatus> workflows = new ArrayList<>();

        StringBuilder sqlBuilder = new StringBuilder();
        List<Object> parameters = new ArrayList<>();

        // Start building the SELECT clause. The order of columns here is critical
        // for mapping to the WorkflowStatus fields by index later in the ResultSet.
        sqlBuilder.append("SELECT workflow_uuid, status, name, recovery_attempts, "
                + "config_name, class_name, authenticated_user, authenticated_roles, "
                + "assumed_role, queue_name, executor_id, created_at, updated_at, "
                + "application_version, application_id, inputs, output, error, "
                + "workflow_deadline_epoch_ms, workflow_timeout_ms ");

        sqlBuilder.append(String.format("FROM %s.workflow_status ", Constants.DB_SCHEMA));

        // --- WHERE Clauses ---
        StringJoiner whereConditions = new StringJoiner(" AND ");

        if (input.getWorkflowName() != null) {
            whereConditions.add("name = ?");
            parameters.add(input.getWorkflowName());
        }
        if (input.getAuthenticatedUser() != null) {
            whereConditions.add("authenticated_user = ?");
            parameters.add(input.getAuthenticatedUser());
        }
        if (input.getStartTime() != null) {
            whereConditions.add("created_at >= ?");
            // Convert OffsetDateTime to epoch milliseconds for comparison with DB column
            parameters.add(input.getStartTime().toInstant().toEpochMilli());
        }
        if (input.getEndTime() != null) {
            whereConditions.add("created_at <= ?");
            // Convert OffsetDateTime to epoch milliseconds for comparison with DB column
            parameters.add(input.getEndTime().toInstant().toEpochMilli());
        }
        if (input.getStatus() != null) {
            whereConditions.add("status = ?");
            parameters.add(input.getStatus());
        }
        if (input.getApplicationVersion() != null) {
            whereConditions.add("application_version = ?");
            parameters.add(input.getApplicationVersion());
        }
        if (input.getWorkflowIDs() != null && !input.getWorkflowIDs().isEmpty()) {
            // Handle IN clause: dynamically generate ? for each ID
            StringJoiner inClausePlaceholders = new StringJoiner(", ", "(", ")");
            for (String id : input.getWorkflowIDs()) {
                inClausePlaceholders.add("?");
                parameters.add(id);
            }
            whereConditions.add("workflow_uuid IN " + inClausePlaceholders.toString());
        }
        if (input.getWorkflowIdPrefix() != null) {
            whereConditions.add("workflow_uuid LIKE ?");
            // Append wildcard directly to the parameter value
            parameters.add(input.getWorkflowIdPrefix() + "%");
        }

        // Only append WHERE keyword if there are actual conditions
        if (whereConditions.length() > 0) {
            sqlBuilder.append(" WHERE ").append(whereConditions.toString());
        }

        // --- ORDER BY Clause ---
        sqlBuilder.append(" ORDER BY created_at ");
        if (input.getSortDesc() != null && input.getSortDesc()) {
            sqlBuilder.append("DESC");
        } else {
            sqlBuilder.append("ASC");
        }

        // --- LIMIT and OFFSET Clauses ---
        if (input.getLimit() != null) {
            sqlBuilder.append(" LIMIT ?");
            parameters.add(input.getLimit());
        }
        if (input.getOffset() != null) {
            sqlBuilder.append(" OFFSET ?");
            parameters.add(input.getOffset());
        }

        try (Connection connection = dataSource.getConnection();
                PreparedStatement pstmt = connection.prepareStatement(sqlBuilder.toString())) {

            for (int i = 0; i < parameters.size(); i++) {

                Object param = parameters.get(i);
                if (param instanceof String) {
                    pstmt.setString(i + 1, (String) param);
                } else if (param instanceof Long) {
                    pstmt.setLong(i + 1, (Long) param);
                } else if (param instanceof Integer) {
                    pstmt.setInt(i + 1, (Integer) param);
                } else {
                    // Fallback for other types, or if OffsetDateTime was directly added
                    // to
                    // parameters list
                    pstmt.setObject(i + 1, param);
                }
            }

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    WorkflowStatus info = new WorkflowStatus();
                    // The column names or their order in the SELECT statement must match.
                    info.setWorkflowId(rs.getString("workflow_uuid"));
                    info.setStatus(rs.getString("status"));
                    info.setName(rs.getString("name"));
                    info.setRecoveryAttempts(rs.getInt("recovery_attempts")); // getObject
                                                                              // for
                                                                              // nullable
                    info.setConfigName(rs.getString("config_name"));
                    info.setClassName(rs.getString("class_name"));
                    info.setAuthenticatedUser(rs.getString("authenticated_user"));

                    String authenticatedRolesJson = rs.getString("authenticated_roles");
                    if (authenticatedRolesJson != null) {
                        info.setAuthenticatedRoles(
                                (String[]) JSONUtil.deserializeToArray(authenticatedRolesJson));
                    }

                    info.setAssumedRole(rs.getString("assumed_role"));
                    info.setQueueName(rs.getString("queue_name"));
                    info.setExecutorId(rs.getString("executor_id"));
                    info.setCreatedAt(rs.getObject("created_at", Long.class)); // getObject
                                                                               // for
                                                                               // nullable
                    info.setUpdatedAt(rs.getObject("updated_at", Long.class)); // getObject
                                                                               // for
                                                                               // nullable
                    info.setAppVersion(rs.getString("application_version"));
                    info.setAppId(rs.getString("application_id"));

                    String serializedInput = rs.getString("inputs");
                    String serializedOutput = rs.getString("output");
                    String serializedError = rs.getString("error");

                    if (serializedInput != null) {
                        info.setInput(JSONUtil.deserializeToArray(serializedInput));
                    }

                    if (serializedOutput != null) {
                        Object[] oArray = JSONUtil.deserializeToArray(serializedOutput);
                        info.setOutput(oArray[0]);
                    }

                    info.setError(serializedError);

                    info.setWorkflowDeadlineEpochMs(
                            rs.getObject("workflow_deadline_epoch_ms", Long.class));
                    info.setWorkflowTimeoutMs(rs.getObject("workflow_timeout_ms", Long.class));

                    workflows.add(info);
                }
            }
        }

        return workflows;
    }

    public List<GetPendingWorkflowsOutput> getPendingWorkflows(String executorId, String appVersion)
            throws SQLException {

        String sqlTemplate = "SELECT workflow_uuid, queue_name " + "FROM %s.workflow_status "
                + "WHERE status = ? " + "AND executor_id = ? " + "AND application_version = ?";

        final String sql = String.format(sqlTemplate, Constants.DB_SCHEMA);

        List<GetPendingWorkflowsOutput> results = new ArrayList<>();

        try (Connection connection = dataSource.getConnection();
                PreparedStatement stmt = connection.prepareStatement(sql)) {

            stmt.setString(1, WorkflowState.PENDING.name());
            stmt.setString(2, executorId);
            stmt.setString(3, appVersion);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(new GetPendingWorkflowsOutput(rs.getString("workflow_uuid"),
                            rs.getString("queue_name")));
                }
            }
        }

        return results;
    }

    public Object awaitWorkflowResult(String workflowId) {

        final String sql = "SELECT status, output, error " + "FROM dbos.workflow_status "
                + "WHERE workflow_uuid = ?";

        while (true) {

            try (Connection connection = dataSource.getConnection();
                    PreparedStatement stmt = connection.prepareStatement(sql)) {

                stmt.setString(1, workflowId);

                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        String status = rs.getString("status");

                        switch (WorkflowState.valueOf(status.toUpperCase())) {
                            case SUCCESS :
                                String output = rs.getString("output");
                                Object[] oArray = JSONUtil.deserializeToArray(output);
                                return oArray[0];

                            case ERROR :
                                String error = rs.getString("error");
                                Object[] eArray = JSONUtil.deserializeToArray(error);
                                SerializableException se = (SerializableException) eArray[0];
                                throw new DBOSAppException(
                                        String.format("Exception of type %s", se.className), se);
                            case CANCELLED :
                                throw new AwaitedWorkflowCancelledException(workflowId);

                            default :
                                // Status is PENDING or other - continue polling
                                break;
                        }
                    }
                    // Row not found - workflow hasn't appeared yet, continue polling
                }
            } catch (SQLException e) {
                logger.error("Database error while polling workflow " + workflowId + ": "
                        + e.getMessage());
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Workflow polling interrupted for " + workflowId, e);
            }
        }
    }

    public void recordChildWorkflow(String parentId, String childId, // workflowId of the
                                                                     // child
            int functionId, // func id in the parent
            String functionName) {

        String sql = String.format(
                "INSERT INTO %s.operation_outputs (workflow_uuid, function_id, function_name, child_workflow_id) "
                        + "VALUES (?, ?, ?, ?)",
                Constants.DB_SCHEMA);

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
                throw new DBOSWorkflowConflictException(parentId, String.format(
                        "Record exists for parent %s and functionId %d",
                        parentId,
                        functionId));
            } else {
                throw new DBOSException(UNEXPECTED.getCode(), sqe.getMessage());
            }
        }
    }

    public Optional<String> checkChildWorkflow(String workflowUuid, int functionId)
            throws SQLException {
        String sql = "SELECT child_workflow_id " + " FROM dbos.operation_outputs "
                + "WHERE workflow_uuid = ? AND function_id = ? ";

        try (Connection connection = dataSource.getConnection();
                PreparedStatement stmt = connection.prepareStatement(sql)) {

            stmt.setString(1, workflowUuid);
            stmt.setInt(2, functionId);

            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    String childWorkflowId = rs.getString("child_workflow_id");
                    return childWorkflowId != null
                            ? Optional.of(childWorkflowId)
                            : Optional.empty();
                }
                return Optional.empty();
            }
        }
    }

    public void cancelWorkflow(String workflowId) throws SQLException {

        try (Connection conn = dataSource.getConnection()) {

            // Check the status of the workflow. If it is complete, do nothing.
            String checkStatusSql = " SELECT status FROM %s.workflow_status WHERE workflow_uuid = ? ";
            checkStatusSql = String.format(checkStatusSql, Constants.DB_SCHEMA);

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
            if (currentStatus == null || WorkflowState.SUCCESS.name().equals(currentStatus)
                    || WorkflowState.ERROR.name().equals(currentStatus)) {
                logger.info("Returning without updating status");
                return;
            }

            // Set the workflow's status to CANCELLED and remove it from any queue it is
            // on
            String updateSql = "UPDATE %s.workflow_status SET status = ?, "
                    + " queue_name = NULL, deduplication_id = NULL, started_at_epoch_ms = NULL "
                    + " WHERE workflow_uuid = ? ";
            updateSql = String.format(updateSql, Constants.DB_SCHEMA);

            try (PreparedStatement stmt = conn.prepareStatement(updateSql)) {
                stmt.setString(1, WorkflowState.CANCELLED.name());
                stmt.setString(2, workflowId);
                stmt.executeUpdate();
            }
        }
    }

    public void resumeWorkflow(String workflowId) throws SQLException {

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);

            try {
                String currentStatus = getWorkflowStatus(connection, workflowId);

                if (currentStatus == null) {
                    connection.rollback();
                    return;
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

        String forkedWorkflowId = options.getForkedWorkflowId() == null
                ? UUID.randomUUID().toString()
                : options.getForkedWorkflowId();

        logger.info("Original " + originalWorkflowId + "forked " + forkedWorkflowId);

        String applicationVersion = options.getApplicationVersion();

        WorkflowStatus status = getWorkflowStatus(originalWorkflowId);

        long timeoutMs = options.getTimeoutMS() == 0
                ? status.getWorkflowTimeoutMs()
                : options.getTimeoutMS();

        if (status == null) {
            throw new NonExistentWorkflowException(originalWorkflowId);
        }

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            try {
                // Create entry for forked workflow
                insertForkedWorkflowStatus(connection,
                        forkedWorkflowId,
                        status,
                        applicationVersion,
                        timeoutMs);

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

    private void insertForkedWorkflowStatus(Connection connection, String forkedWorkflowId,
            WorkflowStatus originalStatus, String applicationVersion, long timeoutMs)
            throws SQLException {

        long workflowDeadlineEpoch = 0;
        if (timeoutMs > 0) {
            workflowDeadlineEpoch = System.currentTimeMillis() + timeoutMs;
        }

        String sql = "INSERT INTO dbos.workflow_status ( "
                + " workflow_uuid, status, name, class_name, config_name, application_version, application_id, "
                + " authenticated_user, authenticated_roles, assumed_role, queue_name, inputs, workflow_deadline_epoch_ms, workflow_timeout_ms "
                + " ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, forkedWorkflowId);
            stmt.setString(2, WorkflowState.ENQUEUED.name());
            stmt.setString(3, originalStatus.getName());
            stmt.setString(4, originalStatus.getClassName());
            stmt.setString(5, originalStatus.getConfigName());

            // Use provided application version or fall back to original
            String appVersion = applicationVersion != null
                    ? applicationVersion
                    : originalStatus.getAppVersion();
            stmt.setString(6, appVersion);

            stmt.setString(7, originalStatus.getAppId());
            stmt.setString(8, originalStatus.getAuthenticatedUser());
            stmt.setString(9, JSONUtil.serializeArray(originalStatus.getAuthenticatedRoles()));
            stmt.setString(10, originalStatus.getAssumedRole());
            stmt.setString(11, Constants.DBOS_INTERNAL_QUEUE);
            stmt.setString(12, JSONUtil.serializeArray(originalStatus.getInput()));
            stmt.setLong(13, workflowDeadlineEpoch);
            stmt.setLong(14, originalStatus.getWorkflowTimeoutMs());

            stmt.executeUpdate();
        }
    }

    private void copyOperationOutputs(Connection connection, String originalWorkflowId,
            String forkedWorkflowId, int startStep) throws SQLException {

        String sql = "INSERT INTO dbos.operation_outputs ( "
                + " workflow_uuid, function_id, output, error, function_name, child_workflow_id) "
                + " SELECT ? as workflow_uuid, function_id, output, error, function_name, child_workflow_id "
                + " FROM dbos.operation_outputs " + " WHERE workflow_uuid = ? "
                + " AND function_id < ? ";

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

    private String getWorkflowStatus(Connection connection, String workflowId) throws SQLException {
        String sql = "SELECT status FROM dbos.workflow_status WHERE workflow_uuid = ?";

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

    private void updateWorkflowToEnqueued(Connection connection, String workflowId)
            throws SQLException {
        String sql = "UPDATE dbos.workflow_status " + " SET status = ?, " + " queue_name = ?, "
                + " recovery_attempts = ?, " + " workflow_deadline_epoch_ms = 0, "
                + " deduplication_id = NULL, " + " started_at_epoch_ms = NULL "
                + " WHERE workflow_uuid = ? ";

        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, WorkflowState.ENQUEUED.name());
            stmt.setString(2, Constants.DBOS_INTERNAL_QUEUE);
            stmt.setInt(3, 0); // recovery_attempts = 0
            stmt.setString(4, workflowId);

            stmt.executeUpdate();
        }
    }

    private Long getRowsCutoff(Connection connection, long rowsThreshold) throws SQLException {
        String sql = "SELECT created_at FROM dbos.workflow_status ORDER BY created_at DESC OFFSET $1 LIMIT 1";
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
                String sql = "DELETE FROM dbos.workflow_status WHERE created_at < $1 AND status NOT IN ($2, $3)";
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
