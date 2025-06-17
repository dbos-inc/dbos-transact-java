package dev.dbos.transact.database;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import dev.dbos.transact.Constants;
import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.exceptions.DBOSDeadLetterQueueException;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.exceptions.DBOSWorkflowConflictException;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.InsertWorkflowResult;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SystemDatabase {

    private Logger logger = LoggerFactory.getLogger(SystemDatabase.class) ;
    private DBOSConfig config ;
    private static SystemDatabase instance ;
    private DataSource dataSource ;

    private SystemDatabase(DBOSConfig cfg) {
        config = cfg ;

        String dbName;
        if (config.getSysDbName() != null) {
            dbName = config.getSysDbName();
        } else {
            dbName = config.getName() + Constants.SYS_DB_SUFFIX;
        }

        createDataSource(dbName);
    }

    public static synchronized void initialize(DBOSConfig cfg) {
        if (instance != null) {
            throw new IllegalStateException("SystemDatabase has already been initialized.");
        }
        instance = new SystemDatabase(cfg);
    }

    public static SystemDatabase getInstance() {
        if (instance == null) {
            throw new RuntimeException("SystemDatabase should be initalized first") ;
        }
        return instance ;
    }

    public synchronized static void destroy() {
        if (instance.dataSource != null) {
            ((HikariDataSource)instance.dataSource).close();
        }
        instance = null ;
    }

    /**
     * Initializes the status of a workflow.
     *
     * @param initStatus The initial workflow status details.
     * @param maxRetries Optional maximum number of retries.
     * @return An object containing the current status and optionally the deadline epoch milliseconds.
     * @throws SQLException If a database error occurs.
     * @throws DBOSWorkflowConflictException If a conflicting workflow already exists.
     * @throws DBOSDeadLetterQueueException If the workflow exceeds max retries.
     */
    public WorkflowInitResult initWorkflowStatus(
            WorkflowStatusInternal initStatus,
            Integer maxRetries
    ) throws SQLException {

        try (Connection connection = dataSource.getConnection()) {

            try {
                connection.setAutoCommit(false);
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);

                InsertWorkflowResult resRow = insertWorkflowStatus(connection, initStatus);

                if (!Objects.equals(resRow.getName(), initStatus.getName())) {
                    String msg = String.format(
                            "Workflow already exists with a different function name: %s, but the provided function name is: %s",
                            resRow.getName(), initStatus.getName()
                    );
                    throw new DBOSWorkflowConflictException(initStatus.getWorkflowUUID(), msg);
                } else if (!Objects.equals(resRow.getClassName(), initStatus.getClassName())) {
                    String msg = String.format(
                            "Workflow already exists with a different class name: %s, but the provided class name is: %s",
                            resRow.getClassName(), initStatus.getClassName()
                    );
                    throw new DBOSWorkflowConflictException(initStatus.getWorkflowUUID(), msg);
                } else if (!Objects.equals(
                        resRow.getConfigName() != null ? resRow.getConfigName() : "",
                        initStatus.getConfigName() != null ? initStatus.getConfigName() : ""
                )) {
                    String msg = String.format(
                            "Workflow already exists with a different class configuration: %s, but the provided class configuration is: %s",
                            resRow.getConfigName(), initStatus.getConfigName()
                    );
                    throw new DBOSWorkflowConflictException(initStatus.getWorkflowUUID(), msg);
                } else if (!Objects.equals(resRow.getQueueName(), initStatus.getQueueName())) {
                    logger.warn(String.format(
                            "Workflow (%s) already exists in queue: %s, but the provided queue name is: %s. The queue is not updated. %s",
                            initStatus.getWorkflowUUID(), resRow.getQueueName(), initStatus.getQueueName(), new Throwable().getStackTrace()[0]
                    ));
                }

                final int attempts = resRow.getRecoveryAttempts();
                if (maxRetries != null && attempts > maxRetries + 1) {

                        UpdateWorkflowOptions options = new UpdateWorkflowOptions();
                        options.setWhereStatus(WorkflowStatus.PENDING.toString());
                        options.setThrowOnFailure(false);

                        updateWorkflowStatus(connection, initStatus.getWorkflowUUID(),
                                WorkflowStatus.RETRIES_EXCEEDED.toString(),
                                options);
                        throw new DBOSDeadLetterQueueException(initStatus.getWorkflowUUID(), maxRetries);
                }

                logger.debug(String.format("Workflow %s attempt number: %d.", initStatus.getWorkflowUUID(), attempts));

                connection.commit(); // Commit transaction on success
                return new WorkflowInitResult(resRow.getStatus(), resRow.getWorkflowDeadlineEpochMs());

            } catch(SQLException e){

                    try {
                        connection.rollback();
                    } catch (SQLException rollbackEx) {
                        logger.error("Rollback failed: " + rollbackEx.getMessage());
                    }
                    throw e; // Re-throw the original SQLException
            } catch(DBOSWorkflowConflictException| DBOSDeadLetterQueueException e){
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
     * @param status @WorkflowStatusInternal holds the data for a workflow_status row
     * @return @InsertWorkflowResult some of the column inserted
     * @throws SQLException
     */

    public InsertWorkflowResult insertWorkflowStatus(
            Connection connection,
            WorkflowStatusInternal status
    ) throws SQLException {



            String insertSQL =
                    "INSERT INTO dbos.workflow_status (" +
                            "workflow_uuid, status, name, class_name, config_name, " +
                            "output, error, executor_id, application_version, application_id, " +
                            "authenticated_user, authenticated_roles, assumed_role, queue_name, " +
                            "recovery_attempts, workflow_timeout_ms, workflow_deadline_epoch_ms, " +
                            "deduplication_id, priority, inputs" +
                            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                            "ON CONFLICT (workflow_uuid) DO UPDATE " +
                            "SET recovery_attempts = EXCLUDED.recovery_attempts + 1, " +
                            "updated_at = EXCLUDED.updated_at, " +
                            "executor_id = EXCLUDED.executor_id " +
                            "RETURNING recovery_attempts, status, workflow_deadline_epoch_ms, name, class_name, config_name, queue_name";


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
                                rs.getInt("recovery_attempts"),
                                rs.getString("status"),
                                rs.getString("name"),
                                rs.getString("class_name"),
                                rs.getString("config_name"),
                                rs.getString("queue_name"),
                                rs.getObject("workflow_deadline_epoch_ms", Long.class)
                        );

                        return result;
                    } else {
                        throw new RuntimeException(
                                "Attempt to insert workflow " + status.getWorkflowUUID() + " failed: No rows returned."
                        );
                    }

                } catch (SQLException e) {

                    if ("23505".equals(e.getSQLState())) {
                        throw new DBOSQueueDuplicatedException(
                            status.getWorkflowUUID(),
                            status.getQueueName() != null ? status.getQueueName() : "",
                            status.getDeduplicationId() != null ? status.getDeduplicationId() : ""
                    );
                }
                // Re-throw other SQL exceptions
                throw e;
            }
        }

    }

    public void updateWorkflowStatus(
            Connection connection,
            String workflowID,
            String status,
            UpdateWorkflowOptions options
    ) throws SQLException { // Declare checked exception

        StringBuilder setClauseBuilder = new StringBuilder("SET status = ?, updated_at = ?");
        StringBuilder whereClauseBuilder = new StringBuilder("WHERE workflow_uuid = ?");

        // List to hold parameters in the order they appear in the final SQL string's '?' placeholders
        List<Object> finalOrderedArgs = new ArrayList<>();

        // Add initial parameters for SET clause (status, updated_at)
        finalOrderedArgs.add(status);
        finalOrderedArgs.add(Instant.now().toEpochMilli());

        // Handle update options directly from the flattened 'options' object
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
            finalOrderedArgs.add(options.getQueueName()); // This handles both String and null
        }

        if (options.getResetDeduplicationID() != null && options.getResetDeduplicationID()) {
            setClauseBuilder.append(", deduplication_id = NULL");
        }

        if (options.getResetStartedAtEpochMs() != null && options.getResetStartedAtEpochMs()) {
            setClauseBuilder.append(", started_at_epoch_ms = NULL");
        }

        // Add parameters for WHERE clause (workflow_uuid and then optional status)
        finalOrderedArgs.add(workflowID); // This must be the first parameter in the WHERE clause part (for WHERE workflow_uuid = ?)

        // Handle where options directly from the flattened 'options' object
        if (options.getWhereStatus() != null) { // Check if where status is provided
            whereClauseBuilder.append(" AND status = ?");
            finalOrderedArgs.add(options.getWhereStatus());
        }

        // Construct the final SQL query
        String sql = String.format(
                "UPDATE %s.workflow_status %s %s",
                Constants.DB_SCHEMA,
                setClauseBuilder.toString(),
                whereClauseBuilder.toString()
        );

        int affectedRows;
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            // Bind all parameters in order
            for (int i = 0; i < finalOrderedArgs.size(); i++) {
                Object arg = finalOrderedArgs.get(i);
                if (arg == null) {
                    // This is a simplification; a more robust solution would infer type
                    // or require type info for each nullable parameter.
                    // For now, assume common types for null.
                    pstmt.setNull(i + 1, Types.VARCHAR); // Default to VARCHAR for null strings
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
            throw new DBOSWorkflowConflictException(workflowID,
                    String.format("Attempt to record transition of nonexistent workflow %s (affected rows: %d)",
                            workflowID, affectedRows)
            );
        }
    }

private void createDataSource(String dbName) {
        HikariConfig hikariConfig = new HikariConfig();

        String dburl = String.format("jdbc:postgresql://%s:%d/%s",config.getDbHost(),config.getDbPort(),dbName);

        hikariConfig.setJdbcUrl(dburl);
        hikariConfig.setUsername(config.getDbUser());
        hikariConfig.setPassword(config.getDbPassword());

        int maximumPoolSize = config.getMaximumPoolSize();
        if (maximumPoolSize > 0) {
            hikariConfig.setMaximumPoolSize(maximumPoolSize);
        } else {
            hikariConfig.setMaximumPoolSize(2);
        }

        int connectionTimeout = config.getConnectionTimeout();
        if (connectionTimeout > 0) {
            hikariConfig.setConnectionTimeout(connectionTimeout);
        }

        dataSource = new HikariDataSource(hikariConfig);
    }

    public static class WorkflowInitResult {
        private String status;
        private Long deadlineEpochMS; // Use Long for nullable number

        public WorkflowInitResult(String status, Long deadlineEpochMS) {
            this.status = status;
            this.deadlineEpochMS = deadlineEpochMS;
        }

        public String getStatus() {
            return status;
        }

        public Long getDeadlineEpochMS() {
            return deadlineEpochMS;
        }
    }

    public static class UpdateWorkflowOptions {

        private String output;
        private String error;
        private Boolean resetRecoveryAttempts;
        private String queueName;
        private Boolean resetDeadline;
        private Boolean resetDeduplicationID;
        private Boolean resetStartedAtEpochMs;

        private String whereStatus;
        private Boolean throwOnFailure;

        public UpdateWorkflowOptions() {
        }


        public UpdateWorkflowOptions withOutput(String output) {
            this.output = output;
            return this;
        }

        public UpdateWorkflowOptions withError(String error) {
            this.error = error;
            return this;
        }

        public UpdateWorkflowOptions withResetRecoveryAttempts(Boolean resetRecoveryAttempts) {
            this.resetRecoveryAttempts = resetRecoveryAttempts;
            return this;
        }

        public UpdateWorkflowOptions withQueueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public UpdateWorkflowOptions withResetDeadline(Boolean resetDeadline) {
            this.resetDeadline = resetDeadline;
            return this;
        }

        public UpdateWorkflowOptions withResetDeduplicationID(Boolean resetDeduplicationID) {
            this.resetDeduplicationID = resetDeduplicationID;
            return this;
        }

        public UpdateWorkflowOptions withResetStartedAtEpochMs(Boolean resetStartedAtEpochMs) {
            this.resetStartedAtEpochMs = resetStartedAtEpochMs;
            return this;
        }


        public String getOutput() {
            return output;
        }

        public String getError() {
            return error;
        }

        public Boolean getResetRecoveryAttempts() {
            return resetRecoveryAttempts;
        }

        public String getQueueName() {
            return queueName;
        }

        public Boolean getResetDeadline() {
            return resetDeadline;
        }

        public Boolean getResetDeduplicationID() {
            return resetDeduplicationID;
        }

        public Boolean getResetStartedAtEpochMs() {
            return resetStartedAtEpochMs;
        }

        public String getWhereStatus() {
            return whereStatus;
        }

        public Boolean getThrowOnFailure() {
            return throwOnFailure;
        }


        public void setOutput(String output) {
            this.output = output;
        }

        public void setError(String error) {
            this.error = error;
        }

        public void setResetRecoveryAttempts(Boolean resetRecoveryAttempts) {
            this.resetRecoveryAttempts = resetRecoveryAttempts;
        }

        public void setQueueName(String queueName) {
            this.queueName = queueName;
        }

        public void setResetDeadline(Boolean resetDeadline) {
            this.resetDeadline = resetDeadline;
        }

        public void setResetDeduplicationID(Boolean resetDeduplicationID) {
            this.resetDeduplicationID = resetDeduplicationID;
        }

        public void setResetStartedAtEpochMs(Boolean resetStartedAtEpochMs) {
            this.resetStartedAtEpochMs = resetStartedAtEpochMs;
        }

        public void setWhereStatus(String status) {
            this.whereStatus = status;
        }

        ;

        public void setThrowOnFailure(Boolean throwOnFailure) {
            this.throwOnFailure = throwOnFailure;
        }
    }

    Connection getSysDBConnection() throws SQLException {
        return dataSource.getConnection();
    }

}
