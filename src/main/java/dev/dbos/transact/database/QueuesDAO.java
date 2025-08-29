package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.queue.ListQueuedWorkflowsInput;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueuesDAO {
    Logger logger = LoggerFactory.getLogger(QueuesDAO.class);
    private HikariDataSource dataSource;

    QueuesDAO(HikariDataSource ds) {
        dataSource = ds;
    }

    /**
     * Get queued workflows based on queue configuration and concurrency limits.
     *
     * @param queue
     *            The queue configuration
     * @param executorId
     *            The executor ID
     * @param appVersion
     *            The application version
     * @return List of workflow UUIDs that are due for execution
     */
    public List<String> getAndStartQueuedWorkflows(Queue queue, String executorId,
            String appVersion) throws SQLException {
        if (dataSource.isClosed()) {
            throw new IllegalStateException("Database is closed!");
        }

        long startTimeMs = System.currentTimeMillis();
        Long limiterPeriodMs = null;
        if (queue.hasLimiter()) {
            limiterPeriodMs = (long) (queue.getRateLimit().getPeriod() * 1000);
        }

        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            // Set snapshot isolation level
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
            }

            int numRecentQueries = 0;

            // Check rate limiter if configured
            if (queue.hasLimiter()) {
                String limiterQuery = "SELECT COUNT(*) " + " FROM %s.workflow_status "
                        + " WHERE queue_name = ? " + " AND status != 'ENQUEUED' "
                        + " AND started_at_epoch_ms > ? ;";

                limiterQuery = String.format(limiterQuery, Constants.DB_SCHEMA);

                try (PreparedStatement ps = connection.prepareStatement(limiterQuery)) {
                    ps.setString(1, queue.getName());
                    ps.setLong(2, startTimeMs - limiterPeriodMs);

                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) {
                            numRecentQueries = rs.getInt(1);
                        }
                    }
                }

                if (numRecentQueries >= queue.getRateLimit().getLimit()) {
                    return new ArrayList<>();
                }
            }

            // Calculate max tasks based on concurrency limits
            int maxTasks = 100;

            if (queue.getWorkerConcurrency() > 0 || queue.getConcurrency() > 0) {
                // Count pending workflows by executor
                String pendingQuery = "SELECT executor_id, COUNT(*) as task_count "
                        + " FROM %s.workflow_status " + " WHERE queue_name = ? "
                        + " AND status = 'PENDING' " + " GROUP BY executor_id; ";

                pendingQuery = String.format(pendingQuery, Constants.DB_SCHEMA);

                Map<String, Integer> pendingWorkflowsDict = new HashMap<>();
                try (PreparedStatement ps = connection.prepareStatement(pendingQuery)) {
                    ps.setString(1, queue.getName());

                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            pendingWorkflowsDict.put(rs.getString("executor_id"),
                                    rs.getInt("task_count"));
                        }
                    }
                }

                int localPendingWorkflows = pendingWorkflowsDict.getOrDefault(executorId, 0);

                // Check worker concurrency limit
                if (queue.getWorkerConcurrency() > 0) {
                    if (localPendingWorkflows > queue.getWorkerConcurrency()) {
                        logger.warn(String.format(
                                "The number of local pending workflows (%d) on queue %s exceeds the local concurrency limit (%d)",
                                localPendingWorkflows,
                                queue.getName(),
                                queue.getWorkerConcurrency()));
                    }
                    maxTasks = Math.max(0, queue.getWorkerConcurrency() - localPendingWorkflows);
                }

                // Check global concurrency limit
                if (queue.getConcurrency() > 0) {
                    int globalPendingWorkflows = pendingWorkflowsDict.values().stream()
                            .mapToInt(Integer::intValue).sum();

                    if (globalPendingWorkflows > queue.getConcurrency()) {
                        logger.warn(String.format(
                                "The total number of pending workflows (%d) on queue %s exceeds the global concurrency limit (%d)",
                                globalPendingWorkflows,
                                queue.getName(),
                                queue.getConcurrency()));
                    }

                    int availableTasks = Math.max(0,
                            queue.getConcurrency() - globalPendingWorkflows);
                    maxTasks = Math.min(maxTasks, availableTasks);
                }
            }

            // Build the main query to select workflows
            StringBuilder queryBuilder = new StringBuilder();
            queryBuilder.append(" SELECT workflow_uuid " + " FROM %s.workflow_status "
                    + " WHERE queue_name = ? " + " AND status = 'ENQUEUED' "
                    + " AND (application_version = ? OR application_version IS NULL) ");

            // Add ordering
            if (queue.isPriorityEnabled()) {
                queryBuilder.append(" ORDER BY priority ASC, created_at ASC");
            } else {
                queryBuilder.append(" ORDER BY created_at ASC");
            }

            // Add limit if not infinite
            if (maxTasks != Integer.MAX_VALUE) {
                queryBuilder.append(" LIMIT ?");
            }

            // Add FOR UPDATE NOWAIT or SKIP Locked
            if (queue.getConcurrency() > 0) {
                queryBuilder.append(" FOR UPDATE NOWAIT");
            } else {
                queryBuilder.append(" FOR UPDATE SKIP LOCKED");
            }

            String workflowsQuery = String.format(queryBuilder.toString(), Constants.DB_SCHEMA);

            List<String> dequeuedIds = new ArrayList<>();
            try (PreparedStatement ps = connection.prepareStatement(workflowsQuery)) {
                int paramIndex = 1;
                ps.setString(paramIndex++, queue.getName());
                ps.setString(paramIndex++, appVersion);

                if (maxTasks != Integer.MAX_VALUE) {
                    ps.setInt(paramIndex++, maxTasks);
                }

                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        dequeuedIds.add(rs.getString("workflow_uuid"));
                    }
                }
            }

            if (!dequeuedIds.isEmpty()) {
                logger.trace(String.format("[%s] dequeueing %d task(s)",
                        queue.getName(),
                        dequeuedIds.size()));
            }

            List<String> retIds = new ArrayList<>();

            // Update workflow status for each dequeued workflow
            String updateQuery = "UPDATE %s.workflow_status " + " SET status = 'PENDING', "
                    + " application_version = ?, " + " executor_id = ?, "
                    + " started_at_epoch_ms = ?, " + "workflow_deadline_epoch_ms = CASE "
                    + "    WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL "
                    + "       THEN ? + workflow_timeout_ms "
                    + "     ELSE workflow_deadline_epoch_ms " + "      END "
                    + " WHERE workflow_uuid = ?;";

            updateQuery = String.format(updateQuery, Constants.DB_SCHEMA);

            try (PreparedStatement updatePs = connection.prepareStatement(updateQuery)) {
                for (String id : dequeuedIds) {
                    // Check limiter again for each workflow
                    if (queue.hasLimiter()) {
                        if (retIds.size() + numRecentQueries >= queue.getRateLimit().getLimit()) {
                            break;
                        }
                    }

                    updatePs.setString(1, appVersion);
                    updatePs.setString(2, executorId);
                    updatePs.setLong(3, startTimeMs);
                    updatePs.setLong(4, startTimeMs);
                    updatePs.setString(5, id);

                    updatePs.addBatch();
                    retIds.add(id);
                }
                // TODO: should we be checking updateCounts?
                int[] updateCounts = updatePs.executeBatch();
            }

            connection.commit();
            return retIds;

        } catch (SQLException e) {
            logger.error("Error starting queued workflows", e);
            throw e;
        }
    }

    public List<WorkflowStatus> getQueuedWorkflows(ListQueuedWorkflowsInput input, boolean loadInput)
            throws SQLException {
        if (dataSource.isClosed()) {
            throw new IllegalStateException("Database is closed!");
        }

        StringBuilder queryBuilder = new StringBuilder();
        List<Object> parameters = new ArrayList<>();

        // Build the base SELECT clause
        queryBuilder.append("SELECT ")
                .append("workflow_uuid, ") // 1
                .append("status, ") // 2
                .append("name, ") // 3
                .append("recovery_attempts, ") // 4
                .append("config_name, ") // 5
                .append("class_name, ") // 6
                .append("authenticated_user, ") // 7
                .append("authenticated_roles, ") // 8
                .append("assumed_role, ") // 9
                .append("queue_name, ") // 10
                .append("executor_id, ") // 11
                .append("created_at, ") // 12
                .append("updated_at, ") // 13
                .append("application_version, ") // 14
                .append("application_id, ") // 15
                .append("workflow_deadline_epoch_ms, ") // 16
                .append("workflow_timeout_ms"); // 17

        if (loadInput) {
            queryBuilder.append(", inputs"); // 18
        }

        queryBuilder.append(" FROM dbos.workflow_status ");

        // Build WHERE clause
        queryBuilder.append("WHERE queue_name IS NOT NULL ")
                .append("AND status IN ('ENQUEUED', 'PENDING') ");

        // Add name filter
        if (input.getName() != null && !input.getName().trim().isEmpty()) {
            queryBuilder.append("AND name = ? ");
            parameters.add(input.getName());
        }

        // Add queue name filter
        if (input.getQueueName() != null && !input.getQueueName().trim().isEmpty()) {
            queryBuilder.append("AND queue_name = ? ");
            parameters.add(input.getQueueName());
        }

        // Add status filter (additional to the base ENQUEUED/PENDING filter)
        if (input.getStatus() != null && !input.getStatus().isEmpty()) {
            queryBuilder.append("AND status IN (");
            for (int i = 0; i < input.getStatus().size(); i++) {
                if (i > 0)
                    queryBuilder.append(", ");
                queryBuilder.append("?");
                parameters.add(input.getStatus().get(i));
            }
            queryBuilder.append(") ");
        }

        // Add start time filter
        if (input.getStartTime() != null) {
            queryBuilder.append("AND created_at >= ? ");
            parameters.add(input.getStartTime().toInstant().toEpochMilli());
        }

        // Add end time filter
        if (input.getEndTime() != null) {
            queryBuilder.append("AND created_at <= ? ");
            parameters.add(input.getEndTime().toInstant().toEpochMilli());
        }

        // Add ordering
        queryBuilder.append("ORDER BY created_at ");
        if (input.isSortDesc()) {
            queryBuilder.append("DESC ");
        } else {
            queryBuilder.append("ASC ");
        }

        // Add limit
        if (input.getLimit() != null && input.getLimit() > 0) {
            queryBuilder.append("LIMIT ? ");
            parameters.add(input.getLimit());
        }

        // Add offset
        if (input.getOffset() != null && input.getOffset() > 0) {
            queryBuilder.append("OFFSET ? ");
            parameters.add(input.getOffset());
        }

        // Execute query
        List<WorkflowStatus> workflowStatuses = new ArrayList<>();

        try (Connection connection = dataSource.getConnection();
                PreparedStatement stmt = connection.prepareStatement(queryBuilder.toString())) {

            // Set parameters
            for (int i = 0; i < parameters.size(); i++) {
                stmt.setObject(i + 1, parameters.get(i));
            }

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    WorkflowStatus info = new WorkflowStatus();

                    info.setWorkflowId(rs.getString(1));
                    info.setStatus(rs.getString(2));
                    info.setName(rs.getString(3));
                    info.setRecoveryAttempts(rs.getInt(4));
                    info.setConfigName(rs.getString(5));
                    info.setClassName(rs.getString(6));
                    info.setAuthenticatedUser(rs.getString(7));

                    // Parse authenticated_roles JSON
                    String rolesJson = rs.getString(8);
                    if (rolesJson != null) {
                        info.setAuthenticatedRoles((String[]) JSONUtil.deserializeToArray(rolesJson));
                    }

                    info.setAssumedRole(rs.getString(9));
                    info.setQueueName(rs.getString(10));
                    info.setExecutorId(rs.getString(11));
                    info.setCreatedAt(rs.getLong(12));
                    info.setUpdatedAt(rs.getLong(13));
                    info.setAppVersion(rs.getString(14));
                    info.setAppId(rs.getString(15));
                    info.setWorkflowDeadlineEpochMs(rs.getLong(16));
                    info.setWorkflowTimeoutMs(rs.getLong(17));

                    String rawInput = null;
                    if (loadInput) {
                        rawInput = rs.getString(18);
                    }

                    info.setInput(JSONUtil.deserializeToArray(rawInput));
                    info.setOutput(null);
                    info.setError(null);

                    workflowStatuses.add(info);
                }
            }
        }

        return workflowStatuses;
    }

    public boolean clearQueueAssignment(String workflowId) throws SQLException {
        if (dataSource.isClosed()) {
            throw new IllegalStateException("Database is closed!");
        }

        String sqlTemplate = "UPDATE %s.workflow_status "
                + "SET started_at_epoch_ms = NULL, status = ? "
                + "WHERE workflow_uuid = ? AND queue_name is NOT NULL AND status = ?";
        final String sql = String.format(sqlTemplate, Constants.DB_SCHEMA);
        try (Connection connection = dataSource.getConnection();
                PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, WorkflowState.ENQUEUED.name());
            stmt.setString(2, workflowId);
            stmt.setString(3, WorkflowState.PENDING.name());

            int affectedRows = stmt.executeUpdate();
            return affectedRows > 0;
        }
    }
}
