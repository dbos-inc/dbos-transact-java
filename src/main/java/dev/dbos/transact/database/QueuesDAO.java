package dev.dbos.transact.database;

import dev.dbos.transact.Constants;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueuesDAO {
  private static final Logger logger = LoggerFactory.getLogger(QueuesDAO.class);
  private HikariDataSource dataSource;

  QueuesDAO(HikariDataSource ds) {
    dataSource = ds;
  }

  /**
   * Get queued workflows based on queue configuration and concurrency limits.
   *
   * @param queue The queue configuration
   * @param executorId The executor ID
   * @param appVersion The application version
   * @return List of workflow UUIDs that are due for execution
   */
  public List<String> getAndStartQueuedWorkflows(Queue queue, String executorId, String appVersion)
      throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    long startTimeMs = System.currentTimeMillis();
    Long limiterPeriodMs = null;
    if (queue.hasLimiter()) {
      limiterPeriodMs = (long) (queue.rateLimit().period() * 1000);
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
        final String limiterQuery =
            """
              SELECT COUNT(*)
              FROM %s.workflow_status
              WHERE queue_name = ?
              AND status != 'ENQUEUED'
              AND started_at_epoch_ms > ?;
              """
                .formatted(Constants.DB_SCHEMA);

        try (PreparedStatement ps = connection.prepareStatement(limiterQuery)) {
          ps.setString(1, queue.name());
          ps.setLong(2, startTimeMs - limiterPeriodMs);

          try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
              numRecentQueries = rs.getInt(1);
            }
          }
        }

        if (numRecentQueries >= queue.rateLimit().limit()) {
          return new ArrayList<>();
        }
      }

      // Calculate max tasks based on concurrency limits
      int maxTasks = 100;

      if (queue.workerConcurrency() > 0 || queue.concurrency() > 0) {
        // Count pending workflows by executor
        final String pendingQuery =
            """
            SELECT executor_id, COUNT(*) as task_count
            FROM %s.workflow_status
            WHERE queue_name = ? AND status = 'PENDING'
            GROUP BY executor_id;
            """
                .formatted(Constants.DB_SCHEMA);

        Map<String, Integer> pendingWorkflowsDict = new HashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(pendingQuery)) {
          ps.setString(1, queue.name());

          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              pendingWorkflowsDict.put(rs.getString("executor_id"), rs.getInt("task_count"));
            }
          }
        }

        int localPendingWorkflows = pendingWorkflowsDict.getOrDefault(executorId, 0);

        // Check worker concurrency limit
        if (queue.workerConcurrency() > 0) {
          maxTasks = Math.max(0, queue.workerConcurrency() - localPendingWorkflows);
        }

        // Check global concurrency limit
        if (queue.concurrency() > 0) {
          int globalPendingWorkflows =
              pendingWorkflowsDict.values().stream().mapToInt(Integer::intValue).sum();

          if (globalPendingWorkflows > queue.concurrency()) {
            logger.warn(
                "The total number of pending workflows ({}) on queue {} exceeds the global concurrency limit ({})",
                globalPendingWorkflows,
                queue.name(),
                queue.concurrency());
          }

          int availableTasks = Math.max(0, queue.concurrency() - globalPendingWorkflows);
          maxTasks = Math.min(maxTasks, availableTasks);
        }
      }

      // Build the main query to select workflows
      StringBuilder queryBuilder =
          new StringBuilder(
              """
                SELECT workflow_uuid
                FROM %s.workflow_status
                WHERE queue_name = ?
                  AND status = 'ENQUEUED'
                  AND (application_version = ? OR application_version IS NULL)
              """);

      // Add ordering
      if (queue.priorityEnabled()) {
        queryBuilder.append(" ORDER BY priority ASC, created_at ASC");
      } else {
        queryBuilder.append(" ORDER BY created_at ASC");
      }

      // Add limit if not infinite
      if (maxTasks != Integer.MAX_VALUE) {
        queryBuilder.append(" LIMIT ?");
      }

      // Add FOR UPDATE NOWAIT or SKIP Locked
      if (queue.concurrency() > 0) {
        queryBuilder.append(" FOR UPDATE NOWAIT");
      } else {
        queryBuilder.append(" FOR UPDATE SKIP LOCKED");
      }

      String workflowsQuery = queryBuilder.toString().formatted(Constants.DB_SCHEMA);

      List<String> dequeuedIds = new ArrayList<>();
      try (PreparedStatement ps = connection.prepareStatement(workflowsQuery)) {
        int paramIndex = 1;
        ps.setString(paramIndex++, queue.name());
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
        logger.debug("{} dequeueing {} task(s)", queue.name(), dequeuedIds.size());
      }

      List<String> retIds = new ArrayList<>();

      // Update workflow status for each dequeued workflow
      final String updateQuery =
          """
            UPDATE %s.workflow_status
            SET status = 'PENDING',
              application_version = ?,
              executor_id = ?,
              started_at_epoch_ms = ?,
              workflow_deadline_epoch_ms = CASE
                WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                THEN ? + workflow_timeout_ms
                ELSE workflow_deadline_epoch_ms
              END
            WHERE workflow_uuid = ?;
          """
              .formatted(Constants.DB_SCHEMA);

      try (PreparedStatement updatePs = connection.prepareStatement(updateQuery)) {
        for (String id : dequeuedIds) {
          // Check limiter again for each workflow
          if (queue.hasLimiter()) {
            if (retIds.size() + numRecentQueries >= queue.rateLimit().limit()) {
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

        updatePs.executeBatch();
      }

      connection.commit();
      return retIds;

    } catch (SQLException e) {
      logger.error("Error starting queued workflows", e);
      throw e;
    }
  }

  public boolean clearQueueAssignment(String workflowId) throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    final String sql =
        """
          UPDATE %s.workflow_status
          SET started_at_epoch_ms = NULL, status = ?
          WHERE workflow_uuid = ? AND queue_name IS NOT NULL AND status = ?
        """
            .formatted(Constants.DB_SCHEMA);
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
