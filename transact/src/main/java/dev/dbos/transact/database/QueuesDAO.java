package dev.dbos.transact.database;

import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class QueuesDAO {
  private static final Logger logger = LoggerFactory.getLogger(QueuesDAO.class);

  private final HikariDataSource dataSource;
  private final String schema;

  QueuesDAO(HikariDataSource ds, String schema) {
    this.dataSource = ds;
    this.schema = Objects.requireNonNull(schema);
  }

  /**
   * Get queued workflows based on queue configuration and concurrency limits.
   *
   * @param queue The queue configuration
   * @param executorId The executor ID
   * @param appVersion The application version
   * @return List of workflow UUIDs that are due for execution
   */
  List<String> getAndStartQueuedWorkflows(
      Queue queue, String executorId, String appVersion, String partitionKey) throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    if (partitionKey != null && partitionKey.length() == 0) {
      partitionKey = null;
    }

    try (Connection connection = dataSource.getConnection()) {
      connection.setAutoCommit(false);

      // Set snapshot isolation level
      try (Statement stmt = connection.createStatement()) {
        stmt.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
      }

      int numRecentQueries = 0;

      // First check the rate limiter
      var rateLimit = queue.rateLimit();
      if (rateLimit != null) {
        // Calculate the cutoff time: current time minus limiter period
        var cutoffTime = Instant.now().minus(rateLimit.period());

        // Count workflows that have started in the limiter period
        var limiterQuery =
            """
              SELECT COUNT(*)
              FROM %s.workflow_status
              WHERE queue_name = ?
              AND status != ?
              AND started_at_epoch_ms > ?
            """
                .formatted(this.schema);
        if (partitionKey != null) {
          limiterQuery += " AND queue_partition_key = ?";
        }

        try (PreparedStatement ps = connection.prepareStatement(limiterQuery)) {
          ps.setString(1, queue.name());
          ps.setString(2, WorkflowState.ENQUEUED.toString());
          ps.setLong(3, cutoffTime.toEpochMilli());
          if (partitionKey != null) {
            ps.setString(4, partitionKey);
          }

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

      // Calculate max_tasks based on concurrency limits
      int maxTasks = 100;

      if (queue.workerConcurrency() != null || queue.concurrency() != null) {
        // Count pending workflows by executor
        String pendingQuery =
            """
              SELECT executor_id, COUNT(*) as task_count
              FROM %s.workflow_status
              WHERE queue_name = ? AND status = ?
            """
                .formatted(this.schema);
        if (partitionKey != null) {
          pendingQuery += " AND queue_partition_key = ?";
        }
        pendingQuery += " GROUP BY executor_id";

        Map<String, Integer> pendingWorkflows = new HashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(pendingQuery)) {
          ps.setString(1, queue.name());
          ps.setString(2, WorkflowState.PENDING.toString());
          if (partitionKey != null) {
            ps.setString(3, partitionKey);
          }

          try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
              var executor = rs.getString("executor_id");
              var count = rs.getInt("task_count");
              pendingWorkflows.put(executor, count);
            }
          }
        }

        int localPendingWorkflows = pendingWorkflows.getOrDefault(executorId, 0);

        // Check worker concurrency limit
        if (queue.workerConcurrency() != null) {
          if (localPendingWorkflows > queue.workerConcurrency()) {
            logger.warn(
                "Local pending workflows ({}) on queue {} exceeds worker concurrency limit ({})",
                localPendingWorkflows,
                queue.name(),
                queue.workerConcurrency());
          }
          maxTasks = Math.max(queue.workerConcurrency() - localPendingWorkflows, 0);
        }

        // Check global concurrency limit
        if (queue.concurrency() != null) {
          var globalPendingWorkflows = 0;
          for (var count : pendingWorkflows.values()) {
            globalPendingWorkflows += count;
          }

          if (globalPendingWorkflows > queue.concurrency()) {
            logger.warn(
                "Total pending workflows ({}) on queue {} exceeds the global concurrency limit ({})",
                globalPendingWorkflows,
                queue.name(),
                queue.concurrency());
          }

          int availableTasks = Math.max(0, queue.concurrency() - globalPendingWorkflows);
          if (availableTasks < maxTasks) {
            maxTasks = availableTasks;
          }
        }
      }

      if (maxTasks <= 0) {
        return new ArrayList<>();
      }

      // Build the query to select workflows for dequeueing
      var query =
          """
              SELECT workflow_uuid
              FROM %s.workflow_status
              WHERE queue_name = ?
                AND status = ?
                AND (application_version = ? OR application_version IS NULL)
          """
              .formatted(this.schema);
      if (partitionKey != null) {
        query += " AND queue_partition_key = ?";
      }

      // Add partition key filter if provided
      if (queue.priorityEnabled()) {
        query += " ORDER BY priority ASC, created_at ASC";
      } else {
        query += " ORDER BY created_at ASC";
      }

      // Use SKIP LOCKED when no global concurrency is set to avoid blocking,
      // otherwise use NOWAIT to ensure consistent view across processes
      if (queue.concurrency() == null) {
        query += " FOR UPDATE SKIP LOCKED";
      } else {
        query += " FOR UPDATE NOWAIT";
      }

      query += " LIMIT %d".formatted(maxTasks);

      // Execute the query to get workflow IDs
      List<String> dequeuedWorkflowIds = new ArrayList<>();
      try (var ps = connection.prepareStatement(query)) {
        ps.setString(1, queue.name());
        ps.setString(2, WorkflowState.ENQUEUED.toString());
        ps.setString(3, appVersion);
        if (partitionKey != null) {
          ps.setString(4, partitionKey);
        }

        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            dequeuedWorkflowIds.add(rs.getString("workflow_uuid"));
          }
        }
      }

      if (!dequeuedWorkflowIds.isEmpty()) {
        logger.debug(
            "attempting to dequeue {} task(s) from {} queue",
            dequeuedWorkflowIds.size(),
            queue.name());
      }

      // Update workflows to PENDING status
      var now = System.currentTimeMillis();
      List<String> updatedWorkflowIds = new ArrayList<>();
      String updateQuery =
          """
        UPDATE %s.workflow_status
        SET status = ?,
            application_version = ?,
            executor_id = ?,
            started_at_epoch_ms = ?,
            workflow_deadline_epoch_ms = CASE
                WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                THEN EXTRACT(epoch FROM NOW()) * 1000 + workflow_timeout_ms
                ELSE workflow_deadline_epoch_ms
            END
        WHERE workflow_uuid = ?
          """
              .formatted(this.schema);

      try (var ps = connection.prepareStatement(updateQuery)) {
        for (var id : dequeuedWorkflowIds) {
          if (queue.rateLimit() != null) {
            if (updatedWorkflowIds.size() + numRecentQueries >= queue.rateLimit().limit()) {
              break;
            }
          }

          ps.setString(1, WorkflowState.PENDING.toString());
          ps.setString(2, appVersion);
          ps.setString(3, executorId);
          ps.setLong(4, now);
          ps.setString(5, id);
          ps.executeUpdate();
          updatedWorkflowIds.add(id);
        }
      }

      // Commit only if workflows were dequeued. Avoids WAL bloat and XID advancement.
      if (updatedWorkflowIds.size() > 0) {
        connection.commit();
      } else {
        connection.rollback();
      }

      return updatedWorkflowIds;
    }
  }

  boolean clearQueueAssignment(String workflowId) throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    final String sql =
        """
          UPDATE %s.workflow_status
          SET started_at_epoch_ms = NULL, status = ?
          WHERE workflow_uuid = ? AND queue_name IS NOT NULL AND status = ?
        """
            .formatted(this.schema);
    try (Connection connection = dataSource.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, WorkflowState.ENQUEUED.name());
      stmt.setString(2, workflowId);
      stmt.setString(3, WorkflowState.PENDING.name());

      int affectedRows = stmt.executeUpdate();
      return affectedRows > 0;
    }
  }

  List<String> getQueuePartitions(String queueName) throws SQLException {
    if (dataSource.isClosed()) {
      throw new IllegalStateException("Database is closed!");
    }

    final String sql =
        """
          SELECT DISTINCT queue_partition_key
          FROM %s.workflow_status
          WHERE queue_name = ?
            AND status = ?
            AND queue_partition_key IS NOT NULL
        """
            .formatted(this.schema);

    try (Connection connection = dataSource.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, queueName);
      stmt.setString(2, WorkflowState.ENQUEUED.name());

      try (ResultSet rs = stmt.executeQuery()) {
        List<String> partitions = new ArrayList<>();
        while (rs.next()) {
          String partitionKey = rs.getString("queue_partition_key");
          partitions.add(partitionKey);
        }
        return partitions;
      }
    }
  }
}
