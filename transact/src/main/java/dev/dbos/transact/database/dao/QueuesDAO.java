package dev.dbos.transact.database.dao;

import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.workflow.Field;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.QueueUpdate;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueuesDAO {

  private QueuesDAO() {}

  private static final Logger logger = LoggerFactory.getLogger(QueuesDAO.class);

  public static List<String> getAndStartQueuedWorkflows(
      DbContext ctx, Queue queue, String executorId, String appVersion, String partitionKey)
      throws SQLException {

    if (partitionKey != null && partitionKey.length() == 0) {
      partitionKey = null;
    }

    try (Connection connection = ctx.getConnection()) {
      connection.setAutoCommit(false);

      try (Statement stmt = connection.createStatement()) {
        stmt.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ");
      }

      int numRecentQueries = 0;

      var rateLimit = queue.rateLimit();
      if (rateLimit != null) {
        var cutoffTime = Instant.now().minus(rateLimit.period());

        var limiterQuery =
            """
              SELECT COUNT(*)
              FROM "%s".workflow_status
              WHERE queue_name = ?
              AND status NOT IN (?, ?)
              AND started_at_epoch_ms > ?
            """
                .formatted(ctx.schema());
        if (partitionKey != null) {
          limiterQuery += " AND queue_partition_key = ?";
        }

        try (PreparedStatement ps = connection.prepareStatement(limiterQuery)) {
          ps.setString(1, queue.name());
          ps.setString(2, WorkflowState.ENQUEUED.name());
          ps.setString(3, WorkflowState.DELAYED.name());
          ps.setLong(4, cutoffTime.toEpochMilli());
          if (partitionKey != null) {
            ps.setString(5, partitionKey);
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

      int maxTasks = 100;

      if (queue.workerConcurrency() != null || queue.concurrency() != null) {
        String pendingQuery =
            """
              SELECT executor_id, COUNT(*) as task_count
              FROM "%s".workflow_status
              WHERE queue_name = ? AND status = ?
            """
                .formatted(ctx.schema());
        if (partitionKey != null) {
          pendingQuery += " AND queue_partition_key = ?";
        }
        pendingQuery += " GROUP BY executor_id";

        Map<String, Integer> pendingWorkflows = new HashMap<>();
        try (PreparedStatement ps = connection.prepareStatement(pendingQuery)) {
          ps.setString(1, queue.name());
          ps.setString(2, WorkflowState.PENDING.name());
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

      var query =
          """
              SELECT workflow_uuid
              FROM "%s".workflow_status
              WHERE queue_name = ?
                AND status = ?
                AND (application_version = ? OR application_version IS NULL)
          """
              .formatted(ctx.schema());
      if (partitionKey != null) {
        query += " AND queue_partition_key = ?";
      }

      if (queue.priorityEnabled()) {
        query += " ORDER BY priority ASC, created_at ASC";
      } else {
        query += " ORDER BY created_at ASC";
      }

      if (queue.concurrency() == null) {
        query += " FOR UPDATE SKIP LOCKED";
      } else {
        query += " FOR UPDATE NOWAIT";
      }

      query += " LIMIT %d".formatted(maxTasks);

      List<String> dequeuedWorkflowIds = new ArrayList<>();
      try (var ps = connection.prepareStatement(query)) {
        ps.setString(1, queue.name());
        ps.setString(2, WorkflowState.ENQUEUED.name());
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

      var now = System.currentTimeMillis();
      List<String> updatedWorkflowIds = new ArrayList<>();
      String updateQuery =
          """
        UPDATE "%s".workflow_status
        SET status = ?,
            application_version = ?,
            executor_id = ?,
            started_at_epoch_ms = ?,
            workflow_deadline_epoch_ms = CASE
                WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                THEN (EXTRACT(epoch FROM now()) * 1000)::bigint + workflow_timeout_ms
                ELSE workflow_deadline_epoch_ms
            END
        WHERE workflow_uuid = ?
          """
              .formatted(ctx.schema());

      try (var ps = connection.prepareStatement(updateQuery)) {
        for (var id : dequeuedWorkflowIds) {
          if (queue.rateLimit() != null) {
            if (updatedWorkflowIds.size() + numRecentQueries >= queue.rateLimit().limit()) {
              break;
            }
          }

          ps.setString(1, WorkflowState.PENDING.name());
          ps.setString(2, appVersion);
          ps.setString(3, executorId);
          ps.setLong(4, now);
          ps.setString(5, id);
          ps.executeUpdate();
          updatedWorkflowIds.add(id);
        }
      }

      if (!updatedWorkflowIds.isEmpty()) {
        connection.commit();
      } else {
        connection.rollback();
      }

      return updatedWorkflowIds;
    }
  }

  public static boolean clearQueueAssignment(DbContext ctx, String workflowId) throws SQLException {

    final String sql =
        """
          UPDATE "%s".workflow_status
          SET started_at_epoch_ms = NULL, status = ?
          WHERE workflow_uuid = ? AND queue_name IS NOT NULL AND status = ?
        """
            .formatted(ctx.schema());
    try (Connection connection = ctx.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, WorkflowState.ENQUEUED.name());
      stmt.setString(2, workflowId);
      stmt.setString(3, WorkflowState.PENDING.name());

      int affectedRows = stmt.executeUpdate();
      return affectedRows > 0;
    }
  }

  public static List<String> getQueuePartitions(DbContext ctx, String queueName)
      throws SQLException {

    final String sql =
        """
          SELECT DISTINCT queue_partition_key
          FROM "%s".workflow_status
          WHERE queue_name = ?
            AND status = ?
            AND queue_partition_key IS NOT NULL
        """
            .formatted(ctx.schema());

    try (Connection connection = ctx.getConnection();
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

  /**
   * Upsert a queue row. Returns true iff a new row was inserted (i.e. the queue did not previously
   * exist). Returns false if the row already existed, regardless of whether it was updated.
   */
  public static boolean upsertQueue(DbContext ctx, Queue queue, boolean updateExisting)
      throws SQLException {
    final String conflictClause =
        updateExisting
            ? """
              ON CONFLICT (name) DO UPDATE SET
                concurrency           = EXCLUDED.concurrency,
                worker_concurrency    = EXCLUDED.worker_concurrency,
                rate_limit_max        = EXCLUDED.rate_limit_max,
                rate_limit_period_sec = EXCLUDED.rate_limit_period_sec,
                priority_enabled      = EXCLUDED.priority_enabled,
                partition_queue       = EXCLUDED.partition_queue,
                polling_interval_sec  = EXCLUDED.polling_interval_sec,
                updated_at            = EXCLUDED.updated_at
              """
            : "ON CONFLICT (name) DO NOTHING";
    final String insertSql =
        """
        INSERT INTO "%s".queues
          (name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec,
            priority_enabled, partition_queue, polling_interval_sec, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        %s
        """
            .formatted(ctx.schema(), conflictClause);
    final String existsSql = "SELECT 1 FROM \"%s\".queues WHERE name = ?".formatted(ctx.schema());

    try (Connection connection = ctx.getConnection()) {
      connection.setAutoCommit(false);
      try {
        boolean existed;
        try (PreparedStatement ps = connection.prepareStatement(existsSql)) {
          ps.setString(1, queue.name());
          try (ResultSet rs = ps.executeQuery()) {
            existed = rs.next();
          }
        }

        try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
          ps.setString(1, queue.name());
          setNullableInt(ps, 2, queue.concurrency());
          setNullableInt(ps, 3, queue.workerConcurrency());
          var rateLimit = queue.rateLimit();
          if (rateLimit != null) {
            ps.setInt(4, rateLimit.limit());
            ps.setDouble(5, rateLimit.period().toMillis() / 1000.0);
          } else {
            ps.setNull(4, java.sql.Types.INTEGER);
            ps.setNull(5, java.sql.Types.DOUBLE);
          }
          ps.setBoolean(6, queue.priorityEnabled());
          ps.setBoolean(7, queue.partitioningEnabled());
          ps.setDouble(8, queue.pollingInterval().toMillis() / 1000.0);
          ps.setLong(9, System.currentTimeMillis());
          ps.executeUpdate();
        }

        connection.commit();
        return !existed;
      } catch (SQLException e) {
        connection.rollback();
        throw e;
      }
    }
  }

  public static Optional<Queue> getQueue(DbContext ctx, String name) throws SQLException {
    final String sql =
        """
        SELECT name, concurrency, worker_concurrency,
          rate_limit_max, rate_limit_period_sec,
          priority_enabled, partition_queue, polling_interval_sec
        FROM "%s".queues
        WHERE name = ?
        """
            .formatted(ctx.schema());

    try (Connection connection = ctx.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, name);
      try (ResultSet rs = stmt.executeQuery()) {
        if (rs.next()) {
          return Optional.of(queueFromResultSet(rs));
        }
        return Optional.empty();
      }
    }
  }

  public static List<Queue> listQueues(DbContext ctx) throws SQLException {
    final String sql =
        """
        SELECT name, concurrency, worker_concurrency,
          rate_limit_max, rate_limit_period_sec,
          priority_enabled, partition_queue, polling_interval_sec
        FROM "%s".queues
        ORDER BY name
        """
            .formatted(ctx.schema());

    try (Connection connection = ctx.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql);
        ResultSet rs = stmt.executeQuery()) {
      List<Queue> queues = new ArrayList<>();
      while (rs.next()) {
        queues.add(queueFromResultSet(rs));
      }
      return queues;
    }
  }

  public static void updateQueue(DbContext ctx, String name, QueueUpdate update)
      throws SQLException {
    if (update.isEmpty()) return;

    List<String> setClauses = new ArrayList<>();
    List<Object> params = new ArrayList<>();

    collectField(setClauses, params, "concurrency", update.concurrency());
    collectField(setClauses, params, "worker_concurrency", update.workerConcurrency());
    collectField(setClauses, params, "rate_limit_max", update.rateLimitMax());
    collectField(setClauses, params, "rate_limit_period_sec", update.rateLimitPeriodSec());
    collectField(setClauses, params, "priority_enabled", update.priorityEnabled());
    collectField(setClauses, params, "partition_queue", update.partitionQueue());
    collectField(setClauses, params, "polling_interval_sec", update.pollingIntervalSec());

    setClauses.add("\"updated_at\" = ?");
    params.add(System.currentTimeMillis());
    params.add(name);

    String sql =
        "UPDATE \"%s\".queues SET %s WHERE name = ?"
            .formatted(ctx.schema(), String.join(", ", setClauses));

    try (Connection connection = ctx.getConnection();
        PreparedStatement ps = connection.prepareStatement(sql)) {
      for (int i = 0; i < params.size(); i++) {
        ps.setObject(i + 1, params.get(i));
      }
      ps.executeUpdate();
    }
  }

  private static <T> void collectField(
      List<String> clauses, List<Object> params, String column, Field<T> field) {
    if (field.isPresent()) {
      clauses.add("\"" + column + "\" = ?");
      params.add(field.get());
    }
  }

  public static boolean deleteQueue(DbContext ctx, String name) throws SQLException {
    final String sql = "DELETE FROM \"%s\".queues WHERE name = ?".formatted(ctx.schema());

    try (Connection connection = ctx.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, name);
      return stmt.executeUpdate() > 0;
    }
  }

  private static Queue queueFromResultSet(ResultSet rs) throws SQLException {
    String name = rs.getString("name");
    Integer concurrency = rs.getObject("concurrency", Integer.class);
    Integer workerConcurrency = rs.getObject("worker_concurrency", Integer.class);
    Integer rateLimitMax = rs.getObject("rate_limit_max", Integer.class);
    Double rateLimitPeriodSec = rs.getObject("rate_limit_period_sec", Double.class);
    boolean priorityEnabled = rs.getBoolean("priority_enabled");
    boolean partitioningEnabled = rs.getBoolean("partition_queue");
    Double pollingIntervalSec = rs.getObject("polling_interval_sec", Double.class);

    Queue.RateLimit rateLimit = null;
    if (rateLimitMax != null && rateLimitPeriodSec != null) {
      rateLimit =
          new Queue.RateLimit(rateLimitMax, Duration.ofMillis((long) (rateLimitPeriodSec * 1000)));
    }
    Duration pollingInterval =
        pollingIntervalSec != null
            ? Duration.ofMillis((long) (pollingIntervalSec * 1000))
            : Queue.DEFAULT_POLLING_INTERVAL;
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        priorityEnabled,
        partitioningEnabled,
        rateLimit,
        pollingInterval);
  }

  private static void setNullableInt(PreparedStatement stmt, int index, Integer value)
      throws SQLException {
    if (value != null) {
      stmt.setInt(index, value);
    } else {
      stmt.setNull(index, java.sql.Types.INTEGER);
    }
  }
}
