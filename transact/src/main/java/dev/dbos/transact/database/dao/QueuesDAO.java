package dev.dbos.transact.database.dao;

import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.workflow.Field;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.QueueOptions;
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

    if (partitionKey != null && partitionKey.isEmpty()) {
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
  public static boolean upsertQueue(
      DbContext ctx, String name, QueueOptions options, boolean updateExisting)
      throws SQLException {
    Queue queue = queueFromOptions(name, options);
    final String insertSql =
        """
        INSERT INTO "%s".queues
          (name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec,
            priority_enabled, partition_queue, polling_interval_sec, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (name) DO NOTHING
        """
            .formatted(ctx.schema());
    final String updateSql =
        """
        UPDATE "%s".queues SET
          concurrency           = ?,
          worker_concurrency    = ?,
          rate_limit_max        = ?,
          rate_limit_period_sec = ?,
          priority_enabled      = ?,
          partition_queue       = ?,
          polling_interval_sec  = ?,
          updated_at            = ?
        WHERE name = ?
        """
            .formatted(ctx.schema());

    try (Connection connection = ctx.getConnection()) {
      boolean inserted;
      try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
        bindQueueParams(ps, queue, 1);
        inserted = ps.executeUpdate() == 1;
      }
      if (!inserted && updateExisting) {
        try (PreparedStatement ps = connection.prepareStatement(updateSql)) {
          setNullableInt(ps, 1, queue.concurrency());
          setNullableInt(ps, 2, queue.workerConcurrency());
          var rateLimit = queue.rateLimit();
          if (rateLimit != null) {
            ps.setInt(3, rateLimit.limit());
            ps.setDouble(4, rateLimit.period().toMillis() / 1000.0);
          } else {
            ps.setNull(3, java.sql.Types.INTEGER);
            ps.setNull(4, java.sql.Types.DOUBLE);
          }
          ps.setBoolean(5, queue.priorityEnabled());
          ps.setBoolean(6, queue.partitioningEnabled());
          ps.setDouble(7, queue.pollingInterval().toMillis() / 1000.0);
          ps.setLong(8, System.currentTimeMillis());
          ps.setString(9, queue.name());
          ps.executeUpdate();
        }
      }
      return inserted;
    }
  }

  private static void bindQueueParams(PreparedStatement ps, Queue queue, int offset)
      throws SQLException {
    ps.setString(offset, queue.name());
    setNullableInt(ps, offset + 1, queue.concurrency());
    setNullableInt(ps, offset + 2, queue.workerConcurrency());
    var rateLimit = queue.rateLimit();
    if (rateLimit != null) {
      ps.setInt(offset + 3, rateLimit.limit());
      ps.setDouble(offset + 4, rateLimit.period().toMillis() / 1000.0);
    } else {
      ps.setNull(offset + 3, java.sql.Types.INTEGER);
      ps.setNull(offset + 4, java.sql.Types.DOUBLE);
    }
    ps.setBoolean(offset + 5, queue.priorityEnabled());
    ps.setBoolean(offset + 6, queue.partitioningEnabled());
    ps.setDouble(offset + 7, queue.pollingInterval().toMillis() / 1000.0);
    ps.setLong(offset + 8, System.currentTimeMillis());
  }

  public static Optional<Queue> findQueue(DbContext ctx, String name) throws SQLException {
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

  public static void updateQueue(DbContext ctx, String name, QueueOptions update)
      throws SQLException {
    if (update.isEmpty()) return;

    List<String> setClauses = new ArrayList<>();
    List<Object> params = new ArrayList<>();

    collectField(setClauses, params, "concurrency", update.concurrency());
    collectField(setClauses, params, "worker_concurrency", update.workerConcurrency());
    collectField(setClauses, params, "rate_limit_max", update.rateLimitMax());
    collectField(
        setClauses, params, "rate_limit_period_sec", durationToSec(update.rateLimitPeriod()));
    collectOptional(setClauses, params, "priority_enabled", update.priorityEnabled());
    collectOptional(setClauses, params, "partition_queue", update.partitionQueue());
    collectOptional(
        setClauses, params, "polling_interval_sec", durationToSec(update.pollingInterval()));

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

  private static <T> void collectOptional(
      List<String> clauses, List<Object> params, String column, Optional<T> opt) {
    opt.ifPresent(
        value -> {
          clauses.add("\"" + column + "\" = ?");
          params.add(value);
        });
  }

  private static Field<Double> durationToSec(Field<Duration> field) {
    if (!field.isPresent()) return Field.absent();
    Duration d = field.get();
    return Field.of(d != null ? d.toMillis() / 1000.0 : null);
  }

  private static Optional<Double> durationToSec(Optional<Duration> opt) {
    return opt.map(d -> d.toMillis() / 1000.0);
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

  private static Queue queueFromOptions(String name, QueueOptions options) {
    Integer concurrencyVal = options.concurrency().isPresent() ? options.concurrency().get() : null;
    Integer workerConcurrencyVal =
        options.workerConcurrency().isPresent() ? options.workerConcurrency().get() : null;
    boolean priorityEnabledVal = options.priorityEnabled().orElse(false);
    boolean partitionQueueVal = options.partitionQueue().orElse(false);

    Queue.RateLimit rateLimit = null;
    if (options.rateLimitMax().isPresent()
        && options.rateLimitPeriod().isPresent()
        && options.rateLimitMax().get() != null
        && options.rateLimitPeriod().get() != null) {
      rateLimit =
          new Queue.RateLimit(options.rateLimitMax().get(), options.rateLimitPeriod().get());
    }

    Duration pollingIntervalVal = options.pollingInterval().orElse(Queue.DEFAULT_POLLING_INTERVAL);

    return new Queue(
        name,
        concurrencyVal,
        workerConcurrencyVal,
        priorityEnabledVal,
        partitionQueueVal,
        rateLimit,
        pollingIntervalVal);
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
