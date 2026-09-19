package dev.dbos.transact.database.dao;

import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.workflow.Field;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.QueueOptions;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueuesDAO {

  private QueuesDAO() {}

  private static final Logger logger = LoggerFactory.getLogger(QueuesDAO.class);

  public static List<String> startQueuedWorkflows(
      DbContext ctx,
      Queue queue,
      String executorId,
      String appVersion,
      String partitionKey,
      long localRunningCount)
      throws SQLException {

    if (partitionKey != null && partitionKey.isEmpty()) {
      partitionKey = null;
    }

    try (Connection connection = ctx.getConnection()) {
      connection.setAutoCommit(false);
      // Use REPEATABLE READ only when global flow control (concurrency or rate limit) is active.
      // Local worker concurrency is tracked in-memory and does not need a consistent DB snapshot.
      if (queue.concurrency() != null || queue.rateLimit() != null) {
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      }

      try {
        long maxTasks = Integer.MAX_VALUE;

        // Worker concurrency uses the caller-supplied in-memory count — no DB round trip needed.
        if (queue.workerConcurrency() != null) {
          maxTasks = Math.max(0, queue.workerConcurrency() - localRunningCount);
          if (maxTasks == 0) {
            // Nothing claimable. End the transaction here rather than leaving it open for
            // the pool to roll back on return.
            connection.rollback();
            return List.of();
          }
        }

        // If there is a rate limit, compute how many functions have started in its period.
        if (queue.rateLimit() != null) {
          var rateLimit = queue.rateLimit();

          var limiterQuery =
              """
              SELECT COUNT(*)
              FROM "%s".workflow_status
              WHERE queue_name = ?
              AND rate_limited = true
              AND status NOT IN (?, ?)
              AND started_at_epoch_ms > ?
            """
                      .formatted(ctx.schema())
                  + ctx.andAppScope();
          if (partitionKey != null) {
            limiterQuery += " AND queue_partition_key = ?";
          }

          try (PreparedStatement ps = connection.prepareStatement(limiterQuery)) {
            ps.setString(1, queue.name());
            ps.setString(2, WorkflowState.ENQUEUED.name());
            ps.setString(3, WorkflowState.DELAYED.name());
            ps.setLong(4, Instant.now().minus(rateLimit.period()).toEpochMilli());
            var index = ctx.bindAppScope(ps, 5);
            if (partitionKey != null) {
              ps.setString(index, partitionKey);
            }

            int numRecentQueries = 0;
            try (ResultSet rs = ps.executeQuery()) {
              if (rs.next()) {
                numRecentQueries = rs.getInt(1);
              }
            }

            // Bound the claim by the limiter's remaining slots, so a backlogged queue locks
            // and starts only as many workflows as the rate limit still allows this period.
            maxTasks = Math.min(maxTasks, Math.max(0, rateLimit.limit() - numRecentQueries));
          }

          if (maxTasks == 0) {
            // Nothing claimable. End the transaction here rather than leaving it open for the pool
            // to roll back, so the candidate SELECT's row locks are released at the return.
            connection.rollback();
            return List.of();
          }
        }

        // Global concurrency still requires a DB query — other workers may be running workflows
        // too.
        if (queue.concurrency() != null) {
          String globalPendingQuery =
              """
              SELECT COUNT(*)
              FROM "%s".workflow_status
              WHERE queue_name = ? AND status = ?
            """
                      .formatted(ctx.schema())
                  + ctx.andAppScope();
          if (partitionKey != null) {
            globalPendingQuery += " AND queue_partition_key = ?";
          }

          int globalPendingWorkflows = 0;
          try (PreparedStatement ps = connection.prepareStatement(globalPendingQuery)) {
            ps.setString(1, queue.name());
            ps.setString(2, WorkflowState.PENDING.name());
            var index = ctx.bindAppScope(ps, 3);
            if (partitionKey != null) {
              ps.setString(index, partitionKey);
            }

            try (ResultSet rs = ps.executeQuery()) {
              if (rs.next()) {
                globalPendingWorkflows = rs.getInt(1);
              }
            }
          }

          if (globalPendingWorkflows > queue.concurrency()) {
            logger.warn(
                "Total pending workflows ({}) on queue {} exceeds the global concurrency limit ({})",
                globalPendingWorkflows,
                queue.name(),
                queue.concurrency());
          }

          int availableTasks = Math.max(0, queue.concurrency() - globalPendingWorkflows);
          maxTasks = Math.min(maxTasks, availableTasks);
        }

        // Version-less workflows (application_version IS NULL) are only dequeued
        // when this worker is running the latest registered application version.
        boolean isLatestVersion = true;
        String latestVersionQuery =
            """
            SELECT version_name FROM "%s".application_versions
          """
                    .formatted(ctx.schema())
                + ctx.whereAppScope()
                + " ORDER BY version_timestamp DESC LIMIT 1";
        try (var ps = connection.prepareStatement(latestVersionQuery)) {
          ctx.bindAppScope(ps, 1);
          try (ResultSet rs = ps.executeQuery()) {
            if (rs.next()) {
              isLatestVersion = rs.getString(1).equals(appVersion);
            }
          }
        }

        String versionClause =
            isLatestVersion
                ? "(application_version = ? OR application_version IS NULL)"
                : "application_version = ?";

        var query =
            """
            SELECT workflow_uuid
            FROM "%s".workflow_status
            WHERE queue_name = ?
              AND status = ?
              AND %s
          """
                    .formatted(ctx.schema(), versionClause)
                + ctx.andAppScope();
        if (partitionKey != null) {
          query += " AND queue_partition_key = ?";
        }

        query += " ORDER BY priority ASC, created_at ASC";

        // Without a global budget, use SKIP LOCKED to only select rows that can be locked. With
        // one, use NOWAIT so all processes see a consistent table: a rate limit is a global budget
        // like concurrency, and SKIP LOCKED would hand a peer disjoint rows, letting it spend the
        // same budget against its own pre-claim snapshot.
        if (queue.concurrency() == null && queue.rateLimit() == null) {
          query += " FOR UPDATE SKIP LOCKED";
        } else {
          query += " FOR UPDATE NOWAIT";
        }

        if (maxTasks != Integer.MAX_VALUE) {
          query += " LIMIT %d".formatted(maxTasks);
        }

        List<String> dequeuedWorkflowIds = new ArrayList<>();
        try (var ps = connection.prepareStatement(query)) {
          ps.setString(1, queue.name());
          ps.setString(2, WorkflowState.ENQUEUED.name());
          ps.setString(3, appVersion);
          var index = ctx.bindAppScope(ps, 4);
          if (partitionKey != null) {
            ps.setString(index, partitionKey);
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

        String updateQuery =
            """
            UPDATE "%s".workflow_status
            SET status = ?,
                application_version = ?,
                executor_id = ?,
                started_at_epoch_ms = ?,
                updated_at = ?,
                rate_limited = ?,
                -- Count this dispatch against the dead-letter budget; no later write does it.
                recovery_attempts = recovery_attempts + 1,
                -- Claim it as it is taken, so the unclaimed backlog drains as workflows run.
                -- Left unclaimed, the row is PENDING and still visible to every peer's
                -- recovery and global-timeout sweeps until it starts executing here.
                application_name = COALESCE(application_name, ?),
                workflow_deadline_epoch_ms = CASE
                    WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                    THEN ? + workflow_timeout_ms
                    ELSE workflow_deadline_epoch_ms
                END
            WHERE workflow_uuid = ?
              AND status = ?
          """
                    .formatted(ctx.schema())
                // Re-check ownership alongside status: the candidate SELECT scoped the row, and the
                // claim must not widen that.
                + ctx.andAppScope();

        List<String> updatedWorkflowIds = new ArrayList<>();
        try (var ps = connection.prepareStatement(updateQuery)) {
          var now = System.currentTimeMillis();
          // No rate-limit cutoff here: the candidate SELECT above is already bounded by the
          // limiter's remaining slots.
          for (var id : dequeuedWorkflowIds) {
            ps.setString(1, WorkflowState.PENDING.name());
            ps.setString(2, appVersion);
            ps.setString(3, executorId);
            ps.setLong(4, now);
            ps.setLong(5, now);
            ps.setBoolean(6, queue.rateLimit() != null);
            ps.setString(7, ctx.appName());
            ps.setLong(8, now);
            ps.setString(9, id);
            ps.setString(10, WorkflowState.ENQUEUED.name());
            ctx.bindAppScope(ps, 11);
            if (ps.executeUpdate() > 0) {
              updatedWorkflowIds.add(id);
            }
          }
        }

        // Commit only if workflows were dequeued, matching Go. The candidate SELECT takes FOR
        // UPDATE row locks, which stamp xmax and consume an XID, so a round that claims nothing is
        // not a free read-only transaction: rolling it back avoids the WAL bloat and XID advance a
        // commit would cost.
        if (!updatedWorkflowIds.isEmpty()) {
          connection.commit();
        } else {
          connection.rollback();
        }

        return updatedWorkflowIds;
      } catch (Throwable t) {
        // No path may leave the transaction open: it may hold FOR UPDATE locks on rows other
        // executors are waiting to claim.
        try {
          connection.rollback();
        } catch (SQLException rollbackFailure) {
          t.addSuppressed(rollbackFailure);
        }
        throw t;
      }
    }
  }

  /**
   * Returns the given executors' PENDING workflows to their queues, and reports which rows moved.
   *
   * <p>This is the whole of recovery. A workflow whose executor is gone goes back to ENQUEUED -- on
   * the internal queue if it never had one of its own -- and whichever executor next polls that
   * queue runs it. Two things follow. The fleet shares the backlog, rather than one executor
   * working through all of it alone. And a repeat costs nothing: the queue's atomic ENQUEUED ->
   * PENDING claim admits exactly one runner, and once a live executor has taken a row, its
   * executor_id no longer matches the dead one this sweep names.
   *
   * <p>The internal queue has no concurrency limit, here as in every other SDK, so a workflow that
   * was never queued stays unthrottled by choice rather than by oversight.
   */
  public static List<String> reenqueueForRecovery(
      DbContext ctx, List<String> executorIds, String appVersion, String recoveryQueueName)
      throws SQLException {

    if (executorIds.isEmpty()) {
      return List.of();
    }

    final String sql =
        """
          UPDATE "%s".workflow_status
          SET status = ?,
              started_at_epoch_ms = NULL,
              updated_at = ?,
              queue_name = COALESCE(NULLIF(queue_name, ''), ?)
          WHERE status = ?
            AND executor_id = ANY(?)
            AND application_version = ?
        """
                .formatted(ctx.schema())
            + ctx.andAppScope()
            + " RETURNING workflow_uuid";

    try (Connection connection = ctx.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {
      Array executorIdArray = connection.createArrayOf("text", executorIds.toArray());
      stmt.setString(1, WorkflowState.ENQUEUED.name());
      stmt.setLong(2, System.currentTimeMillis());
      stmt.setString(3, recoveryQueueName);
      stmt.setString(4, WorkflowState.PENDING.name());
      stmt.setArray(5, executorIdArray);
      stmt.setString(6, appVersion);
      ctx.bindAppScope(stmt, 7);

      var workflowIds = new ArrayList<String>();
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          workflowIds.add(rs.getString("workflow_uuid"));
        }
      }
      return workflowIds;
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
                .formatted(ctx.schema())
            + ctx.andAppScope();

    try (Connection connection = ctx.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {
      stmt.setString(1, queueName);
      stmt.setString(2, WorkflowState.ENQUEUED.name());
      ctx.bindAppScope(stmt, 3);

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
   *
   * @param applicationName the application that owns the queue and polls it; null for this handle's
   *     own, which is a nameless handle's way of leaving the queue unclaimed
   */
  // Reads the stored partitioning surface directly; moves to the resolved limits in #507's
  // persistence and dequeue slices, which is where these call sites change.
  @SuppressWarnings("removal")
  public static boolean upsertQueue(
      DbContext ctx,
      String name,
      QueueOptions options,
      boolean updateExisting,
      @Nullable String applicationName)
      throws SQLException {
    refusePartitionLimits(options);
    Queue queue = queueFromOptions(name, options);
    var requestedOwner = applicationName != null ? applicationName : ctx.appName();
    final String insertSql =
        """
        INSERT INTO "%s".queues
          (name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec,
            priority_enabled, partition_queue, polling_interval_sec, updated_at, application_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
          updated_at            = ?,
          -- Claim only an unclaimed row, so a registration landing between the ownership
          -- check above and this write keeps the name it just took.
          application_name      = COALESCE(application_name, ?)
        WHERE name = ?
        """
            .formatted(ctx.schema());

    try (Connection connection = ctx.getConnection()) {
      // Read the current owner first: the writes below are silent about why they declined to claim.
      var owner =
          RowOwner.resolve(
              connection, ctx.schema(), "queues", "name", queue.name(), requestedOwner, "Queue");
      boolean inserted;
      try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
        var index = bindQueueParams(ps, queue, 1);
        ps.setString(index, owner);
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
          ps.setString(9, owner);
          ps.setString(10, queue.name());
          ps.executeUpdate();
        }
      }
      return inserted;
    }
  }

  /**
   * Refuses options carrying a per-partition limit, which this slice cannot yet persist.
   *
   * <p>Deleted by the persistence slice of #507, which writes the columns. Until then the surface
   * exists and the storage does not, and failing loudly beats writing a queue that silently has no
   * limit -- the symptom of which arrives much later, as a partition key rejected by a queue the
   * caller believes is partitioned.
   */
  private static void refusePartitionLimits(QueueOptions options) {
    if (options.partitionConcurrency().isPresent()
        || options.partitionWorkerConcurrency().isPresent()
        || options.partitionRateLimitMax().isPresent()
        || options.partitionRateLimitPeriod().isPresent()) {
      throw new UnsupportedOperationException(
          "Per-partition queue limits are not persisted yet; see dbos-transact-java#507");
    }
  }

  /** Binds a queue row's columns from {@code offset}, returning the next free index. */
  // Reads the stored partitioning surface directly; moves to the resolved limits in #507's
  // persistence and dequeue slices, which is where these call sites change.
  @SuppressWarnings("removal")
  private static int bindQueueParams(PreparedStatement ps, Queue queue, int offset)
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
    return offset + 9;
  }

  public static Optional<Queue> findQueue(DbContext ctx, String name) throws SQLException {
    final String sql =
        """
        SELECT name, concurrency, worker_concurrency,
          rate_limit_max, rate_limit_period_sec,
          priority_enabled, partition_queue, polling_interval_sec, application_name
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
    return listQueues(ctx, null);
  }

  /**
   * Lists queues owned by {@code applicationName}, plus unclaimed ones. Null lists this
   * application's own; an explicitly empty list covers every application's.
   */
  public static List<Queue> listQueues(DbContext ctx, @Nullable List<String> applicationName)
      throws SQLException {
    var names = ctx.scopeNames(applicationName);
    var scope =
        names == null ? "" : " WHERE (application_name = ANY(?) OR application_name IS NULL)";
    final String sql =
        """
        SELECT name, concurrency, worker_concurrency,
          rate_limit_max, rate_limit_period_sec,
          priority_enabled, partition_queue, polling_interval_sec, application_name
        FROM "%s".queues
        """
                .formatted(ctx.schema())
            + scope
            + " ORDER BY name";

    try (Connection connection = ctx.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {
      Array namesArray = null;
      if (names != null) {
        namesArray = connection.createArrayOf("text", names.toArray());
        stmt.setArray(1, namesArray);
      }
      try (ResultSet rs = stmt.executeQuery()) {
        List<Queue> queues = new ArrayList<>();
        while (rs.next()) {
          queues.add(queueFromResultSet(rs));
        }
        return queues;
      } finally {
        if (namesArray != null) {
          namesArray.free();
        }
      }
    }
  }

  // Reads the stored partitioning surface directly; moves to the resolved limits in #507's
  // persistence and dequeue slices, which is where these call sites change.
  @SuppressWarnings("removal")
  public static void updateQueue(DbContext ctx, String name, QueueOptions update)
      throws SQLException {
    refusePartitionLimits(update);
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

  // Reads the stored partitioning surface directly; moves to the resolved limits in #507's
  // persistence and dequeue slices, which is where these call sites change.
  @SuppressWarnings("removal")
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
        pollingInterval,
        rs.getString("application_name"));
  }

  // Reads the stored partitioning surface directly; moves to the resolved limits in #507's
  // persistence and dequeue slices, which is where these call sites change.
  @SuppressWarnings("removal")
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
        pollingIntervalVal,
        null); // the owner is resolved and written separately by upsertQueue
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
