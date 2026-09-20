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
      long localRunningCount,
      long partitionLocalRunningCount)
      throws SQLException {

    if (partitionKey != null && partitionKey.isEmpty()) {
      partitionKey = null;
    }
    final String partition = partitionKey;

    // Read every limit at the scope it is enforced at, not off the queue's raw columns: a legacy
    // partitioned queue records its limits queue-wide but enforces them per partition.
    var limits = queue.resolveLimits();

    // A queue-wide concurrency limit or limiter is a budget shared with every other executor.
    boolean queueWideBudget = limits.concurrency() != null || limits.rateLimit() != null;
    // Spending a queue-wide budget while sweeping one partition at a time is a write skew: each
    // partition's sweep reads the same budget and claims against its own snapshot, so the
    // partitions together can overshoot it. Only serializability rules that out.
    boolean crossPartitionBudget = queueWideBudget && partition != null;
    boolean sharedBudget =
        queueWideBudget
            || limits.partitionConcurrency() != null
            || limits.partitionRateLimit() != null;

    try (Connection connection = ctx.getConnection()) {
      connection.setAutoCommit(false);
      // Worker-scoped concurrency is tracked in memory and needs no consistent DB snapshot, so a
      // dequeue bounded only by that stays at READ COMMITTED.
      if (crossPartitionBudget) {
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      } else if (sharedBudget) {
        connection.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
      }

      try {
        long maxTasks = Integer.MAX_VALUE;

        // Both worker-scoped limits use the caller-supplied in-memory counts — no DB round trip.
        if (limits.workerConcurrency() != null) {
          if (localRunningCount > limits.workerConcurrency()) {
            logger.warn(
                "Local running workflows ({}) on queue {} exceeds the worker concurrency limit ({})",
                localRunningCount,
                queue.name(),
                limits.workerConcurrency());
          }
          maxTasks =
              Math.min(maxTasks, Math.max(0, limits.workerConcurrency() - localRunningCount));
        }
        if (limits.partitionWorkerConcurrency() != null) {
          maxTasks =
              Math.min(
                  maxTasks,
                  Math.max(0, limits.partitionWorkerConcurrency() - partitionLocalRunningCount));
        }
        if (maxTasks == 0) {
          // Nothing claimable. End the transaction here rather than leaving it open for
          // the pool to roll back on return.
          connection.rollback();
          return List.of();
        }

        // Bound the claim by each limiter's remaining slots, so a backlogged queue locks and
        // starts only as many workflows as the rate limit still allows this period.
        if (limits.rateLimit() != null) {
          maxTasks =
              Math.min(
                  maxTasks,
                  rateLimitRemaining(ctx, connection, queue.name(), limits.rateLimit(), null));
        }
        if (limits.partitionRateLimit() != null) {
          maxTasks =
              Math.min(
                  maxTasks,
                  rateLimitRemaining(
                      ctx, connection, queue.name(), limits.partitionRateLimit(), partition));
        }
        if (maxTasks == 0) {
          // Nothing claimable. End the transaction here rather than leaving it open for the pool
          // to roll back, so the candidate SELECT's row locks are released at the return.
          connection.rollback();
          return List.of();
        }

        // Concurrency at either shared scope still requires a DB query — other workers may be
        // running workflows too.
        if (limits.concurrency() != null) {
          int pending = pendingCount(ctx, connection, queue.name(), null);
          if (pending > limits.concurrency()) {
            logger.warn(
                "Total pending workflows ({}) on queue {} exceeds the global concurrency limit ({})",
                pending,
                queue.name(),
                limits.concurrency());
          }
          maxTasks = Math.min(maxTasks, Math.max(0, limits.concurrency() - pending));
        }
        if (limits.partitionConcurrency() != null) {
          int pending = pendingCount(ctx, connection, queue.name(), partition);
          if (pending > limits.partitionConcurrency()) {
            logger.warn(
                "Total pending workflows ({}) on partition {} of queue {} exceeds the partition"
                    + " concurrency limit ({})",
                pending,
                partition,
                queue.name(),
                limits.partitionConcurrency());
          }
          maxTasks = Math.min(maxTasks, Math.max(0, limits.partitionConcurrency() - pending));
        }
        if (maxTasks == 0) {
          connection.rollback();
          return List.of();
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
        if (partition != null) {
          query += " AND queue_partition_key = ?";
        }

        query += " ORDER BY priority ASC, created_at ASC";

        // Without a shared budget, use SKIP LOCKED to only select rows that can be locked. With
        // one, use NOWAIT so all processes see a consistent table: a rate limit is a shared budget
        // like concurrency, and SKIP LOCKED would hand a peer disjoint rows, letting it spend the
        // same budget against its own pre-claim snapshot.
        if (!sharedBudget) {
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
          if (partition != null) {
            ps.setString(index, partition);
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
            // Whichever scope the limiter is at: rateLimitRemaining counts only rows marked
            // here, so a queue limited solely per partition would otherwise count none of its
            // own claims and hand back the full limit every poll.
            ps.setBoolean(6, limits.rateLimit() != null || limits.partitionRateLimit() != null);
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
  public static boolean upsertQueue(
      DbContext ctx,
      String name,
      QueueOptions options,
      boolean updateExisting,
      @Nullable String applicationName)
      throws SQLException {
    Queue queue = queueFromOptions(name, options);
    // The rules the constructor cannot apply, because it is also the read path.
    queue.validateForRegistration();
    var requestedOwner = applicationName != null ? applicationName : ctx.appName();
    final String insertSql =
        """
        INSERT INTO "%s".queues
          (name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec,
            partition_concurrency, partition_worker_concurrency,
            partition_rate_limit_max, partition_rate_limit_period_sec,
            priority_enabled, partition_queue, polling_interval_sec, updated_at, application_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (name) DO NOTHING
        """
            .formatted(ctx.schema());
    final String updateSql =
        """
        UPDATE "%s".queues SET
          concurrency                     = ?,
          worker_concurrency              = ?,
          rate_limit_max                  = ?,
          rate_limit_period_sec           = ?,
          partition_concurrency           = ?,
          partition_worker_concurrency    = ?,
          partition_rate_limit_max        = ?,
          partition_rate_limit_period_sec = ?,
          priority_enabled                = ?,
          partition_queue                 = ?,
          polling_interval_sec            = ?,
          updated_at                      = ?,
          -- Claim only an unclaimed row, so a registration landing between the ownership
          -- check above and this write keeps the name it just took.
          application_name                = COALESCE(application_name, ?)
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
          setRateLimit(ps, 3, queue.rateLimit());
          setNullableInt(ps, 5, queue.partitionConcurrency());
          setNullableInt(ps, 6, queue.partitionWorkerConcurrency());
          setRateLimit(ps, 7, queue.partitionRateLimit());
          ps.setBoolean(9, queue.priorityEnabled());
          // Derived, not copied: the column follows the per-partition limits, and other SDKs
          // read it to decide whether to dequeue per partition.
          ps.setBoolean(10, queue.isPartitioned());
          ps.setDouble(11, queue.pollingInterval().toMillis() / 1000.0);
          ps.setLong(12, System.currentTimeMillis());
          ps.setString(13, owner);
          ps.setString(14, queue.name());
          ps.executeUpdate();
        }
      }
      return inserted;
    }
  }

  private static int rateLimitRemaining(
      DbContext ctx,
      Connection connection,
      String queueName,
      Queue.RateLimit rateLimit,
      @Nullable String partitionKey)
      throws SQLException {
    var sql =
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
      sql += " AND queue_partition_key = ?";
    }

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      ps.setString(1, queueName);
      ps.setString(2, WorkflowState.ENQUEUED.name());
      ps.setString(3, WorkflowState.DELAYED.name());
      ps.setLong(4, Instant.now().minus(rateLimit.period()).toEpochMilli());
      var index = ctx.bindAppScope(ps, 5);
      if (partitionKey != null) {
        ps.setString(index, partitionKey);
      }

      int recent = 0;
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          recent = rs.getInt(1);
        }
      }
      return Math.max(0, rateLimit.limit() - recent);
    }
  }

  private static int pendingCount(
      DbContext ctx, Connection connection, String queueName, @Nullable String partitionKey)
      throws SQLException {
    var sql =
        """
        SELECT COUNT(*)
        FROM "%s".workflow_status
        WHERE queue_name = ? AND status = ?
      """
                .formatted(ctx.schema())
            + ctx.andAppScope();
    if (partitionKey != null) {
      sql += " AND queue_partition_key = ?";
    }

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      ps.setString(1, queueName);
      ps.setString(2, WorkflowState.PENDING.name());
      var index = ctx.bindAppScope(ps, 3);
      if (partitionKey != null) {
        ps.setString(index, partitionKey);
      }

      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() ? rs.getInt(1) : 0;
      }
    }
  }

  /** Binds a queue row's columns from {@code offset}, returning the next free index. */
  private static int bindQueueParams(PreparedStatement ps, Queue queue, int offset)
      throws SQLException {
    ps.setString(offset, queue.name());
    setNullableInt(ps, offset + 1, queue.concurrency());
    setNullableInt(ps, offset + 2, queue.workerConcurrency());
    setRateLimit(ps, offset + 3, queue.rateLimit());
    setNullableInt(ps, offset + 5, queue.partitionConcurrency());
    setNullableInt(ps, offset + 6, queue.partitionWorkerConcurrency());
    setRateLimit(ps, offset + 7, queue.partitionRateLimit());
    ps.setBoolean(offset + 9, queue.priorityEnabled());
    // Derived, not copied: the column follows the per-partition limits, and other SDKs read it
    // to decide whether to dequeue per partition.
    ps.setBoolean(offset + 10, queue.isPartitioned());
    ps.setDouble(offset + 11, queue.pollingInterval().toMillis() / 1000.0);
    ps.setLong(offset + 12, System.currentTimeMillis());
    return offset + 13;
  }

  public static Optional<Queue> findQueue(DbContext ctx, String name) throws SQLException {
    try (Connection connection = ctx.getConnection()) {
      return findQueue(connection, ctx.schema(), name, false);
    }
  }

  /**
   * Reads one queue on a caller-supplied connection.
   *
   * @param forUpdate locks the row for the rest of the caller's transaction, so a read-modify-write
   *     cannot lose a concurrent registration
   */
  private static Optional<Queue> findQueue(
      Connection connection, String schema, String name, boolean forUpdate) throws SQLException {
    final String sql =
        """
        SELECT name, concurrency, worker_concurrency,
          rate_limit_max, rate_limit_period_sec,
          partition_concurrency, partition_worker_concurrency,
          partition_rate_limit_max, partition_rate_limit_period_sec,
          priority_enabled, partition_queue, polling_interval_sec, application_name
        FROM "%s".queues
        WHERE name = ?
        """
                .formatted(schema)
            + (forUpdate ? " FOR UPDATE" : "");

    try (PreparedStatement stmt = connection.prepareStatement(sql)) {
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
          partition_concurrency, partition_worker_concurrency,
          partition_rate_limit_max, partition_rate_limit_period_sec,
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

  public static void updateQueue(DbContext ctx, String name, QueueOptions update)
      throws SQLException {
    if (update.isEmpty()) return;

    // Read, apply, validate, write -- all on one connection in one transaction. Validating the
    // row an update would produce, rather than the update on its own, is the only way to check a
    // rule that spans fields the caller did not all supply. Sharing the transaction is what makes
    // the check mean anything: read and write on separate connections would let a concurrent
    // registration land in between, and this write would then silently discard it.
    try (Connection connection = ctx.getConnection()) {
      connection.setAutoCommit(false);
      boolean committed = false;
      try {
        var current = findQueue(connection, ctx.schema(), name, true);
        // No row to update: the statement below would match nothing anyway.
        if (current.isEmpty()) {
          connection.rollback();
          return;
        }
        requireNotLegacyPartitioned(current.get(), update);
        var updated = applyUpdate(current.get(), update);
        updated.validateForRegistration();

        List<String> setClauses = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        collectField(setClauses, params, "concurrency", update.concurrency());
        collectField(setClauses, params, "worker_concurrency", update.workerConcurrency());
        collectField(setClauses, params, "rate_limit_max", update.rateLimitMax());
        collectField(
            setClauses, params, "rate_limit_period_sec", durationToSec(update.rateLimitPeriod()));
        collectField(setClauses, params, "partition_concurrency", update.partitionConcurrency());
        collectField(
            setClauses,
            params,
            "partition_worker_concurrency",
            update.partitionWorkerConcurrency());
        collectField(
            setClauses, params, "partition_rate_limit_max", update.partitionRateLimitMax());
        collectField(
            setClauses,
            params,
            "partition_rate_limit_period_sec",
            durationToSec(update.partitionRateLimitPeriod()));
        collectOptional(setClauses, params, "priority_enabled", update.priorityEnabled());
        // Partitioning is inferred from the per-partition limits, so the stored flag follows them
        // on every write. Left alone it would go stale in both directions: unset on a queue that
        // just gained its first partition limit, and still set on one that just lost its last.
        setClauses.add("\"partition_queue\" = ?");
        params.add(updated.isPartitioned());
        collectOptional(
            setClauses, params, "polling_interval_sec", durationToSec(update.pollingInterval()));

        setClauses.add("\"updated_at\" = ?");
        params.add(System.currentTimeMillis());
        params.add(name);

        String sql =
            "UPDATE \"%s\".queues SET %s WHERE name = ?"
                .formatted(ctx.schema(), String.join(", ", setClauses));

        try (PreparedStatement ps = connection.prepareStatement(sql)) {
          for (int i = 0; i < params.size(); i++) {
            ps.setObject(i + 1, params.get(i));
          }
          ps.executeUpdate();
        }
        connection.commit();
        committed = true;

        // Partitioning an existing queue strands whatever is already on it: those rows were
        // enqueued without a partition key -- enqueue refused one while the queue was
        // unpartitioned -- and a partitioned queue only dequeues from its partitions, which
        // getQueuePartitions reads from the keys present. Go warns at the same transition.
        if (!current.get().isPartitioned() && updated.isPartitioned()) {
          logger.warn(
              "Queue {} is now partitioned by its per-partition limits. Workflows already"
                  + " enqueued on it have no partition key and will never be dequeued; drain the"
                  + " queue before partitioning it, or re-enqueue them with a partition key.",
              name);
        }
      } finally {
        if (!committed) {
          connection.rollback();
        }
      }
    }
  }

  /**
   * Refuses to carry a queue between the two partitioning modes by an update to its limits.
   *
   * <p>Under the deprecated {@code partitionQueue} flag the queue-wide limits are enforced per
   * partition, so the two modes disagree about what {@code concurrency} means. A legacy queue that
   * gained its first per-partition limit would silently rescope every limit the update did not
   * mention, and one that then lost it again would come back unpartitioned, after which every
   * enqueue carrying a partition key fails. Re-registration is the supported way across, which is
   * what the message points at; this is Go's {@code requireNotLegacyPartitioned}.
   */
  @SuppressWarnings("removal") // reads the legacy partitionQueue option
  private static void requireNotLegacyPartitioned(Queue current, QueueOptions update) {
    if (current.isLegacyPartitioned()) {
      var field = firstLimitSet(update);
      if (field != null) {
        throw new IllegalArgumentException(
            ("cannot set %s on queue %s: it is registered with the deprecated partitionQueue"
                    + " option, under which concurrency, workerConcurrency and rateLimit apply per"
                    + " partition; re-register the queue with the partition limits instead")
                .formatted(field, current.name()));
      }
    } else if (current.hasPartitionLimits() && update.partitionQueue().isPresent()) {
      throw new IllegalArgumentException(
          ("cannot set partitionQueue on queue %s: it is partitioned by its partition limits;"
                  + " clear those instead")
              .formatted(current.name()));
    }
  }

  /** The first limit this update sets, named as the caller named it, or null if it sets none. */
  private static @Nullable String firstLimitSet(QueueOptions update) {
    if (update.concurrency().isPresent()) return "concurrency";
    if (update.workerConcurrency().isPresent()) return "workerConcurrency";
    if (update.rateLimitMax().isPresent() || update.rateLimitPeriod().isPresent()) {
      return "rateLimit";
    }
    if (update.partitionConcurrency().isPresent()) return "partitionConcurrency";
    if (update.partitionWorkerConcurrency().isPresent()) return "partitionWorkerConcurrency";
    if (update.partitionRateLimitMax().isPresent() || update.partitionRateLimitPeriod().isPresent())
      return "partitionRateLimit";
    return null;
  }

  /**
   * The queue a partial update would produce, built through the {@link Queue} constructor so it is
   * checked by the same rules that reading the row back would apply.
   */
  @SuppressWarnings("removal") // reads the legacy partitionQueue option verbatim
  private static Queue applyUpdate(Queue current, QueueOptions update) {
    var updated =
        new Queue(
            current.name(),
            intAfter(current.concurrency(), update.concurrency()),
            intAfter(current.workerConcurrency(), update.workerConcurrency()),
            update.priorityEnabled().orElse(current.priorityEnabled()),
            // Carry the legacy flag's meaning, not the stored column: the column is derived, so
            // on a queue partitioned by its limits it is already true and would read back as a
            // request for legacy partitioning that the caller never made.
            update.partitionQueue().orElse(current.isLegacyPartitioned()),
            rateLimitAfter(
                current.name(),
                "rateLimit",
                current.rateLimit(),
                update.rateLimitMax(),
                update.rateLimitPeriod()),
            intAfter(current.partitionConcurrency(), update.partitionConcurrency()),
            intAfter(current.partitionWorkerConcurrency(), update.partitionWorkerConcurrency()),
            rateLimitAfter(
                current.name(),
                "partitionRateLimit",
                current.partitionRateLimit(),
                update.partitionRateLimitMax(),
                update.partitionRateLimitPeriod()),
            update.pollingInterval().orElse(current.pollingInterval()),
            current.applicationName());
    return updated;
  }

  /** The value a partial update leaves in a nullable integer column. */
  private static @Nullable Integer intAfter(@Nullable Integer current, Field<Integer> update) {
    return update.isPresent() ? update.get() : current;
  }

  /**
   * The rate limit a partial update leaves, where either half may be set, cleared or untouched.
   *
   * <p>A result with exactly one half set is refused rather than read as no limit. The UPDATE
   * writes only the columns the caller supplied, so collapsing such a result to null would validate
   * a queue with no limit while storing one half of one: {@code rateLimitMax = 0} alone passes as
   * null here and lands in the row, and a later update supplying only the period then completes a
   * live limit that neither update ever validated as a pair. For the partition limits the derived
   * {@code partition_queue} flag would go out with it, computed from a limit this saw as absent.
   */
  private static Queue.@Nullable RateLimit rateLimitAfter(
      String queue,
      String name,
      Queue.@Nullable RateLimit current,
      Field<Integer> max,
      Field<Duration> period) {
    Integer newMax = max.isPresent() ? max.get() : (current != null ? current.limit() : null);
    Duration newPeriod =
        period.isPresent() ? period.get() : (current != null ? current.period() : null);
    if (newMax == null && newPeriod == null) return null;
    if (newMax == null || newPeriod == null) {
      throw new IllegalArgumentException(
          ("cannot leave queue %s with half of %s: %sMax and %sPeriod are set and cleared"
                  + " together")
              .formatted(queue, name, name, name));
    }
    return new Queue.RateLimit(newMax, newPeriod);
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

  /** A rate limit from its two columns, or null when either is unset. */
  private static Queue.@Nullable RateLimit rateLimitFromResultSet(
      ResultSet rs, String maxColumn, String periodColumn) throws SQLException {
    Integer max = rs.getObject(maxColumn, Integer.class);
    Double periodSec = rs.getObject(periodColumn, Double.class);
    if (max == null || periodSec == null) return null;
    return new Queue.RateLimit(max, Duration.ofMillis((long) (periodSec * 1000)));
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
        rs.getObject("partition_concurrency", Integer.class),
        rs.getObject("partition_worker_concurrency", Integer.class),
        rateLimitFromResultSet(rs, "partition_rate_limit_max", "partition_rate_limit_period_sec"),
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

    Queue.RateLimit partitionRateLimit = null;
    if (options.partitionRateLimitMax().isPresent()
        && options.partitionRateLimitPeriod().isPresent()
        && options.partitionRateLimitMax().get() != null
        && options.partitionRateLimitPeriod().get() != null) {
      partitionRateLimit =
          new Queue.RateLimit(
              options.partitionRateLimitMax().get(), options.partitionRateLimitPeriod().get());
    }

    Duration pollingIntervalVal = options.pollingInterval().orElse(Queue.DEFAULT_POLLING_INTERVAL);

    return new Queue(
        name,
        concurrencyVal,
        workerConcurrencyVal,
        priorityEnabledVal,
        partitionQueueVal,
        rateLimit,
        options.partitionConcurrency().isPresent() ? options.partitionConcurrency().get() : null,
        options.partitionWorkerConcurrency().isPresent()
            ? options.partitionWorkerConcurrency().get()
            : null,
        partitionRateLimit,
        pollingIntervalVal,
        null); // the owner is resolved and written separately by upsertQueue
  }

  /** Binds a rate limit's two columns, or two nulls, at {@code index} and {@code index + 1}. */
  private static void setRateLimit(
      PreparedStatement stmt, int index, Queue.@Nullable RateLimit rateLimit) throws SQLException {
    if (rateLimit != null) {
      stmt.setInt(index, rateLimit.limit());
      stmt.setDouble(index + 1, rateLimit.period().toMillis() / 1000.0);
    } else {
      stmt.setNull(index, java.sql.Types.INTEGER);
      stmt.setNull(index + 1, java.sql.Types.DOUBLE);
    }
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
