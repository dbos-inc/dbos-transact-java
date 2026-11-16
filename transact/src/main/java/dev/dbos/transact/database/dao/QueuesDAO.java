package dev.dbos.transact.database.dao;

import dev.dbos.transact.database.AdvisoryLocks;
import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.database.DequeueResult;
import dev.dbos.transact.database.DequeueResult.StopReason;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.internal.DebugTriggers;
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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueuesDAO {

  private QueuesDAO() {}

  private static final Logger logger = LoggerFactory.getLogger(QueuesDAO.class);

  public static DequeueResult startQueuedWorkflows(
      DbContext ctx,
      Queue queue,
      String executorId,
      String appVersion,
      String partitionKey,
      long localRunningCount,
      long partitionLocalRunningCount,
      String ownerXid)
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
    // partition's claim reads the same budget, so the claim lock must span the whole queue.
    boolean crossPartitionBudget = queueWideBudget && partition != null;
    boolean sharedBudget =
        queueWideBudget
            || limits.partitionConcurrency() != null
            || limits.partitionRateLimit() != null;

    long maxTasks = Integer.MAX_VALUE;

    // This worker's own limits use the caller-supplied in-memory counts and need no database, so a
    // full worker returns before taking a claim lock a peer could use.
    if (limits.workerConcurrency() != null) {
      // A limit of 0 or less is a paused queue, not one running over its limit.
      if (limits.workerConcurrency() > 0 && localRunningCount > limits.workerConcurrency()) {
        logger.warn(
            "Local running workflows ({}) on queue {} exceeds the worker concurrency limit ({})",
            localRunningCount,
            queue.name(),
            limits.workerConcurrency());
      }
      maxTasks = Math.min(maxTasks, Math.max(0, limits.workerConcurrency() - localRunningCount));
      if (maxTasks == 0) {
        return DequeueResult.stopped(StopReason.WORKER_CONCURRENCY);
      }
    }
    if (limits.partitionWorkerConcurrency() != null) {
      maxTasks =
          Math.min(
              maxTasks,
              Math.max(0, limits.partitionWorkerConcurrency() - partitionLocalRunningCount));
      if (maxTasks == 0) {
        return DequeueResult.stopped(StopReason.PARTITION_WORKER_CONCURRENCY);
      }
    }

    // Workers that pick the same head collide on its row lock, but workers that pick different
    // heads -- different versions mid-deploy, or a higher-priority row enqueued between their reads
    // -- lock different rows, and no isolation short of SERIALIZABLE stops both claiming. So a
    // claim that spends a shared budget first takes an advisory lock on that budget's scope.
    // CockroachDB has no advisory locks to rely on, so there the claim runs at SERIALIZABLE.
    boolean useClaimLock = sharedBudget && !ctx.isCockroach();
    try (Connection connection = ctx.getConnection()) {
      connection.setAutoCommit(false);
      if (useClaimLock) {
        // The lock keeps other claimers out, so each statement need only see what the previous
        // holder committed, which a snapshot taken before the lock was granted would not.
        connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
      } else if (sharedBudget) {
        // CockroachDB, with no lock: SERIALIZABLE aborts one of two claims that would overspend.
        connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
      }
      // Worker-scoped concurrency is tracked in memory and needs no consistent DB snapshot, so a
      // dequeue bounded only by that keeps the pool's level.

      try {
        if (useClaimLock
            && !tryClaimLock(
                connection,
                AdvisoryLocks.queueClaimKey(
                    ctx.schema(), queue.name(), crossPartitionBudget ? null : partition))) {
          // Another worker is claiming from this scope, so it still has work; the next poll
          // retries.
          connection.rollback();
          return DequeueResult.stopped(StopReason.CLAIM_LOCK);
        }

        // Bound the claim by each limiter's remaining slots, so a backlogged queue locks and
        // starts only as many workflows as the rate limit still allows this period.
        if (limits.rateLimit() != null) {
          maxTasks =
              Math.min(
                  maxTasks,
                  rateLimitRemaining(ctx, connection, queue.name(), limits.rateLimit(), null));
          if (maxTasks == 0) {
            connection.rollback();
            return DequeueResult.stopped(StopReason.RATE_LIMIT);
          }
        }
        if (limits.partitionRateLimit() != null) {
          maxTasks =
              Math.min(
                  maxTasks,
                  rateLimitRemaining(
                      ctx, connection, queue.name(), limits.partitionRateLimit(), partition));
          if (maxTasks == 0) {
            connection.rollback();
            return DequeueResult.stopped(StopReason.PARTITION_RATE_LIMIT);
          }
        }

        // Concurrency at either shared scope still requires a DB query — other workers may be
        // running workflows too.
        if (limits.concurrency() != null) {
          int pending = pendingCount(ctx, connection, queue.name(), null);
          if (limits.concurrency() > 0 && pending > limits.concurrency()) {
            logger.warn(
                "Total pending workflows ({}) on queue {} exceeds the global concurrency limit ({})",
                pending,
                queue.name(),
                limits.concurrency());
          }
          // In long: a stored limit near Integer.MIN_VALUE less the pending count would wrap.
          maxTasks = Math.min(maxTasks, Math.max(0, (long) limits.concurrency() - pending));
          if (maxTasks == 0) {
            connection.rollback();
            return DequeueResult.stopped(StopReason.CONCURRENCY);
          }
        }
        if (limits.partitionConcurrency() != null) {
          int pending = pendingCount(ctx, connection, queue.name(), partition);
          if (limits.partitionConcurrency() > 0 && pending > limits.partitionConcurrency()) {
            logger.warn(
                "Total pending workflows ({}) on partition {} of queue {} exceeds the partition"
                    + " concurrency limit ({})",
                pending,
                partition,
                queue.name(),
                limits.partitionConcurrency());
          }
          maxTasks =
              Math.min(maxTasks, Math.max(0, (long) limits.partitionConcurrency() - pending));
          if (maxTasks == 0) {
            connection.rollback();
            return DequeueResult.stopped(StopReason.PARTITION_CONCURRENCY);
          }
        }

        String versionClause = versionClause(ctx, connection, appVersion);

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
        DebugTriggers.debugTriggerPoint(DebugTriggers.DEBUG_TRIGGER_FIND_AND_MARK_AFTER_SELECT);

        if (!dequeuedWorkflowIds.isEmpty()) {
          logger.debug(
              "attempting to dequeue {} task(s) from {} queue",
              dequeuedWorkflowIds.size(),
              queue.name());
        }

        String updateQuery =
            """
            UPDATE "%1$s".workflow_status
            SET status = ?,
                application_version = ?,
                executor_id = ?,
                -- The claim's token: the execution it dispatches owns the workflow with it.
                owner_xid = ?,
                started_at_epoch_ms = %2$s,
                updated_at = %2$s,
                rate_limited = ?,
                -- Count this dispatch against the dead-letter budget; no later write does it.
                recovery_attempts = recovery_attempts + 1,
                -- Claim it as it is taken, so the unclaimed backlog drains as workflows run.
                -- Left unclaimed, the row is PENDING and still visible to every peer's
                -- recovery and global-timeout sweeps until it starts executing here.
                application_name = COALESCE(application_name, ?),
                -- The timeout runs from the claim, on the database's clock like every deadline.
                workflow_deadline_epoch_ms = CASE
                    WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                    THEN %3$s
                    ELSE workflow_deadline_epoch_ms
                END
            WHERE workflow_uuid = ?
              AND status = ?
          """
                    .formatted(
                        ctx.schema(),
                        SystemDatabase.NOW_EPOCH_MS,
                        SystemDatabase.deadlineEpochMs("workflow_timeout_ms"))
                // Re-check ownership alongside status: the candidate SELECT scoped the row, and the
                // claim must not widen that.
                + ctx.andAppScope();

        List<String> updatedWorkflowIds = new ArrayList<>();
        try (var ps = connection.prepareStatement(updateQuery)) {
          // No rate-limit cutoff here: the candidate SELECT above is already bounded by the
          // limiter's remaining slots.
          for (var id : dequeuedWorkflowIds) {
            ps.setString(1, WorkflowState.PENDING.name());
            ps.setString(2, appVersion);
            ps.setString(3, executorId);
            ps.setString(4, ownerXid);
            // Whichever scope the limiter is at: rateLimitRemaining counts only rows marked
            // here, so a queue limited solely per partition would otherwise count none of its
            // own claims and hand back the full limit every poll.
            ps.setBoolean(5, limits.rateLimit() != null || limits.partitionRateLimit() != null);
            ps.setString(6, ctx.appName());
            ps.setString(7, id);
            ps.setString(8, WorkflowState.ENQUEUED.name());
            ctx.bindAppScope(ps, 9);
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

        return new DequeueResult(updatedWorkflowIds, null);
      } catch (Throwable t) {
        // No path may leave the transaction open: it may hold FOR UPDATE locks, or the claim lock,
        // that other executors are waiting to claim under.
        try {
          connection.rollback();
        } catch (SQLException rollbackFailure) {
          t.addSuppressed(rollbackFailure);
        }
        throw t;
      }
    }
  }

  /** The most partition heads one batched sweep claims, before this worker's own budget. */
  public static final int PARTITIONED_DEQUEUE_SWEEP_CAP = 8192;

  /**
   * The most partitions a batched sweep claims in one transaction, which bounds how many locks it
   * holds at once, and for how long.
   */
  public static final int PARTITIONED_DEQUEUE_BATCH_SIZE = 64;

  /**
   * Claims the head workflow of every partition that has none running.
   *
   * <p>The per-partition sweep costs a transaction per partition per poll. This walks the distinct
   * partition keys with a recursive CTE -- a loose index scan, one seek per key -- so the cost
   * grows with the number of partitions, not with the depth of their backlog.
   *
   * <p>The walk takes no lock. The partitions it finds are then claimed {@code batchSize} to a
   * transaction. Each batch first takes an advisory lock on each of its partitions, leaving any
   * that another sweep holds to that sweep or the next poll, and then picks the heads of the rest
   * again under those locks. Without them, two sweeps that pick different heads of one idle
   * partition -- different versions mid-deploy, or a higher-priority row enqueued between their
   * reads -- would lock different rows and could both claim. On CockroachDB, which has no advisory
   * locks to rely on, each batch runs at SERIALIZABLE instead.
   *
   * @param maxTasks this worker's remaining budget, which bounds the claim alongside {@code
   *     sweepCap}
   * @param sweepCap the most heads one sweep claims; {@link #PARTITIONED_DEQUEUE_SWEEP_CAP} outside
   *     tests
   * @param batchSize the most partitions one transaction claims; {@link
   *     #PARTITIONED_DEQUEUE_BATCH_SIZE} outside tests
   * @param ownerXid the token the claim installs on every workflow it takes, which the executions
   *     it dispatches own them with
   * @return the claimed workflow IDs, ordered by partition key
   * @throws IllegalArgumentException if {@link Queue#canBatchPartitionedDequeue} rejects the queue
   */
  public static List<String> startQueuedPartitionedWorkflows(
      DbContext ctx,
      Queue queue,
      String executorId,
      String appVersion,
      long maxTasks,
      int sweepCap,
      int batchSize,
      String ownerXid)
      throws SQLException {
    if (!queue.canBatchPartitionedDequeue()) {
      throw new IllegalArgumentException(
          "Batched partitioned dequeue requires a partitioned queue with partition concurrency 1"
              + " and no queue-wide concurrency or rate limit: "
              + queue.name());
    }
    long sweepLimit = Math.min(sweepCap, maxTasks);
    if (sweepLimit <= 0) {
      return List.of();
    }
    // When this worker's budget is the binding limit, probe partitions in random order: key order
    // would hand every sweep to the same low keys and starve the high ones.
    String sweepOrder = sweepLimit < sweepCap ? "random()" : "partitions.pk ASC";

    boolean usePartitionLocks = !ctx.isCockroach();
    var claimedIds = new ArrayList<String>();
    Connection connection = ctx.getConnection();
    try {
      claimPartitionHeads(
          ctx,
          connection,
          queue,
          executorId,
          appVersion,
          sweepLimit,
          sweepOrder,
          usePartitionLocks,
          batchSize,
          ownerXid,
          claimedIds);
    } catch (Throwable t) {
      try {
        connection.close();
      } catch (SQLException closeFailure) {
        t.addSuppressed(closeFailure);
      }
      throw t;
    }
    try {
      connection.close();
    } catch (SQLException closeFailure) {
      // The batches have committed, so their workflows are claimed whether or not the connection
      // closes cleanly. Dropping the IDs would leave them PENDING with nothing dispatching them.
      if (claimedIds.isEmpty()) {
        throw closeFailure;
      }
      logger.warn(
          "Closing the connection after claiming {} partition head(s) of queue {} failed",
          claimedIds.size(),
          queue.name(),
          closeFailure);
    }
    return claimedIds;
  }

  /**
   * Walks the idle partitions and claims their heads in batches on {@code connection}, adding the
   * IDs each committed batch claims to {@code claimedIds} as it commits. Throws only when no batch
   * has committed; a failure after that ends the sweep with what it already claimed.
   */
  private static void claimPartitionHeads(
      DbContext ctx,
      Connection connection,
      Queue queue,
      String executorId,
      String appVersion,
      long sweepLimit,
      String sweepOrder,
      boolean usePartitionLocks,
      int batchSize,
      String ownerXid,
      List<String> claimedIds)
      throws SQLException {
    connection.setAutoCommit(false);
    // The walk only reads, and each batch below reads again under its locks.
    connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
    String versionClause;
    List<String> partitionKeys = new ArrayList<>();
    try {
      versionClause = versionClause(ctx, connection, appVersion);

      var walkQuery =
          """
            %1$s,
            chosen AS (
              SELECT partitions.pk
              FROM partitions
              WHERE partitions.pk IS NOT NULL
                AND %2$s
              ORDER BY %3$s
              LIMIT ?
            )
            SELECT chosen.pk
            FROM chosen
            -- Only partitions with a head this worker may run. LATERAL plans as a nested loop of
            -- top-1 probes; a correlated scalar subquery would run as a slower per-row SubPlan.
            JOIN LATERAL (%4$s) head ON TRUE
            -- Which partitions to claim is settled above; this orders the claim and its dispatch.
            ORDER BY chosen.pk ASC
            """
              .formatted(
                  partitionKeysCte(ctx),
                  noPending(ctx, "partitions.pk"),
                  sweepOrder,
                  headOf(ctx, "chosen.pk", versionClause));

      try (var ps = connection.prepareStatement(walkQuery)) {
        int i = bindPartitionKeysCte(ctx, ps, 1, queue.name());
        i = bindNoPending(ps, i, queue.name());
        ps.setLong(i++, sweepLimit);
        bindHeadOf(ctx, ps, i, queue.name(), appVersion);
        try (ResultSet rs = ps.executeQuery()) {
          while (rs.next()) {
            partitionKeys.add(rs.getString(1));
          }
        }
      }
      // It only read, so there is nothing to commit.
      connection.rollback();
    } catch (Throwable t) {
      try {
        connection.rollback();
      } catch (SQLException rollbackFailure) {
        t.addSuppressed(rollbackFailure);
      }
      throw t;
    }
    if (partitionKeys.isEmpty()) {
      return;
    }

    if (!usePartitionLocks) {
      // CockroachDB, without advisory locks: SERIALIZABLE aborts one of two sweeps that claim
      // into the same partition.
      connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
    }
    // One short transaction per batch, so a sweep holds at most a batch's locks, and only while
    // it claims that batch.
    for (int start = 0; start < partitionKeys.size(); start += batchSize) {
      var batch = partitionKeys.subList(start, Math.min(start + batchSize, partitionKeys.size()));
      try {
        var batchIds =
            claimBatch(
                ctx,
                connection,
                queue,
                executorId,
                appVersion,
                versionClause,
                ownerXid,
                batch,
                usePartitionLocks);
        // As in the per-partition dequeue: a batch that claims nothing rolls back rather than
        // committing the row locks it took.
        if (batchIds.isEmpty()) {
          connection.rollback();
        } else {
          connection.commit();
        }
        claimedIds.addAll(batchIds);
      } catch (Throwable t) {
        // No path may leave the transaction open: it holds partition locks and row locks other
        // executors are waiting to claim under.
        try {
          connection.rollback();
        } catch (SQLException rollbackFailure) {
          t.addSuppressed(rollbackFailure);
        }
        if (claimedIds.isEmpty() || !(t instanceof Exception)) {
          throw t;
        }
        // Earlier batches committed their claims, which must still reach dispatch; the next
        // poll retries the rest.
        logger.warn(
            "Stopping a partitioned sweep of queue {} after {} claim(s)",
            queue.name(),
            claimedIds.size(),
            t);
        break;
      }
    }
    if (!claimedIds.isEmpty()) {
      logger.debug("dequeued {} partition head(s) from {} queue", claimedIds.size(), queue.name());
    }
  }

  /**
   * Claims the heads of {@code batch}'s partitions in the transaction open on {@code connection},
   * and returns the IDs it marked PENDING, in partition key order. Leaves the transaction open for
   * the caller to end.
   */
  private static List<String> claimBatch(
      DbContext ctx,
      Connection connection,
      Queue queue,
      String executorId,
      String appVersion,
      String versionClause,
      String ownerXid,
      List<String> batch,
      boolean usePartitionLocks)
      throws SQLException {
    List<String> held = batch;
    if (usePartitionLocks) {
      DebugTriggers.debugTriggerPoint(DebugTriggers.DEBUG_TRIGGER_PARTITIONED_DEQUEUE_BEFORE_LOCKS);
      // Partitions another sweep holds are left to it, or to the next poll.
      held = lockPartitions(connection, ctx.schema(), queue.name(), batch);
      if (held.isEmpty()) {
        return List.of();
      }
    }

    // Pick the heads again under the locks, so the previous holder's claims, and any head that has
    // become visible since the walk, are seen.
    var headQuery =
        """
        SELECT head.workflow_uuid
        FROM unnest(CAST(? AS text[])) AS chosen(pk)
        JOIN LATERAL (%s) head ON TRUE
        WHERE %s
        ORDER BY chosen.pk ASC
        """
            .formatted(headOf(ctx, "chosen.pk", versionClause), noPending(ctx, "chosen.pk"));
    List<String> candidateIds = new ArrayList<>();
    try (var ps = connection.prepareStatement(headQuery)) {
      ps.setArray(1, connection.createArrayOf("text", held.toArray()));
      int i = bindHeadOf(ctx, ps, 2, queue.name(), appVersion);
      bindNoPending(ps, i, queue.name());
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          candidateIds.add(rs.getString(1));
        }
      }
    }
    if (candidateIds.isEmpty()) {
      return List.of();
    }
    DebugTriggers.debugTriggerPoint(
        DebugTriggers.DEBUG_TRIGGER_PARTITIONED_DEQUEUE_AFTER_CANDIDATES);

    // Re-check queue, partition and version alongside status, so a row moved to another queue
    // since the head read is dropped rather than claimed from the wrong queue.
    String claimGuard =
        """
        workflow_uuid = ANY(?)
          AND status = ?
          AND queue_name = ?
          AND queue_partition_key IS NOT NULL
          AND %s"""
                .formatted(versionClause)
            + ctx.andAppScope();

    // Lock the fixed candidate set. A LIMIT query's SKIP LOCKED could slide past a locked head
    // to the row behind it, and admit a partition out of order.
    Array candidateArray = connection.createArrayOf("text", candidateIds.toArray());
    var lockedIds = new HashSet<String>();
    try (var ps =
        connection.prepareStatement(
            """
            SELECT workflow_uuid FROM "%s".workflow_status WHERE %s FOR UPDATE SKIP LOCKED
            """
                .formatted(ctx.schema(), claimGuard))) {
      ps.setArray(1, candidateArray);
      ps.setString(2, WorkflowState.ENQUEUED.name());
      ps.setString(3, queue.name());
      ps.setString(4, appVersion);
      ctx.bindAppScope(ps, 5);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          lockedIds.add(rs.getString(1));
        }
      }
    }
    var claimIds = candidateIds.stream().filter(lockedIds::contains).toList();
    if (claimIds.isEmpty()) {
      return List.of();
    }

    String updateQuery =
        """
        UPDATE "%1$s".workflow_status
        SET status = ?,
            application_version = ?,
            executor_id = ?,
            -- The claim's token, as in the per-partition dequeue.
            owner_xid = ?,
            started_at_epoch_ms = %2$s,
            updated_at = %2$s,
            rate_limited = FALSE,
            -- Count this dispatch against the dead-letter budget; no later write does it.
            recovery_attempts = recovery_attempts + 1,
            -- Claim it as it is taken, as the per-partition dequeue does.
            application_name = COALESCE(application_name, ?),
            -- The database's clock, as in the per-partition dequeue.
            workflow_deadline_epoch_ms = CASE
                WHEN workflow_timeout_ms IS NOT NULL AND workflow_deadline_epoch_ms IS NULL
                THEN %4$s
                ELSE workflow_deadline_epoch_ms
            END
        WHERE %3$s
        RETURNING workflow_uuid
        """
            .formatted(
                ctx.schema(),
                SystemDatabase.NOW_EPOCH_MS,
                claimGuard,
                SystemDatabase.deadlineEpochMs("workflow_timeout_ms"));

    var flippedIds = new HashSet<String>();
    try (var ps = connection.prepareStatement(updateQuery)) {
      ps.setString(1, WorkflowState.PENDING.name());
      ps.setString(2, appVersion);
      ps.setString(3, executorId);
      ps.setString(4, ownerXid);
      ps.setString(5, ctx.appName());
      ps.setArray(6, connection.createArrayOf("text", claimIds.toArray()));
      ps.setString(7, WorkflowState.ENQUEUED.name());
      ps.setString(8, queue.name());
      ps.setString(9, appVersion);
      ctx.bindAppScope(ps, 10);
      try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
          flippedIds.add(rs.getString(1));
        }
      }
    }

    // Preserve partition order for dispatch.
    return claimIds.stream().filter(flippedIds::contains).toList();
  }

  /**
   * Takes {@code key}'s advisory lock for the open transaction, if no other transaction holds it.
   *
   * @return whether this transaction now holds it
   */
  private static boolean tryClaimLock(Connection connection, long key) throws SQLException {
    try (var ps = connection.prepareStatement("SELECT pg_try_advisory_xact_lock(?)")) {
      ps.setLong(1, key);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getBoolean(1);
      }
    }
  }

  /**
   * Takes the open transaction's advisory lock on each of {@code partitionKeys}, skipping any
   * another transaction holds. Two-int keys, the queue and then the partition, so the locks of one
   * queue's partitions never collide with another queue's.
   *
   * @return the partitions this transaction now holds
   */
  private static List<String> lockPartitions(
      Connection connection, String schema, String queueName, List<String> partitionKeys)
      throws SQLException {
    var lockIds =
        partitionKeys.stream().map(AdvisoryLocks::partitionClaimId).toArray(Integer[]::new);
    try (var ps =
        connection.prepareStatement(
            """
            SELECT coalesce(array_agg(p.pk), '{}')
            FROM unnest(CAST(? AS text[]), CAST(? AS int4[])) AS p(pk, lock_id)
            WHERE pg_try_advisory_xact_lock(CAST(? AS int4), p.lock_id)
            """)) {
      ps.setArray(1, connection.createArrayOf("text", partitionKeys.toArray()));
      ps.setArray(2, connection.createArrayOf("int4", lockIds));
      ps.setInt(3, AdvisoryLocks.partitionClaimQueueId(schema, queueName));
      try (ResultSet rs = ps.executeQuery()) {
        if (!rs.next()) {
          return List.of();
        }
        var held = rs.getArray(1);
        return held == null ? List.of() : List.of((String[]) held.getArray());
      }
    }
  }

  /**
   * A partition's head: its first ENQUEUED workflow this worker may run, as a subquery for a
   * LATERAL join on the partition key expression {@code pk}. Bind its parameters with {@link
   * #bindHeadOf}.
   */
  private static String headOf(DbContext ctx, String pk, String versionClause) {
    return """
        SELECT workflow_uuid
        FROM "%1$s".workflow_status
        WHERE queue_name = ? AND status = ?
          AND queue_partition_key = %2$s
          AND %3$s%4$s
        -- workflow_uuid breaks created_at ties, so every worker picks the same head, and it ends
        -- idx_workflow_status_partition_dequeue_v3, so this stays a pure index probe.
        ORDER BY priority ASC, created_at ASC, workflow_uuid ASC
        LIMIT 1"""
        .formatted(ctx.schema(), pk, versionClause, ctx.andAppScope());
  }

  /** Binds {@link #headOf}'s parameters from {@code index}, and returns the next free index. */
  private static int bindHeadOf(
      DbContext ctx, PreparedStatement ps, int index, String queueName, String appVersion)
      throws SQLException {
    ps.setString(index++, queueName);
    ps.setString(index++, WorkflowState.ENQUEUED.name());
    ps.setString(index++, appVersion);
    return ctx.bindAppScope(ps, index);
  }

  /**
   * A predicate that the partition named by the key expression {@code pk} has no PENDING workflow.
   * Not scoped to this application: a partition running any owner's workflow is busy. Bind its
   * parameters with {@link #bindNoPending}.
   */
  private static String noPending(DbContext ctx, String pk) {
    return """
        NOT EXISTS (
          SELECT 1
          FROM "%s".workflow_status
          WHERE queue_name = ? AND status = ?
            AND queue_partition_key IS NOT NULL
            AND queue_partition_key = %s)"""
        .formatted(ctx.schema(), pk);
  }

  /** Binds {@link #noPending}'s parameters from {@code index}, and returns the next free index. */
  private static int bindNoPending(PreparedStatement ps, int index, String queueName)
      throws SQLException {
    ps.setString(index++, queueName);
    ps.setString(index++, WorkflowState.PENDING.name());
    return index;
  }

  /**
   * The predicate on {@code application_version} a dequeue admits, with one {@code ?} for this
   * worker's version.
   *
   * <p>Version-less workflows (application_version IS NULL) are only dequeued when this worker is
   * running the latest registered application version.
   */
  private static String versionClause(DbContext ctx, Connection connection, String appVersion)
      throws SQLException {
    var latest = ApplicationVersionDAO.latestApplicationVersion(ctx, connection);
    boolean isLatestVersion = latest == null || latest.versionName().equals(appVersion);
    return isLatestVersion
        ? "(application_version = ? OR application_version IS NULL)"
        : "application_version = ?";
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
          UPDATE "%1$s".workflow_status
          SET status = ?,
              started_at_epoch_ms = NULL,
              updated_at = %2$s,
              -- Released: an execution still running on the named executor stops at its next
              -- checkpoint, and the claim that picks the workflow up installs its own token.
              owner_xid = NULL,
              queue_name = COALESCE(NULLIF(queue_name, ''), ?)
          WHERE status = ?
            AND executor_id = ANY(?)
            AND application_version = ?
        """
                .formatted(ctx.schema(), SystemDatabase.NOW_EPOCH_MS)
            + ctx.andAppScope()
            + " RETURNING workflow_uuid";

    try (Connection connection = ctx.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {
      Array executorIdArray = connection.createArrayOf("text", executorIds.toArray());
      stmt.setString(1, WorkflowState.ENQUEUED.name());
      stmt.setString(2, recoveryQueueName);
      stmt.setString(3, WorkflowState.PENDING.name());
      stmt.setArray(4, executorIdArray);
      stmt.setString(5, appVersion);
      ctx.bindAppScope(stmt, 6);

      var workflowIds = new ArrayList<String>();
      try (ResultSet rs = stmt.executeQuery()) {
        while (rs.next()) {
          workflowIds.add(rs.getString("workflow_uuid"));
        }
      }
      return workflowIds;
    }
  }

  /**
   * How many of this queue's workflows are running across every executor, counted as the dequeue
   * counts them against the queue-wide concurrency limit.
   */
  public static int countPendingQueueWorkflows(DbContext ctx, String queueName)
      throws SQLException {
    try (Connection connection = ctx.getConnection()) {
      return pendingCount(ctx, connection, queueName, null);
    }
  }

  /**
   * The partition keys with an ENQUEUED workflow on the queue, in no guaranteed order.
   *
   * <p>Walks them with {@link #partitionKeysCte}, so the cost grows with the number of partitions
   * rather than the backlog: SELECT DISTINCT reads every ENQUEUED row to find the same keys.
   */
  public static List<String> getQueuePartitions(DbContext ctx, String queueName)
      throws SQLException {

    final String sql = partitionKeysCte(ctx) + " SELECT pk FROM partitions WHERE pk IS NOT NULL";

    try (Connection connection = ctx.getConnection();
        PreparedStatement stmt = connection.prepareStatement(sql)) {
      bindPartitionKeysCte(ctx, stmt, 1, queueName);

      try (ResultSet rs = stmt.executeQuery()) {
        List<String> partitions = new ArrayList<>();
        while (rs.next()) {
          partitions.add(rs.getString(1));
        }
        return partitions;
      }
    }
  }

  /**
   * A recursive CTE named {@code partitions} whose {@code pk} column lists the distinct partition
   * keys with an ENQUEUED workflow this application may dequeue, ending in one NULL row.
   *
   * <p>It is a loose index scan: each step is one seek on idx_workflow_status_partition_dequeue_v3
   * for the next key above the last, so it costs one seek per partition however deep each
   * partition's backlog is. Bind its parameters with {@link #bindPartitionKeysCte}.
   */
  private static String partitionKeysCte(DbContext ctx) {
    return """
        WITH RECURSIVE partitions AS (
          (SELECT MIN(queue_partition_key) AS pk
           FROM "%1$s".workflow_status
           WHERE queue_name = ? AND status = ? AND queue_partition_key IS NOT NULL%2$s)
          UNION ALL
          (SELECT (SELECT MIN(queue_partition_key)
                   FROM "%1$s".workflow_status
                   WHERE queue_name = ? AND status = ?
                     AND queue_partition_key > partitions.pk%2$s)
           FROM partitions
           WHERE partitions.pk IS NOT NULL)
        )"""
        .formatted(ctx.schema(), ctx.andAppScope());
  }

  /** Binds {@link #partitionKeysCte}'s parameters from {@code index}; returns the next free one. */
  private static int bindPartitionKeysCte(
      DbContext ctx, PreparedStatement stmt, int index, String queueName) throws SQLException {
    for (int step = 0; step < 2; step++) {
      stmt.setString(index++, queueName);
      stmt.setString(index++, WorkflowState.ENQUEUED.name());
      index = ctx.bindAppScope(stmt, index);
    }
    return index;
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
    // The constructor checks no limits, because it is also the read path.
    queue.validateForRegistration();
    var requestedOwner = applicationName != null ? applicationName : ctx.appName();
    final String insertSql =
        """
        INSERT INTO "%1$s".queues
          (name, concurrency, worker_concurrency, rate_limit_max, rate_limit_period_sec,
            partition_concurrency, partition_worker_concurrency,
            partition_rate_limit_max, partition_rate_limit_period_sec,
            priority_enabled, partition_queue, polling_interval_sec, updated_at, application_name)
        -- priority_enabled is vestigial: every queue dispatches in priority order.
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, TRUE, ?, ?, %2$s, ?)
        ON CONFLICT (name) DO NOTHING
        """
            .formatted(ctx.schema(), SystemDatabase.NOW_EPOCH_MS);
    final String updateSql =
        """
        UPDATE "%1$s".queues SET
          concurrency                     = ?,
          worker_concurrency              = ?,
          rate_limit_max                  = ?,
          rate_limit_period_sec           = ?,
          partition_concurrency           = ?,
          partition_worker_concurrency    = ?,
          partition_rate_limit_max        = ?,
          partition_rate_limit_period_sec = ?,
          priority_enabled                = TRUE,
          partition_queue                 = ?,
          polling_interval_sec            = ?,
          updated_at                      = %2$s,
          -- Claim only an unclaimed row, so a registration landing between the ownership
          -- check above and this write keeps the name it just took.
          application_name                = COALESCE(application_name, ?)
        WHERE name = ?
        """
            .formatted(ctx.schema(), SystemDatabase.NOW_EPOCH_MS);

    try (Connection connection = ctx.getConnection()) {
      // Read the current owner first: the writes below are silent about why they declined to claim.
      var owner =
          RowOwner.resolve(
              connection, ctx.schema(), "queues", "name", queue.name(), requestedOwner, "Queue");
      boolean inserted;
      try (PreparedStatement ps = connection.prepareStatement(insertSql)) {
        ps.setString(1, queue.name());
        var index = bindQueueParams(ps, queue, 2);
        ps.setString(index, owner);
        inserted = ps.executeUpdate() == 1;
      }
      if (!inserted && updateExisting) {
        try (PreparedStatement ps = connection.prepareStatement(updateSql)) {
          var index = bindQueueParams(ps, queue, 1);
          ps.setString(index, owner);
          ps.setString(index + 1, queue.name());
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
    // No write stores either of these, but a row is loaded as it stands. A max of 0 or less leaves
    // no room, whatever the window. A period of 0 or less counts no earlier starts, so each poll
    // may start up to the max.
    if (rateLimit.limit() <= 0) {
      return 0;
    }
    var sql =
        """
        SELECT COUNT(*)
        FROM "%1$s".workflow_status
        WHERE queue_name = ?
        AND rate_limited = true
        AND status NOT IN (?, ?)
        AND started_at_epoch_ms > %2$s - ?
      """
                .formatted(ctx.schema(), SystemDatabase.NOW_EPOCH_MS)
            + ctx.andAppScope();
    if (partitionKey != null) {
      sql += " AND queue_partition_key = ?";
    }

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
      ps.setString(1, queueName);
      ps.setString(2, WorkflowState.ENQUEUED.name());
      ps.setString(3, WorkflowState.DELAYED.name());
      ps.setLong(4, rateLimit.period().toMillis());
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

  /**
   * Binds a queue row's columns, from {@code concurrency} through {@code polling_interval_sec},
   * starting at {@code offset}, and returns the next free index. The insert and the update list
   * those columns in the same order, with {@code priority_enabled} written as a literal in both.
   */
  private static int bindQueueParams(PreparedStatement ps, Queue queue, int offset)
      throws SQLException {
    setNullableInt(ps, offset, queue.concurrency());
    setNullableInt(ps, offset + 1, queue.workerConcurrency());
    setRateLimit(ps, offset + 2, queue.rateLimit());
    setNullableInt(ps, offset + 4, queue.partitionConcurrency());
    setNullableInt(ps, offset + 5, queue.partitionWorkerConcurrency());
    setRateLimit(ps, offset + 6, queue.partitionRateLimit());
    // Derived, not copied: the column follows the per-partition limits, and other SDKs read it
    // to decide whether to dequeue per partition.
    ps.setBoolean(offset + 8, queue.isPartitioned());
    ps.setDouble(offset + 9, queue.pollingInterval().toMillis() / 1000.0);
    return offset + 10;
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
          partition_queue, polling_interval_sec, application_name
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
          partition_queue, polling_interval_sec, application_name
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
        // Vestigial, but other SDKs still read it; every write heals a row an earlier version
        // stored as false.
        setClauses.add("\"priority_enabled\" = TRUE");
        // Partitioning is inferred from the per-partition limits, so the stored flag follows them
        // on every write. Left alone it would go stale in both directions: unset on a queue that
        // just gained its first partition limit, and still set on one that just lost its last.
        setClauses.add("\"partition_queue\" = ?");
        params.add(updated.isPartitioned());
        collectOptional(
            setClauses, params, "polling_interval_sec", durationToSec(update.pollingInterval()));

        setClauses.add("\"updated_at\" = " + SystemDatabase.NOW_EPOCH_MS);
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
   * The queue a partial update would produce, as a whole row, so that {@link
   * Queue#validateForRegistration} can check rules that span fields the update did not all supply.
   */
  @SuppressWarnings("removal") // reads the legacy partitionQueue option verbatim
  private static Queue applyUpdate(Queue current, QueueOptions update) {
    var updated =
        new Queue(
            current.name(),
            intAfter(current.concurrency(), update.concurrency()),
            intAfter(current.workerConcurrency(), update.workerConcurrency()),
            true, // every queue dispatches in priority order
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
          ("queue %s cannot have half of %s: %sMax and %sPeriod are set and cleared together")
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
    // Writes refuse a non-positive interval, but a row is loaded as stored, and polling on one
    // would spin: read it as unset.
    if (pollingInterval.isNegative() || pollingInterval.isZero()) {
      pollingInterval = Queue.DEFAULT_POLLING_INTERVAL;
    }
    // Built without validation, as the row stands, and refusing to load it would hide the queue
    // from listQueues and leave its listener polling on stale configuration. Every dequeue path
    // gives a limit no write would accept a defined meaning: a concurrency limit or rate-limit max
    // of 0 or less, at any scope, leaves no room, and a rate-limit period of 0 or less allows up to
    // the max per poll (see rateLimitRemaining).
    return new Queue(
        name,
        concurrency,
        workerConcurrency,
        true, // every queue dispatches in priority order, whatever the vestigial column says
        partitioningEnabled,
        rateLimit,
        rs.getObject("partition_concurrency", Integer.class),
        rs.getObject("partition_worker_concurrency", Integer.class),
        rateLimitFromResultSet(rs, "partition_rate_limit_max", "partition_rate_limit_period_sec"),
        pollingInterval,
        rs.getString("application_name"));
  }

  // Reads the deprecated partitionQueue option verbatim.
  @SuppressWarnings("removal")
  private static Queue queueFromOptions(String name, QueueOptions options) {
    Integer concurrencyVal = options.concurrency().isPresent() ? options.concurrency().get() : null;
    Integer workerConcurrencyVal =
        options.workerConcurrency().isPresent() ? options.workerConcurrency().get() : null;
    boolean partitionQueueVal = options.partitionQueue().orElse(false);

    // A new queue has no current limit, so a half-set pair is refused exactly as an update that
    // would leave one behind is, rather than silently registering no limit at all.
    Queue.RateLimit rateLimit =
        rateLimitAfter(name, "rateLimit", null, options.rateLimitMax(), options.rateLimitPeriod());
    Queue.RateLimit partitionRateLimit =
        rateLimitAfter(
            name,
            "partitionRateLimit",
            null,
            options.partitionRateLimitMax(),
            options.partitionRateLimitPeriod());

    Duration pollingIntervalVal = options.pollingInterval().orElse(Queue.DEFAULT_POLLING_INTERVAL);

    return new Queue(
        name,
        concurrencyVal,
        workerConcurrencyVal,
        true, // every queue dispatches in priority order
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
