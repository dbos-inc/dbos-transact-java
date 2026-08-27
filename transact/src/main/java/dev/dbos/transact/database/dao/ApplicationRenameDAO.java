package dev.dbos.transact.database.dao;

import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.internal.Validation;
import dev.dbos.transact.workflow.ApplicationRowCounts;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * Moving ownership of a system database's rows from one application name to another, for after an
 * application is renamed or when adopting the rows an upgrade left unclaimed.
 *
 * <p>The renamed application must be stopped: its dequeues claim rows, and would race this.
 */
public class ApplicationRenameDAO {

  /** Workflows and steps re-owned per transaction, when the caller does not choose. */
  public static final int DEFAULT_RENAME_BATCH_SIZE = 10_000;

  /**
   * Statuses whose rows move in the same transaction as the queues, schedules and versions. A
   * half-owned application would dequeue work whose version row it can no longer see.
   */
  private static final List<String> ATOMIC_STATUSES =
      List.of(
          WorkflowState.PENDING.name(),
          WorkflowState.ENQUEUED.name(),
          WorkflowState.DELAYED.name());

  private ApplicationRenameDAO() {}

  /**
   * Rows a rename moves: an application's own, unclaimed ones, or both. Unlike the scope a listing
   * or a dequeue uses, unclaimed rows are not implied here -- they move only when asked.
   *
   * <p>Appends this predicate's parameters to {@code params}, in order.
   */
  private static String renameSource(
      @Nullable String oldName, boolean adoptUnclaimedRows, List<Object> params) {
    var clauses = new ArrayList<String>();
    if (oldName != null) {
      clauses.add("application_name = ?");
      params.add(oldName);
    }
    if (adoptUnclaimedRows) {
      clauses.add("application_name IS NULL");
    }
    // renameApplication validates that at least one source is named.
    return "(" + String.join(" OR ", clauses) + ")";
  }

  private static long update(Connection conn, String sql, List<Object> params) throws SQLException {
    try (PreparedStatement stmt = conn.prepareStatement(sql)) {
      for (int i = 0; i < params.size(); i++) {
        stmt.setObject(i + 1, params.get(i));
      }
      return stmt.executeUpdate();
    }
  }

  /**
   * Re-own a table's rows in half-open key ranges, so a long history neither moves in one
   * transaction nor rescans what it already moved; a re-run resumes where it stopped.
   */
  private static long renameRowsInBatches(
      DbContext ctx,
      String table,
      String keyColumn,
      @Nullable String oldName,
      String newName,
      @Nullable Integer batchSize,
      boolean adoptUnclaimedRows)
      throws SQLException {

    if (batchSize == null) {
      try (var conn = ctx.getConnection()) {
        var params = new ArrayList<Object>();
        params.add(newName);
        var predicate = renameSource(oldName, adoptUnclaimedRows, params);
        var sql =
            "UPDATE \"%s\".%s SET application_name = ? WHERE %s"
                .formatted(ctx.schema(), table, predicate);
        return update(conn, sql, params);
      }
    }

    long total = 0;
    // Ranges, not LIMIT: a LIMIT repages every row already moved, and an IN list of keys plans as
    // a whole-table hash join.
    String watermark = null;
    while (true) {
      try (var conn = ctx.getConnection()) {
        var boundParams = new ArrayList<Object>();
        var predicate = renameSource(oldName, adoptUnclaimedRows, boundParams);
        var scope = predicate;
        if (watermark != null) {
          scope = predicate + " AND " + keyColumn + " > ?";
          boundParams.add(watermark);
        }

        // The batchSize-th matching key bounds this range; distinct, so a key's rows are never
        // split across batches.
        String upper = null;
        var upperSql =
            "SELECT DISTINCT %s FROM \"%s\".%s WHERE %s ORDER BY %s LIMIT 1 OFFSET %d"
                .formatted(keyColumn, ctx.schema(), table, scope, keyColumn, batchSize - 1);
        try (var stmt = conn.prepareStatement(upperSql)) {
          for (int i = 0; i < boundParams.size(); i++) {
            stmt.setObject(i + 1, boundParams.get(i));
          }
          try (var rs = stmt.executeQuery()) {
            if (rs.next()) {
              upper = rs.getString(1);
            }
          }
        }

        var updateParams = new ArrayList<Object>();
        updateParams.add(newName);
        var batch = renameSource(oldName, adoptUnclaimedRows, updateParams);
        if (upper != null) {
          if (watermark != null) {
            batch = batch + " AND " + keyColumn + " > ?";
            updateParams.add(watermark);
          }
          batch = batch + " AND " + keyColumn + " <= ?";
          updateParams.add(upper);
        }
        // The final batch drops the watermark, so rows that appeared below it still move.
        var updateSql =
            "UPDATE \"%s\".%s SET application_name = ? WHERE %s"
                .formatted(ctx.schema(), table, batch);
        total += update(conn, updateSql, updateParams);

        // Fewer than a full batch remained, so that update took the rest.
        if (upper == null) {
          return total;
        }
        watermark = upper;
      }
    }
  }

  /**
   * Give {@code newName} ownership of the rows {@code oldName} holds, of unclaimed rows, or of
   * both.
   *
   * @param oldName the application being renamed; null to adopt only unclaimed rows
   * @param newName the application that ends up owning the rows
   * @param batchSize workflows and steps re-owned per transaction; null moves them all in one
   * @param adoptUnclaimedRows whether to also take rows no application owns
   */
  public static ApplicationRowCounts renameApplication(
      DbContext ctx,
      @Nullable String oldName,
      String newName,
      @Nullable Integer batchSize,
      boolean adoptUnclaimedRows)
      throws SQLException {

    if (oldName != null && oldName.isEmpty()) {
      throw new IllegalArgumentException("The application's previous name cannot be empty.");
    }
    if (oldName == null && !adoptUnclaimedRows) {
      throw new IllegalArgumentException(
          "Nothing to re-own: name the application to rename, adopt unclaimed rows, or both.");
    }
    if (!Validation.isValidApplicationName(newName)) {
      throw new IllegalArgumentException(
          Validation.invalidApplicationName("application name", newName));
    }
    if (newName.equals(oldName)) {
      throw new IllegalArgumentException(
          "Application '%s' already holds that name; nothing to rename.".formatted(newName));
    }
    if (batchSize != null && batchSize < 1) {
      throw new IllegalArgumentException(
          "batchSize must be a positive integer, got %d".formatted(batchSize));
    }

    long queues;
    long schedules;
    long versions;
    long inFlight;
    // Never a merge: queue, schedule and version names are globally unique whatever their owner,
    // so this cannot collide.
    try (var conn = ctx.getConnection()) {
      conn.setAutoCommit(false);
      try {
        queues = move(conn, ctx.schema(), "queues", oldName, newName, adoptUnclaimedRows, false);
        schedules =
            move(
                conn,
                ctx.schema(),
                "workflow_schedules",
                oldName,
                newName,
                adoptUnclaimedRows,
                false);
        versions =
            move(
                conn,
                ctx.schema(),
                "application_versions",
                oldName,
                newName,
                adoptUnclaimedRows,
                false);
        inFlight =
            move(conn, ctx.schema(), "workflow_status", oldName, newName, adoptUnclaimedRows, true);
        conn.commit();
      } catch (SQLException | RuntimeException e) {
        conn.rollback();
        throw e;
      } finally {
        conn.setAutoCommit(true);
      }
    }

    // Only terminal rows are left to match, and they scope observability and garbage collection
    // alone, so they may lag behind the commit above.
    var terminal =
        renameRowsInBatches(
            ctx,
            "workflow_status",
            "workflow_uuid",
            oldName,
            newName,
            batchSize,
            adoptUnclaimedRows);
    var steps =
        renameRowsInBatches(
            ctx,
            "operation_outputs",
            "workflow_uuid",
            oldName,
            newName,
            batchSize,
            adoptUnclaimedRows);

    return new ApplicationRowCounts(queues, schedules, versions, inFlight + terminal, steps);
  }

  private static long move(
      Connection conn,
      String schema,
      String table,
      @Nullable String oldName,
      String newName,
      boolean adoptUnclaimedRows,
      boolean atomicStatusesOnly)
      throws SQLException {
    var params = new ArrayList<Object>();
    params.add(newName);
    var where = renameSource(oldName, adoptUnclaimedRows, params);
    if (atomicStatusesOnly) {
      where += " AND status = ANY(?)";
    }
    var sql = "UPDATE \"%s\".%s SET application_name = ? WHERE %s".formatted(schema, table, where);
    try (var stmt = conn.prepareStatement(sql)) {
      for (int i = 0; i < params.size(); i++) {
        stmt.setObject(i + 1, params.get(i));
      }
      if (atomicStatusesOnly) {
        stmt.setArray(
            params.size() + 1, conn.createArrayOf("text", ATOMIC_STATUSES.toArray(String[]::new)));
      }
      return stmt.executeUpdate();
    }
  }
}
