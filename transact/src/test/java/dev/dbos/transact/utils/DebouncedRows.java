package dev.dbos.transact.utils;

import dev.dbos.transact.workflow.WorkflowState;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;

/**
 * Plants the row a newer SDK version writes for a debounced workflow: the user workflow itself,
 * waiting DELAYED on its queue and holding its debounce key as its deduplication ID. Java does not
 * write this shape yet, so tests that must coalesce into one build it by hand.
 */
public final class DebouncedRows {

  private DebouncedRows() {}

  public record Spec(
      String workflowName,
      String className,
      @Nullable String instanceName,
      String queueName,
      String deduplicationId,
      long delayUntilEpochMs,
      @Nullable Long debounceDeadlineEpochMs,
      String inputs,
      @Nullable String serialization,
      @Nullable String applicationVersion,
      @Nullable String applicationName) {}

  /** Inserts the row and returns its workflow id. */
  public static String insert(DataSource dataSource, Spec spec) throws SQLException {
    var workflowId = UUID.randomUUID().toString();
    var sql =
        """
          INSERT INTO "dbos".workflow_status
              (workflow_uuid, status, name, class_name, config_name,
               queue_name, deduplication_id, delay_until_epoch_ms,
               is_debounced, debounce_deadline_epoch_ms,
               inputs, serialization, application_version, application_name,
               created_at, updated_at, recovery_attempts, priority)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, TRUE, ?, ?, ?, ?, ?, ?, ?, 0, 0)
        """;
    long now = System.currentTimeMillis();
    try (Connection conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setString(2, WorkflowState.DELAYED.name());
      stmt.setString(3, spec.workflowName());
      stmt.setString(4, spec.className());
      stmt.setString(5, spec.instanceName());
      stmt.setString(6, spec.queueName());
      stmt.setString(7, spec.deduplicationId());
      stmt.setLong(8, spec.delayUntilEpochMs());
      stmt.setObject(9, spec.debounceDeadlineEpochMs());
      stmt.setString(10, spec.inputs());
      stmt.setString(11, spec.serialization());
      stmt.setString(12, spec.applicationVersion());
      stmt.setString(13, spec.applicationName());
      stmt.setLong(14, now);
      stmt.setLong(15, now);
      stmt.executeUpdate();
    }
    return workflowId;
  }

  /** Plants the payload-table copy of a workflow's inputs, which readers prefer when present. */
  public static void insertInput(DataSource dataSource, String workflowId, String inputs)
      throws SQLException {
    var sql =
        """
          INSERT INTO "dbos".workflow_input (workflow_uuid, inputs, retention_timestamp)
          VALUES (?, ?, ?)
        """;
    try (Connection conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setString(2, inputs);
      stmt.setLong(3, System.currentTimeMillis());
      stmt.executeUpdate();
    }
  }

  /** The payload-table copy of a workflow's inputs, or null if there is none. */
  public static @Nullable String readInput(DataSource dataSource, String workflowId)
      throws SQLException {
    var sql = "SELECT inputs FROM \"dbos\".workflow_input WHERE workflow_uuid = ?";
    try (Connection conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        return rs.next() ? rs.getString("inputs") : null;
      }
    }
  }

  /**
   * What the row holds now: the columns a bounce or a transition touches. The inputs are the ones
   * the workflow would run with, read the way the SDK reads them: payload table first, the legacy
   * column as a fallback.
   */
  public record State(
      String status,
      @Nullable String deduplicationId,
      @Nullable Long delayUntilEpochMs,
      String inputs,
      @Nullable String serialization,
      @Nullable String applicationName) {}

  public static State read(DataSource dataSource, String workflowId) throws SQLException {
    var sql =
        """
          SELECT ws.status, ws.deduplication_id, ws.delay_until_epoch_ms,
                 COALESCE(wi.inputs, ws.inputs) AS inputs, ws.serialization, ws.application_name
            FROM "dbos".workflow_status ws
            LEFT JOIN "dbos".workflow_input wi ON wi.workflow_uuid = ws.workflow_uuid
           WHERE ws.workflow_uuid = ?
        """;
    try (Connection conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      try (var rs = stmt.executeQuery()) {
        if (!rs.next()) {
          throw new AssertionError("no workflow_status row for " + workflowId);
        }
        return new State(
            rs.getString("status"),
            rs.getString("deduplication_id"),
            rs.getObject("delay_until_epoch_ms", Long.class),
            rs.getString("inputs"),
            rs.getString("serialization"),
            rs.getString("application_name"));
      }
    }
  }

  public static int countByName(DataSource dataSource, String workflowName) throws SQLException {
    var sql = "SELECT COUNT(*) FROM \"dbos\".workflow_status WHERE name = ?";
    try (Connection conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowName);
      try (var rs = stmt.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }
}
