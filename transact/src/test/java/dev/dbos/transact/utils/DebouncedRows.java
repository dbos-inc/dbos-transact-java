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

  /** What the row holds now: the columns a bounce or a transition touches. */
  public record State(
      String status,
      @Nullable String deduplicationId,
      @Nullable Long delayUntilEpochMs,
      String inputs,
      @Nullable String serialization) {}

  public static State read(DataSource dataSource, String workflowId) throws SQLException {
    var sql =
        """
          SELECT status, deduplication_id, delay_until_epoch_ms, inputs, serialization
            FROM "dbos".workflow_status
           WHERE workflow_uuid = ?
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
            rs.getString("serialization"));
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
