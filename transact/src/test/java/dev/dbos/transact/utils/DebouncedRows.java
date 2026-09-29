package dev.dbos.transact.utils;

import dev.dbos.transact.Constants;
import dev.dbos.transact.workflow.WorkflowState;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.UUID;

import javax.sql.DataSource;

import org.jspecify.annotations.Nullable;

/**
 * Plants and reads the rows the debouncers leave behind: a debounced workflow -- the user workflow
 * itself, waiting DELAYED on its queue and holding its debounce key as its deduplication ID -- and
 * the debouncer service workflow SDK versions before 1.2 used instead, with the steps such a
 * version recorded for a workflow that debounced.
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

  /**
   * Plants a debouncer service workflow as a version before 1.2 enqueued it: ENQUEUED on the
   * internal queue, holding the debounce key, with its inputs. Under {@code applicationVersion} set
   * to one no executor serves it is stranded -- nothing will ever run it, as once the last node of
   * the SDK version that enqueued it is gone; under the executor's own version, this process runs
   * it, as a live node of that version would.
   */
  public static String insertService(
      DataSource dataSource,
      String deduplicationId,
      String inputs,
      @Nullable String serialization,
      String applicationVersion,
      @Nullable String applicationName)
      throws SQLException {
    var workflowId = UUID.randomUUID().toString();
    var sql =
        """
          INSERT INTO "dbos".workflow_status
              (workflow_uuid, status, name, class_name, queue_name, deduplication_id,
               serialization, application_version, application_name,
               created_at, updated_at, recovery_attempts, priority)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)
        """;
    long now = System.currentTimeMillis();
    try (Connection conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setString(2, WorkflowState.ENQUEUED.name());
      stmt.setString(3, Constants.DEBOUNCER_WORKFLOW_NAME);
      stmt.setString(4, Constants.DEBOUNCER_CLASS_NAME);
      stmt.setString(5, Constants.DBOS_INTERNAL_QUEUE);
      stmt.setString(6, deduplicationId);
      stmt.setString(7, serialization);
      stmt.setString(8, applicationVersion);
      stmt.setString(9, applicationName);
      stmt.setLong(10, now);
      stmt.setLong(11, now);
      stmt.executeUpdate();
    }
    insertInput(dataSource, workflowId, inputs);
    return workflowId;
  }

  /**
   * Plants one recorded step of {@code workflowId}, as an earlier version wrote it: a step's
   * output, or, with {@code childWorkflowId}, the child a workflow started in that slot.
   */
  public static void insertStep(
      DataSource dataSource,
      String workflowId,
      int functionId,
      String functionName,
      @Nullable String output,
      @Nullable String serialization,
      @Nullable String childWorkflowId)
      throws SQLException {
    var sql =
        """
          INSERT INTO "dbos".operation_outputs
              (workflow_uuid, function_id, function_name, output, child_workflow_id,
               started_at_epoch_ms, completed_at_epoch_ms, serialization)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """;
    long now = System.currentTimeMillis();
    try (Connection conn = dataSource.getConnection();
        var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, workflowId);
      stmt.setInt(2, functionId);
      stmt.setString(3, functionName);
      stmt.setString(4, output);
      stmt.setString(5, childWorkflowId);
      stmt.setLong(6, now);
      stmt.setLong(7, now);
      stmt.setString(8, serialization);
      stmt.executeUpdate();
    }
  }

  /** A recorded step's output and the format it is in. */
  public record Step(@Nullable String output, @Nullable String serialization) {}

  /** Deletes every recorded step of {@code workflowId}. */
  public static void deleteSteps(DataSource dataSource, String workflowId) throws SQLException {
    try (Connection conn = dataSource.getConnection();
        var stmt =
            conn.prepareStatement(
                "DELETE FROM \"dbos\".operation_outputs WHERE workflow_uuid = ?")) {
      stmt.setString(1, workflowId);
      stmt.executeUpdate();
    }
  }

  /** Deletes a workflow's status row and its input. */
  public static void deleteWorkflow(DataSource dataSource, String workflowId) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      for (var table : new String[] {"workflow_input", "workflow_status"}) {
        try (var stmt =
            conn.prepareStatement(
                "DELETE FROM \"dbos\".%s WHERE workflow_uuid = ?".formatted(table))) {
          stmt.setString(1, workflowId);
          stmt.executeUpdate();
        }
      }
    }
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
      @Nullable String queueName,
      @Nullable String deduplicationId,
      @Nullable Long delayUntilEpochMs,
      boolean isDebounced,
      @Nullable Long debounceDeadlineEpochMs,
      int priority,
      String inputs,
      @Nullable String serialization,
      @Nullable String applicationName) {}

  public static State read(DataSource dataSource, String workflowId) throws SQLException {
    var sql =
        """
          SELECT ws.status, ws.queue_name, ws.deduplication_id, ws.delay_until_epoch_ms,
                 ws.is_debounced, ws.debounce_deadline_epoch_ms, ws.priority,
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
            rs.getString("queue_name"),
            rs.getString("deduplication_id"),
            rs.getObject("delay_until_epoch_ms", Long.class),
            rs.getBoolean("is_debounced"),
            rs.getObject("debounce_deadline_epoch_ms", Long.class),
            rs.getInt("priority"),
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
