package dev.dbos.transact.database;

import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.WorkflowSchedule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;

import javax.sql.DataSource;

class SchedulesDAO {

  private final DataSource dataSource;
  private final String schema;
  private final DBOSSerializer serializer;

  SchedulesDAO(DataSource dataSource, String schema, DBOSSerializer serializer) {
    this.dataSource = Objects.requireNonNull(dataSource);
    this.schema = Objects.requireNonNull(schema);
    this.serializer = serializer;
  }

  void createSchedule(WorkflowSchedule schedule) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      createSchedule(conn, schema, serializer, schedule);
    }
  }

  static void createSchedule(
      Connection conn, String schema, DBOSSerializer serializer, WorkflowSchedule schedule)
      throws SQLException {
    String sql =
        """
        INSERT INTO "%s".workflow_schedules
            (schedule_id, schedule_name, workflow_name, workflow_class_name,
             schedule, status, context, last_fired_at, automatic_backfill,
             cron_timezone, queue_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
            .formatted(schema);

    // https://github.com/dbos-inc/dbos-transact-java/issues/330
    // tracking portable serialization support
    var serializedContext =
        SerializationUtil.serializeValue(schedule.context(), serializer.name(), serializer);

    var timeZone = schedule.cronTimezone() == null ? null : schedule.cronTimezone().getId();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, schedule.id() != null ? schedule.id() : UUID.randomUUID().toString());
      ps.setString(2, schedule.scheduleName());
      ps.setString(3, schedule.workflowName());
      ps.setString(4, schedule.className());
      ps.setString(5, schedule.cron());
      ps.setString(6, Objects.requireNonNull(schedule.status()).name());
      ps.setString(7, serializedContext.serializedValue());
      ps.setString(8, schedule.lastFiredAt() != null ? schedule.lastFiredAt().toString() : null);
      ps.setBoolean(9, schedule.automaticBackfill());
      ps.setString(10, timeZone);
      ps.setString(11, schedule.queueName());
      ps.executeUpdate();
    } catch (SQLException e) {
      if ("23505".equals(e.getSQLState())) {
        throw new RuntimeException(
            "Schedule '%s' already exists".formatted(schedule.scheduleName()), e);
      }
      throw e;
    }
  }

  List<WorkflowSchedule> listSchedules(
      List<ScheduleStatus> statuses, List<String> workflowNames, List<String> scheduleNamePrefixes)
      throws SQLException {

    StringBuilder sql =
        new StringBuilder(
            """
            SELECT schedule_id, schedule_name, workflow_name, workflow_class_name,
                   schedule, status, context, last_fired_at, automatic_backfill,
                   cron_timezone, queue_name
            FROM "%s".workflow_schedules
            WHERE TRUE
            """
                .formatted(schema));

    List<Object> params = new ArrayList<>();

    if (statuses != null && !statuses.isEmpty()) {
      sql.append(" AND status = ANY(?)");
      params.add(statuses.stream().map(ScheduleStatus::name).toArray(String[]::new));
    }
    if (workflowNames != null && !workflowNames.isEmpty()) {
      sql.append(" AND workflow_name = ANY(?)");
      params.add(workflowNames.toArray(new String[0]));
    }
    if (scheduleNamePrefixes != null && !scheduleNamePrefixes.isEmpty()) {
      sql.append(" AND (");
      StringJoiner orClauses = new StringJoiner(" OR ");
      for (int i = 0; i < scheduleNamePrefixes.size(); i++) {
        orClauses.add("schedule_name LIKE ?");
        params.add(scheduleNamePrefixes.get(i).replace("%", "\\%").replace("_", "\\_") + "%");
      }
      sql.append(orClauses).append(")");
    }

    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql.toString())) {
      int idx = 1;
      for (Object param : params) {
        if (param instanceof String[] arr) {
          ps.setArray(idx++, conn.createArrayOf("text", arr));
        } else {
          ps.setString(idx++, (String) param);
        }
      }
      try (ResultSet rs = ps.executeQuery()) {
        List<WorkflowSchedule> results = new ArrayList<>();
        while (rs.next()) {
          results.add(rowToSchedule(rs, serializer));
        }
        return results;
      }
    }
  }

  Optional<WorkflowSchedule> getSchedule(String name) throws SQLException {
    String sql =
        """
        SELECT schedule_id, schedule_name, workflow_name, workflow_class_name,
               schedule, status, context, last_fired_at, automatic_backfill,
               cron_timezone, queue_name
        FROM "%s".workflow_schedules
        WHERE schedule_name = ?
        """
            .formatted(schema);

    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, name);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return Optional.of(rowToSchedule(rs, serializer));
        }
        return Optional.empty();
      }
    }
  }

  void pauseSchedule(String name) throws SQLException {
    setScheduleStatus(name, ScheduleStatus.PAUSED);
  }

  void resumeSchedule(String name) throws SQLException {
    setScheduleStatus(name, ScheduleStatus.ACTIVE);
  }

  private void setScheduleStatus(String name, ScheduleStatus status) throws SQLException {
    String sql =
        """
        UPDATE "%s".workflow_schedules SET status = ? WHERE schedule_name = ?
        """
            .formatted(schema);

    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, status.name());
      ps.setString(2, name);
      ps.executeUpdate();
    }
  }

  void updateScheduleLastFiredAt(String name, Instant lastFiredAt) throws SQLException {
    String sql =
        """
        UPDATE "%s".workflow_schedules SET last_fired_at = ? WHERE schedule_name = ?
        """
            .formatted(schema);

    try (Connection conn = dataSource.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, lastFiredAt != null ? lastFiredAt.toString() : null);
      ps.setString(2, name);
      ps.executeUpdate();
    }
  }

  void deleteSchedule(String name) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      deleteSchedule(conn, schema, name);
    }
  }

  static void deleteSchedule(Connection conn, String schema, String name) throws SQLException {
    String sql =
        """
        DELETE FROM "%s".workflow_schedules WHERE schedule_name = ?
        """
            .formatted(schema);

    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, name);
      ps.executeUpdate();
    }
  }

  void applySchedules(List<WorkflowSchedule> schedules) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      conn.setAutoCommit(false);
      try {
        for (WorkflowSchedule schedule : schedules) {
          deleteSchedule(conn, schema, schedule.scheduleName());
          createSchedule(conn, schema, serializer, schedule);
        }
        conn.commit();
      } catch (SQLException e) {
        conn.rollback();
        throw e;
      } finally {
        conn.setAutoCommit(true);
      }
    }
  }

  private static WorkflowSchedule rowToSchedule(ResultSet rs, DBOSSerializer serializer)
      throws SQLException {
    // https://github.com/dbos-inc/dbos-transact-java/issues/330
    // tracking portable serialization support
    Object context =
        SerializationUtil.deserializeValue(rs.getString(7), serializer.name(), serializer);
    String lastFiredAtStr = rs.getString(8);
    String timeZoneStr = rs.getString(10);

    return new WorkflowSchedule(
        rs.getString(1),
        rs.getString(2),
        rs.getString(3),
        rs.getString(4),
        rs.getString(5),
        ScheduleStatus.valueOf(rs.getString(6)),
        context,
        lastFiredAtStr != null ? Instant.parse(lastFiredAtStr) : null,
        rs.getBoolean(9),
        timeZoneStr != null ? ZoneId.of(timeZoneStr) : null,
        rs.getString(11));
  }
}
