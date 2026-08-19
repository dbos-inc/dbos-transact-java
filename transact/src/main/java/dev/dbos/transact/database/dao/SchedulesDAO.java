package dev.dbos.transact.database.dao;

import dev.dbos.transact.database.DbContext;
import dev.dbos.transact.execution.SchedulerService;
import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.WorkflowSchedule;

import java.sql.Array;
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

import org.jspecify.annotations.Nullable;

public class SchedulesDAO {

  private SchedulesDAO() {}

  public static void createSchedule(DbContext ctx, WorkflowSchedule schedule) throws SQLException {
    try (Connection conn = ctx.getConnection()) {
      createSchedule(conn, ctx.schema(), ctx.serializer(), schedule, ctx.appName());
    }
  }

  static void createSchedule(
      Connection conn,
      String schema,
      DBOSSerializer serializer,
      WorkflowSchedule schedule,
      @Nullable String appName)
      throws SQLException {

    Objects.requireNonNull(schedule, "schedule must not be null");
    Objects.requireNonNull(schedule.scheduleName(), "scheduleName must not be null");
    Objects.requireNonNull(schedule.workflowName(), "workflowName must not be null");
    // Note, class name may be null since we may be creating portable schedules in a different
    // language
    Objects.requireNonNull(schedule.status(), "status must not be null");
    Objects.requireNonNull(schedule.cron(), "cron must not be null");
    SchedulerService.CRON_PARSER.parse(schedule.cron()).validate();

    var owner =
        RowOwner.resolve(
            conn,
            schema,
            "workflow_schedules",
            "schedule_name",
            schedule.scheduleName(),
            appName,
            "Schedule");

    String sql =
        """
        INSERT INTO "%s".workflow_schedules
            (schedule_id, schedule_name, workflow_name, workflow_class_name,
             schedule, status, context, last_fired_at, automatic_backfill,
             cron_timezone, queue_name, application_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
            .formatted(schema);

    var serializedContext =
        SerializationUtil.serializeValue(
            schedule.context(), serializer != null ? serializer.name() : null, serializer);

    var timeZone = schedule.cronTimezone() == null ? null : schedule.cronTimezone().getId();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, schedule.id() != null ? schedule.id() : UUID.randomUUID().toString());
      ps.setString(2, schedule.scheduleName());
      ps.setString(3, schedule.workflowName());
      ps.setString(4, schedule.className());
      ps.setString(5, schedule.cron());
      ps.setString(6, schedule.status().name());
      ps.setString(7, serializedContext.serializedValue());
      ps.setString(8, schedule.lastFiredAt() != null ? schedule.lastFiredAt().toString() : null);
      ps.setBoolean(9, schedule.automaticBackfill());
      ps.setString(10, timeZone);
      ps.setString(11, schedule.queueName());
      ps.setString(12, owner);
      ps.executeUpdate();
    } catch (SQLException e) {
      if ("23505".equals(e.getSQLState())) {
        throw new RuntimeException(
            "Schedule '%s' already exists".formatted(schedule.scheduleName()), e);
      }
      throw e;
    }
  }

  public static List<WorkflowSchedule> listSchedules(
      DbContext ctx,
      List<ScheduleStatus> statuses,
      List<String> workflowNames,
      List<String> scheduleNamePrefixes)
      throws SQLException {
    return listSchedules(ctx, statuses, workflowNames, scheduleNamePrefixes, null);
  }

  /**
   * Lists schedules owned by {@code applicationName}, plus unclaimed ones. Null lists this
   * application's own; an explicitly empty list covers every application's.
   */
  public static List<WorkflowSchedule> listSchedules(
      DbContext ctx,
      List<ScheduleStatus> statuses,
      List<String> workflowNames,
      List<String> scheduleNamePrefixes,
      @Nullable List<String> applicationName)
      throws SQLException {
    return listScheduleRecords(ctx, statuses, workflowNames, scheduleNamePrefixes, applicationName)
        .stream()
        .map(r -> r.toWorkflowSchedule(ctx.serializer()))
        .toList();
  }

  // Raw form of listSchedules: keeps context as its serialized string instead of deserializing it.
  // Used by the scheduler's poller to detect definition changes without relying on the equals() of
  // whatever type the deserialized context happens to be.
  public static List<ScheduleRecord> listScheduleRecords(
      DbContext ctx,
      List<ScheduleStatus> statuses,
      List<String> workflowNames,
      List<String> scheduleNamePrefixes)
      throws SQLException {
    return listScheduleRecords(ctx, statuses, workflowNames, scheduleNamePrefixes, null);
  }

  public static List<ScheduleRecord> listScheduleRecords(
      DbContext ctx,
      List<ScheduleStatus> statuses,
      List<String> workflowNames,
      List<String> scheduleNamePrefixes,
      @Nullable List<String> applicationName)
      throws SQLException {

    StringBuilder sql =
        new StringBuilder(
            """
            SELECT schedule_id, schedule_name, workflow_name, workflow_class_name,
                   schedule, status, context, last_fired_at, automatic_backfill,
                   cron_timezone, queue_name, application_name
            FROM "%s".workflow_schedules
            WHERE TRUE
            """
                .formatted(ctx.schema()));

    List<Object> params = new ArrayList<>();

    var appNames = ctx.scopeNames(applicationName);
    if (appNames != null) {
      sql.append(" AND (application_name = ANY(?) OR application_name IS NULL)");
      params.add(appNames.toArray(String[]::new));
    }

    if (statuses != null && !statuses.isEmpty()) {
      sql.append(" AND status = ANY(?)");
      params.add(statuses.stream().map(ScheduleStatus::name).toArray(String[]::new));
    }
    if (workflowNames != null && !workflowNames.isEmpty()) {
      sql.append(" AND workflow_name = ANY(?)");
      params.add(workflowNames.toArray(String[]::new));
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

    try (Connection conn = ctx.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql.toString())) {
      List<Array> arrays = new ArrayList<>();
      try {
        int idx = 1;
        for (Object param : params) {
          if (param instanceof String[] arr) {
            Array sqlArray = conn.createArrayOf("text", arr);
            arrays.add(sqlArray);
            ps.setArray(idx++, sqlArray);
          } else {
            ps.setString(idx++, (String) param);
          }
        }
        try (ResultSet rs = ps.executeQuery()) {
          List<ScheduleRecord> results = new ArrayList<>();
          while (rs.next()) {
            results.add(rowToScheduleRecord(rs));
          }
          return results;
        }
      } finally {
        for (Array array : arrays) {
          array.free();
        }
      }
    }
  }

  public static Optional<WorkflowSchedule> getSchedule(DbContext ctx, String name)
      throws SQLException {
    return getScheduleRecord(ctx, name).map(r -> r.toWorkflowSchedule(ctx.serializer()));
  }

  // Raw form of getSchedule: keeps context as its serialized string instead of deserializing it.
  public static Optional<ScheduleRecord> getScheduleRecord(DbContext ctx, String name)
      throws SQLException {
    String sql =
        """
        SELECT schedule_id, schedule_name, workflow_name, workflow_class_name,
               schedule, status, context, last_fired_at, automatic_backfill,
               cron_timezone, queue_name, application_name
        FROM "%s".workflow_schedules
        WHERE schedule_name = ?
        """
            .formatted(ctx.schema());

    try (Connection conn = ctx.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, name);
      try (ResultSet rs = ps.executeQuery()) {
        if (rs.next()) {
          return Optional.of(rowToScheduleRecord(rs));
        }
        return Optional.empty();
      }
    }
  }

  public static void pauseSchedule(DbContext ctx, String name) throws SQLException {
    setScheduleStatus(ctx, name, ScheduleStatus.PAUSED);
  }

  public static void resumeSchedule(DbContext ctx, String name) throws SQLException {
    setScheduleStatus(ctx, name, ScheduleStatus.ACTIVE);
  }

  private static void setScheduleStatus(DbContext ctx, String name, ScheduleStatus status)
      throws SQLException {
    String sql =
        """
        UPDATE "%s".workflow_schedules SET status = ? WHERE schedule_name = ?
        """
            .formatted(ctx.schema());

    try (Connection conn = ctx.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, status.name());
      ps.setString(2, name);
      ps.executeUpdate();
    }
  }

  public static void updateScheduleLastFiredAt(DbContext ctx, String name, Instant lastFiredAt)
      throws SQLException {
    String sql =
        """
        UPDATE "%s".workflow_schedules SET last_fired_at = ? WHERE schedule_name = ?
        """
            .formatted(ctx.schema());

    try (Connection conn = ctx.getConnection();
        PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, lastFiredAt != null ? lastFiredAt.toString() : null);
      ps.setString(2, name);
      ps.executeUpdate();
    }
  }

  public static void deleteSchedule(DbContext ctx, String name) throws SQLException {
    try (var conn = ctx.getConnection()) {
      deleteSchedule(conn, ctx.schema(), name);
    }
  }

  static void deleteSchedule(Connection conn, String schema, String name) throws SQLException {
    String sql =
        """
        DELETE FROM "%s".workflow_schedules WHERE schedule_name = ?
        """
            .formatted(schema);

    try (var stmt = conn.prepareStatement(sql)) {
      stmt.setString(1, name);
      stmt.executeUpdate();
    }
  }

  public static void applySchedules(DbContext ctx, List<WorkflowSchedule> schedules)
      throws SQLException {
    try (var conn = ctx.getConnection()) {
      conn.setAutoCommit(false);
      try {
        for (WorkflowSchedule schedule : schedules) {
          upsertSchedule(
              conn,
              ctx.schema(),
              ctx.serializer(),
              schedule
                  .withScheduleId(UUID.randomUUID().toString())
                  .withStatus(ScheduleStatus.ACTIVE)
                  .withLastFiredAt(null),
              ctx.appName());
        }
        conn.commit();
      } catch (SQLException | RuntimeException e) {
        // A name owned by another application throws DBOSApplicationNameConflictException, and an
        // invalid cron throws too; both must roll back the schedules already written above.
        conn.rollback();
        throw e;
      } finally {
        conn.setAutoCommit(true);
      }
    }
  }

  // Idempotent upsert by schedule_name, so concurrent applySchedules calls for the same name
  // can't race each other into a duplicate-key error. On conflict, schedule_id, status, and
  // last_fired_at are preserved from the existing row; the poller detects the changed
  // definition and restarts the schedule's future.
  private static void upsertSchedule(
      Connection conn,
      String schema,
      DBOSSerializer serializer,
      WorkflowSchedule schedule,
      @Nullable String appName)
      throws SQLException {

    SchedulerService.CRON_PARSER.parse(schedule.cron()).validate();

    var owner =
        RowOwner.resolve(
            conn,
            schema,
            "workflow_schedules",
            "schedule_name",
            schedule.scheduleName(),
            appName,
            "Schedule");

    String sql =
        """
        INSERT INTO "%s".workflow_schedules
            (schedule_id, schedule_name, workflow_name, workflow_class_name,
             schedule, status, context, last_fired_at, automatic_backfill,
             cron_timezone, queue_name, application_name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (schedule_name) DO UPDATE SET
            workflow_name = EXCLUDED.workflow_name,
            workflow_class_name = EXCLUDED.workflow_class_name,
            schedule = EXCLUDED.schedule,
            context = EXCLUDED.context,
            automatic_backfill = EXCLUDED.automatic_backfill,
            cron_timezone = EXCLUDED.cron_timezone,
            queue_name = EXCLUDED.queue_name,
            -- Claim only an unclaimed row, so a registration landing between the ownership
            -- check above and this write keeps the name it just took.
            application_name = COALESCE(workflow_schedules.application_name, EXCLUDED.application_name)
        """
            .formatted(schema);

    var serializedContext =
        SerializationUtil.serializeValue(
            schedule.context(), serializer != null ? serializer.name() : null, serializer);

    var timeZone = schedule.cronTimezone() == null ? null : schedule.cronTimezone().getId();
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      ps.setString(1, schedule.id() != null ? schedule.id() : UUID.randomUUID().toString());
      ps.setString(2, schedule.scheduleName());
      ps.setString(3, schedule.workflowName());
      ps.setString(4, schedule.className());
      ps.setString(5, schedule.cron());
      ps.setString(6, schedule.status().name());
      ps.setString(7, serializedContext.serializedValue());
      ps.setString(8, schedule.lastFiredAt() != null ? schedule.lastFiredAt().toString() : null);
      ps.setBoolean(9, schedule.automaticBackfill());
      ps.setString(10, timeZone);
      ps.setString(11, schedule.queueName());
      ps.setString(12, owner);
      ps.executeUpdate();
    }
  }

  private static ScheduleRecord rowToScheduleRecord(ResultSet rs) throws SQLException {
    String lastFiredAtStr = rs.getString(8);
    String timeZoneStr = rs.getString(10);

    return new ScheduleRecord(
        rs.getString(1),
        rs.getString(2),
        rs.getString(3),
        rs.getString(4),
        rs.getString(5),
        ScheduleStatus.valueOf(rs.getString(6)),
        rs.getString(7),
        lastFiredAtStr != null ? Instant.parse(lastFiredAtStr) : null,
        rs.getBoolean(9),
        timeZoneStr != null ? ZoneId.of(timeZoneStr) : null,
        rs.getString(11),
        rs.getString(12));
  }
}
