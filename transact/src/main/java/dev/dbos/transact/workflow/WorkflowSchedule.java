package dev.dbos.transact.workflow;

import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.NonNull;

public record WorkflowSchedule(
    String scheduleId,
    String scheduleName,
    String workflowName,
    String workflowClassName,
    String schedule,
    ScheduleStatus status,
    Object context,
    Instant lastFiredAt,
    boolean automaticBackfill,
    String cronTimezone, // IANA timezone name, stored as string in DB
    String queueName) {

  public WorkflowSchedule withScheduleId(@NonNull String value) {
    return new WorkflowSchedule(
        Objects.requireNonNull(value),
        scheduleName,
        workflowName,
        workflowClassName,
        schedule,
        status,
        context,
        lastFiredAt,
        automaticBackfill,
        cronTimezone,
        queueName);
  }

  public WorkflowSchedule withLastFiredAt(Instant value) {
    return new WorkflowSchedule(
        scheduleId,
        scheduleName,
        workflowName,
        workflowClassName,
        schedule,
        status,
        context,
        value,
        automaticBackfill,
        cronTimezone,
        queueName);
  }

  public WorkflowSchedule withStatus(ScheduleStatus value) {
    return new WorkflowSchedule(
        scheduleId,
        scheduleName,
        workflowName,
        workflowClassName,
        schedule,
        value,
        context,
        lastFiredAt,
        automaticBackfill,
        cronTimezone,
        queueName);
  }
}
