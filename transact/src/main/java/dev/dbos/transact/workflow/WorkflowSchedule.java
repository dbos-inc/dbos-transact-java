package dev.dbos.transact.workflow;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;

import org.jspecify.annotations.NonNull;

public record WorkflowSchedule(
    String scheduleId,
    String name,
    String workflowName,
    String className,
    String schedule,
    ScheduleStatus status,
    Object context,
    Instant lastFiredAt,
    boolean automaticBackfill,
    ZoneId cronTimezone,
    String queueName) {

  public boolean isActive() {
    return status == ScheduleStatus.ACTIVE;
  }

  public WorkflowSchedule withScheduleId(@NonNull String value) {
    return new WorkflowSchedule(
        Objects.requireNonNull(value),
        name,
        workflowName,
        className,
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
        name,
        workflowName,
        className,
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
        name,
        workflowName,
        className,
        schedule,
        value,
        context,
        lastFiredAt,
        automaticBackfill,
        cronTimezone,
        queueName);
  }
}
