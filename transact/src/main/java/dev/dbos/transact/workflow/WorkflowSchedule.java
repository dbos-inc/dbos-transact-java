package dev.dbos.transact.workflow;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;

import org.jspecify.annotations.NonNull;

public record WorkflowSchedule(
    String id,
    String scheduleName,
    String workflowName,
    String className,
    String cron,
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
        scheduleName,
        workflowName,
        className,
        cron,
        status,
        context,
        lastFiredAt,
        automaticBackfill,
        cronTimezone,
        queueName);
  }

  public WorkflowSchedule withLastFiredAt(Instant value) {
    return new WorkflowSchedule(
        id,
        scheduleName,
        workflowName,
        className,
        cron,
        status,
        context,
        value,
        automaticBackfill,
        cronTimezone,
        queueName);
  }

  public WorkflowSchedule withStatus(ScheduleStatus value) {
    return new WorkflowSchedule(
        id,
        scheduleName,
        workflowName,
        className,
        cron,
        value,
        context,
        lastFiredAt,
        automaticBackfill,
        cronTimezone,
        queueName);
  }
}
