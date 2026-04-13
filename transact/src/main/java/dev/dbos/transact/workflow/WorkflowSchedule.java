package dev.dbos.transact.workflow;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Objects;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

public record WorkflowSchedule(
    @Nullable String id,
    @NonNull String scheduleName,
    @NonNull String workflowName,
    @NonNull String className,
    @NonNull String cron,
    @NonNull ScheduleStatus status,
    @Nullable Object context,
    @Nullable Instant lastFiredAt,
    boolean automaticBackfill,
    @Nullable ZoneId cronTimezone,
    @Nullable String queueName) {

  public WorkflowSchedule {
    Objects.requireNonNull(scheduleName, "scheduleName must not be null");
    Objects.requireNonNull(workflowName, "workflowName must not be null");
    // Note, class name is required in java but not all other DBOS languages so we don't validate
    // not null here
    Objects.requireNonNull(cron, "cron must not be null");
    Objects.requireNonNull(status, "status must not be null");
  }

  public WorkflowSchedule(
      @NonNull String scheduleName,
      @NonNull String workflowName,
      @Nullable String className,
      @NonNull String cron) {
    this(
        null,
        scheduleName,
        workflowName,
        className,
        cron,
        ScheduleStatus.ACTIVE,
        null,
        null,
        false,
        null,
        null);
  }

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

  public WorkflowSchedule withScheduleName(@NonNull String value) {
    return new WorkflowSchedule(
        id,
        Objects.requireNonNull(value),
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

  public WorkflowSchedule withWorkflowName(@NonNull String value) {
    return new WorkflowSchedule(
        id,
        scheduleName,
        Objects.requireNonNull(value),
        className,
        cron,
        status,
        context,
        lastFiredAt,
        automaticBackfill,
        cronTimezone,
        queueName);
  }

  public WorkflowSchedule withClassName(@NonNull String value) {
    return new WorkflowSchedule(
        id,
        scheduleName,
        workflowName,
        Objects.requireNonNull(value),
        cron,
        status,
        context,
        lastFiredAt,
        automaticBackfill,
        cronTimezone,
        queueName);
  }

  public WorkflowSchedule withCron(@NonNull String value) {
    return new WorkflowSchedule(
        id,
        scheduleName,
        workflowName,
        className,
        Objects.requireNonNull(value),
        status,
        context,
        lastFiredAt,
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

  public WorkflowSchedule withContext(@Nullable Object value) {
    return new WorkflowSchedule(
        id,
        scheduleName,
        workflowName,
        className,
        cron,
        status,
        value,
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

  public WorkflowSchedule withAutomaticBackfill(boolean value) {
    return new WorkflowSchedule(
        id,
        scheduleName,
        workflowName,
        className,
        cron,
        status,
        context,
        lastFiredAt,
        value,
        cronTimezone,
        queueName);
  }

  public WorkflowSchedule withCronTimezone(@Nullable ZoneId value) {
    return new WorkflowSchedule(
        id,
        scheduleName,
        workflowName,
        className,
        cron,
        status,
        context,
        lastFiredAt,
        automaticBackfill,
        value,
        queueName);
  }

  public WorkflowSchedule withQueueName(@Nullable String value) {
    return new WorkflowSchedule(
        id,
        scheduleName,
        workflowName,
        className,
        cron,
        status,
        context,
        lastFiredAt,
        automaticBackfill,
        cronTimezone,
        value);
  }
}
