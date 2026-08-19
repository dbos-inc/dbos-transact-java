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
    @Nullable String queueName,
    /**
     * The application that owns this schedule and runs its workflows, as recorded in the system
     * database, or null if the schedule is unclaimed and so is run by every application sharing it.
     * Set by the database when a schedule is read back; ignored when creating one, which always
     * records the creating application.
     */
    @Nullable String applicationName) {

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
        queueName,
        applicationName);
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
        queueName,
        applicationName);
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
        queueName,
        applicationName);
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
        queueName,
        applicationName);
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
        queueName,
        applicationName);
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
        queueName,
        applicationName);
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
        queueName,
        applicationName);
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
        queueName,
        applicationName);
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
        queueName,
        applicationName);
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
        queueName,
        applicationName);
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
        value,
        applicationName);
  }
}
