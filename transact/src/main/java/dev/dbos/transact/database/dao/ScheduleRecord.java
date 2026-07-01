package dev.dbos.transact.database.dao;

import dev.dbos.transact.json.DBOSSerializer;
import dev.dbos.transact.json.SerializationUtil;
import dev.dbos.transact.workflow.ScheduleStatus;
import dev.dbos.transact.workflow.WorkflowSchedule;

import java.time.Instant;
import java.time.ZoneId;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

// Raw form of a schedule row as read from the database: context is kept as its serialized string
// rather than deserialized. This lets callers that only need to detect definition changes (the
// scheduler's poller) compare schedules without deserializing, and without depending on the
// equals() of whatever type the deserialized context happens to be. Callers that need the actual
// context object convert via toWorkflowSchedule().
public record ScheduleRecord(
    @Nullable String id,
    @NonNull String scheduleName,
    @NonNull String workflowName,
    @NonNull String className,
    @NonNull String cron,
    @NonNull ScheduleStatus status,
    @Nullable String context,
    @Nullable Instant lastFiredAt,
    boolean automaticBackfill,
    @Nullable ZoneId cronTimezone,
    @Nullable String queueName) {

  public boolean isActive() {
    return status == ScheduleStatus.ACTIVE;
  }

  public WorkflowSchedule toWorkflowSchedule(@Nullable DBOSSerializer serializer) {
    Object deserializedContext =
        SerializationUtil.deserializeValue(
            context, serializer != null ? serializer.name() : null, serializer);
    return new WorkflowSchedule(
        id,
        scheduleName,
        workflowName,
        className,
        cron,
        status,
        deserializedContext,
        lastFiredAt,
        automaticBackfill,
        cronTimezone,
        queueName);
  }
}
