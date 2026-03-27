package dev.dbos.transact.workflow;

import java.time.Instant;

public record WorkflowSchedule(
    String scheduleId,
    String scheduleName,
    String workflowName,
    String workflowClassName, // nullable
    String schedule,
    ScheduleStatus status,
    String context, // TODO: context needs to be object like WF inputs
    Instant lastFiredAt, // nullable
    boolean automaticBackfill,
    String cronTimezone, // nullable, IANA timezone name, stored as string in DB
    String queueName) { // nullable

  public WorkflowSchedule withScheduleId(String id) {
    return new WorkflowSchedule(
        id,
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
}
