package dev.dbos.transact.workflow;

public record WorkflowSchedule(
    String scheduleId,
    String scheduleName,
    String workflowName,
    String workflowClassName,
    String schedule,
    ScheduleStatus status,
    String context,
    String lastFiredAt,
    boolean automaticBackfill,
    String cronTimezone,
    String queueName) {

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
