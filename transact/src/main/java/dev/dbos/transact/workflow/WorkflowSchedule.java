package dev.dbos.transact.workflow;

public record WorkflowSchedule(
    String scheduleId,
    String scheduleName,
    String workflowName,
    String workflowClassName,
    String schedule,
    String status,
    String context,
    String lastFiredAt,
    boolean automaticBackfill,
    String cronTimezone,
    String queueName) {}
