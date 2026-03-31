package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.WorkflowSchedule;

public record ScheduleOutput(
    String schedule_id,
    String schedule_name,
    String workflow_name,
    String workflow_class_name,
    String schedule,
    String status,
    String context,
    String last_fired_at,
    boolean automatic_backfill,
    String cron_timezone,
    String queue_name) {

  public static ScheduleOutput from(WorkflowSchedule s, boolean loadContext) {
    return new ScheduleOutput(
        s.id(),
        s.scheduleName(),
        s.workflowName(),
        s.className(),
        s.cron(),
        s.status().name(),
        loadContext && s.context() != null ? JSONUtil.toJson(s.context()) : null,
        s.lastFiredAt() != null ? s.lastFiredAt().toString() : null,
        s.automaticBackfill(),
        s.cronTimezone() != null ? s.cronTimezone().getId() : null,
        s.queueName());
  }
}
