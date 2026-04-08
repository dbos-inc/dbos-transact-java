package dev.dbos.transact.conductor.protocol;

public enum MessageType {
  ALERT("alert"),
  BACKFILL_SCHEDULE("backfill_schedule"),
  CANCEL("cancel"),
  DELETE("delete"),
  EXECUTOR_INFO("executor_info"),
  EXIST_PENDING_WORKFLOWS("exist_pending_workflows"),
  EXPORT_WORKFLOW("export_workflow"),
  FORK_WORKFLOW("fork_workflow"),
  GET_METRICS("get_metrics"),
  GET_SCHEDULE("get_schedule"),
  GET_WORKFLOW_AGGREGATES("get_workflow_aggregates"),
  GET_WORKFLOW_EVENTS("get_workflow_events"),
  GET_WORKFLOW_NOTIFICATIONS("get_workflow_notifications"),
  GET_WORKFLOW_STREAMS("get_workflow_streams"),
  GET_WORKFLOW("get_workflow"),
  IMPORT_WORKFLOW("import_workflow"),
  LIST_APPLICATION_VERSIONS("list_application_versions"),
  LIST_QUEUED_WORKFLOWS("list_queued_workflows"),
  LIST_SCHEDULES("list_schedules"),
  LIST_STEPS("list_steps"),
  LIST_WORKFLOWS("list_workflows"),
  PAUSE_SCHEDULE("pause_schedule"),
  RECOVERY("recovery"),
  RESTART("restart"),
  RESUME("resume"),
  RESUME_SCHEDULE("resume_schedule"),
  RETENTION("retention"),
  SET_LATEST_APPLICATION_VERSION("set_latest_application_version"),
  TRIGGER_SCHEDULE("trigger_schedule");

  private final String value;

  MessageType(String value) {
    this.value = value;
  }

  public String getValue() {
    return value;
  }

  // Optional: Lookup from string
  public static MessageType fromValue(String value) {
    for (MessageType type : values()) {
      if (type.value.equals(value)) {
        return type;
      }
    }
    throw new IllegalArgumentException("Unknown message type: " + value);
  }

  @Override
  public String toString() {
    return value;
  }
}
