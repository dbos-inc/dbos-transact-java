package dev.dbos.transact.conductor.protocol;

public enum MessageType {
  ALERT("alert"),
  CANCEL("cancel"),
  DELETE("delete"),
  EXECUTOR_INFO("executor_info"),
  EXIST_PENDING_WORKFLOWS("exist_pending_workflows"),
  EXPORT_WORKFLOW("export_workflow"),
  FORK_WORKFLOW("fork_workflow"),
  GET_METRICS("get_metrics"),
  GET_WORKFLOW("get_workflow"),
  IMPORT_WORKFLOW("import_workflow"),
  LIST_QUEUED_WORKFLOWS("list_queued_workflows"),
  LIST_STEPS("list_steps"),
  LIST_WORKFLOWS("list_workflows"),
  RECOVERY("recovery"),
  RESTART("restart"),
  RESUME("resume"),
  RETENTION("retention");

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
