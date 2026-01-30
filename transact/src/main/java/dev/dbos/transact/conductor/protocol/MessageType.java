package dev.dbos.transact.conductor.protocol;

public enum MessageType {
  EXECUTOR_INFO("executor_info"),
  RECOVERY("recovery"),
  CANCEL("cancel"),
  DELETE("delete"),
  LIST_WORKFLOWS("list_workflows"),
  LIST_QUEUED_WORKFLOWS("list_queued_workflows"),
  RESUME("resume"),
  RESTART("restart"),
  GET_WORKFLOW("get_workflow"),
  EXIST_PENDING_WORKFLOWS("exist_pending_workflows"),
  LIST_STEPS("list_steps"),
  FORK_WORKFLOW("fork_workflow"),
  RETENTION("retention"),
  GET_METRICS("get_metrics");

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
