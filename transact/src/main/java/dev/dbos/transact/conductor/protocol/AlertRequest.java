package dev.dbos.transact.conductor.protocol;

import java.util.Map;

public class AlertRequest extends BaseMessage {
  public String name;
  public String message;
  public Map<String, String> metadata;

  public AlertRequest() {}

  public AlertRequest(String requestId, String name, String message, Map<String, String> metadata) {
    this.type = MessageType.ALERT.getValue();
    this.request_id = requestId;
    this.name = name;
    this.message = message;
    this.metadata = metadata;
  }
}
