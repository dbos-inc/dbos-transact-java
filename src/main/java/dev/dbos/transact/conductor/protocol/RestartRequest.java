package dev.dbos.transact.conductor.protocol;

public class RestartRequest extends BaseMessage {
  public String workflow_id;

  public RestartRequest() {}

  public RestartRequest(String requestId, String workflowId) {
    this.type = MessageType.RESTART.getValue();
    this.request_id = requestId;
    this.workflow_id = workflowId;
  }
}
