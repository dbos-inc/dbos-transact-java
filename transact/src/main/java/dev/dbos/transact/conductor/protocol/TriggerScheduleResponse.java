package dev.dbos.transact.conductor.protocol;

public class TriggerScheduleResponse extends BaseResponse {
  public String workflow_id;

  public TriggerScheduleResponse() {}

  public TriggerScheduleResponse(BaseMessage message, String workflowId) {
    super(message.type, message.request_id);
    this.workflow_id = workflowId;
  }

  public TriggerScheduleResponse(BaseMessage message, String errorMessage, boolean dummy) {
    super(message.type, message.request_id, errorMessage);
    this.workflow_id = null;
  }
}
