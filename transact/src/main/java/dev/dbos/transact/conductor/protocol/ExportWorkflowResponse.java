package dev.dbos.transact.conductor.protocol;

public class ExportWorkflowResponse extends BaseResponse {
  public String serialized_workflow; // optional

  public ExportWorkflowResponse(BaseMessage message, String serializedWorkflow) {
    super(MessageType.EXPORT_WORKFLOW.getValue(), message.request_id);
    this.serialized_workflow = serializedWorkflow;
  }

  public ExportWorkflowResponse(BaseMessage message, Exception ex) {
    super(message.type, message.request_id, ex.getMessage());
  }
}
