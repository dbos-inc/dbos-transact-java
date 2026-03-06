package dev.dbos.transact.conductor.protocol;

public class ImportWorkflowRequest extends BaseMessage {
  public String serialized_workflow;

  public ImportWorkflowRequest() {}

  public ImportWorkflowRequest(String requestId, String serializedWorkflow) {
    this.type = MessageType.IMPORT_WORKFLOW.getValue();
    this.request_id = requestId;
    this.serialized_workflow = serializedWorkflow;
  }
}
