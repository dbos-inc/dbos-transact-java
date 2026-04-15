package dev.dbos.transact.conductor.protocol;

public class ExportWorkflowRequest extends BaseMessage {
  public String workflow_id;
  public boolean export_children;

  public ExportWorkflowRequest() {}

  public ExportWorkflowRequest(String requestId, String workflowId, boolean exportChildren) {
    this.type = MessageType.EXPORT_WORKFLOW.getValue();
    this.request_id = requestId;
    this.workflow_id = workflowId;
    this.export_children = exportChildren;
  }
}
