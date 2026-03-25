package dev.dbos.transact.conductor.protocol;

import java.util.List;

public class DeleteRequest extends BaseMessage {
  public String workflow_id;
  public List<String> workflow_ids;
  public boolean delete_children;

  public DeleteRequest() {}

  public DeleteRequest(String requestId, String workflowId, boolean deleteChildren) {
    this.type = MessageType.DELETE.getValue();
    this.request_id = requestId;
    this.workflow_id = workflowId;
    this.delete_children = deleteChildren;
  }
}
