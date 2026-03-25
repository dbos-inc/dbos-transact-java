package dev.dbos.transact.conductor.protocol;

import java.util.List;

public class CancelRequest extends BaseMessage {
  public String workflow_id;
  public List<String> workflow_ids;

  public CancelRequest() {}

  public CancelRequest(String requestId, String workflowId) {
    this.type = MessageType.CANCEL.getValue();
    this.request_id = requestId;
    this.workflow_id = workflowId;
  }
}
