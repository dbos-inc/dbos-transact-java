package dev.dbos.transact.conductor.protocol;

import java.util.List;

public class ResumeRequest extends BaseMessage {
  public String workflow_id;
  public List<String> workflow_ids;
  public String queue_name;

  public ResumeRequest() {}

  public ResumeRequest(String requestId, String workflowId) {
    this.type = MessageType.RESUME.getValue();
    this.request_id = requestId;
    this.workflow_id = workflowId;
  }
}
