package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.RewindOptions;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

public class RewindWorkflowRequest extends BaseMessage {
  public RewindWorkflowBody body;

  public RewindWorkflowRequest() {}

  public RewindWorkflowRequest(String requestId, String workflowId, Integer startStep) {
    this.type = MessageType.REWIND_WORKFLOW.getValue();
    this.request_id = requestId;
    this.body = new RewindWorkflowBody();
    this.body.workflow_id = workflowId;
    this.body.start_step = startStep;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class RewindWorkflowBody {
    public String workflow_id;
    public Integer start_step; // optional: omitted rewinds the whole history
    public String application_version; // optional
    public String queue_name; // optional
    public String queue_partition_key; // optional
  }

  public int startStep() {
    return body.start_step == null ? 0 : body.start_step;
  }

  public RewindOptions toOptions() {
    return new RewindOptions(body.application_version, body.queue_name, body.queue_partition_key);
  }
}
