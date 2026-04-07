package dev.dbos.transact.conductor.protocol;

import dev.dbos.transact.workflow.ListWorkflowsInput;

import java.util.List;

public class GetWorkflowRequest extends BaseMessage {
  public String workflow_id;
  public boolean load_input = true;
  public boolean load_output = true;

  public GetWorkflowRequest() {}

  public GetWorkflowRequest(String requestId, String workflowId) {
    this.type = MessageType.GET_WORKFLOW.getValue();
    this.request_id = requestId;
    this.workflow_id = workflowId;
  }

  public ListWorkflowsInput toInput() {
    return new ListWorkflowsInput()
        .withWorkflowIds(List.of(workflow_id))
        .withLoadInput(load_input)
        .withLoadOutput(load_output);
  }
}
