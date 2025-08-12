package dev.dbos.transact.conductor.protocol;

public class GetWorkflowRequest extends BaseMessage {
    public String workflow_id;

    public GetWorkflowRequest() {
    }

    public GetWorkflowRequest(String requestId, String workflowId) {
        this.type = MessageType.GET_WORKFLOW.getValue();
        this.request_id = requestId;
        this.workflow_id = workflowId;
    }
}
