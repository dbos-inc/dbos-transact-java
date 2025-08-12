package dev.dbos.transact.conductor.protocol;

public class ListStepsRequest extends BaseMessage {
    public String workflow_id;

    public ListStepsRequest() {
    }

    public ListStepsRequest(String requestId, String workflowId) {
        this.type = MessageType.LIST_STEPS.getValue();
        this.request_id = requestId;
        this.workflow_id = workflowId;
    }
}
