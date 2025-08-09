package dev.dbos.transact.conductor.protocol;

public class CancelRequest extends BaseMessage {
    public String workflow_id;

    public CancelRequest() {
    }

    public CancelRequest(String requestId, String workflowId) {
        this.type = MessageType.CANCEL.getValue();
        this.request_id = requestId;
        this.workflow_id = workflowId;
    }
}
