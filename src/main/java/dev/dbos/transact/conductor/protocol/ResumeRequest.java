package dev.dbos.transact.conductor.protocol;

public class ResumeRequest extends BaseMessage {
    public String workflow_id;

    public ResumeRequest() {
    }

    public ResumeRequest(String requestId, String workflowId) {
        this.type = MessageType.RESUME.getValue();
        this.request_id = requestId;
        this.workflow_id = workflowId;
    }

}
