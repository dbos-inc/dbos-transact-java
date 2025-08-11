package dev.dbos.transact.conductor.protocol;

public class ForkWorkflowResponse extends BaseResponse {
    public String new_workflow_id; // optional

    public ForkWorkflowResponse() {}

    public ForkWorkflowResponse(BaseMessage message, String new_workflow_id, String errorMessage) {
        super(message.type, message.request_id, errorMessage);
        this.new_workflow_id = new_workflow_id;
    }

    public ForkWorkflowResponse(BaseMessage message, String new_workflow_id) {
        this(message, new_workflow_id, null);
    }

    public ForkWorkflowResponse(BaseMessage message, Exception ex) {
        this(message, null, ex.getMessage());
    }

}
