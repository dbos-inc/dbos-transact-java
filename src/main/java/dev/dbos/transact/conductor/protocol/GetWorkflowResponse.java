package dev.dbos.transact.conductor.protocol;

public class GetWorkflowResponse extends BaseResponse {
    public WorkflowsOutput output; // optional

    public GetWorkflowResponse() {
    }

    public GetWorkflowResponse(BaseMessage message, WorkflowsOutput output) {
        super(message.type, message.request_id);
        this.output = output;
    }

    public GetWorkflowResponse(BaseMessage message, Exception exception) {
        super(message.type, message.request_id, exception.getMessage());
    }

}
