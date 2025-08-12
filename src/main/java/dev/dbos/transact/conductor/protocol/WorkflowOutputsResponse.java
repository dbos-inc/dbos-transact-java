package dev.dbos.transact.conductor.protocol;

import java.util.Collections;
import java.util.List;

public class WorkflowOutputsResponse extends BaseResponse {
    public List<WorkflowsOutput> output;

    public WorkflowOutputsResponse() {
    }

    public WorkflowOutputsResponse(BaseMessage message, List<WorkflowsOutput> output) {
        super(message.type, message.request_id);
        this.output = output;
    }

    public WorkflowOutputsResponse(BaseMessage message, Exception ex) {
        super(message.type, message.request_id, ex.getMessage());
        this.output = Collections.emptyList();
    }
}
