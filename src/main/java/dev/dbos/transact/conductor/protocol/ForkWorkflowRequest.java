package dev.dbos.transact.conductor.protocol;

public class ForkWorkflowRequest extends BaseMessage {
    public ForkWorkflowBody body;

    public ForkWorkflowRequest() {
    }

    public ForkWorkflowRequest(String requestId, String workflowId, int startStep, String appVer,
            String newWorkflowId) {
        this.type = MessageType.FORK_WORKFLOW.getValue();
        this.request_id = requestId;
        this.body = new ForkWorkflowBody();
        this.body.workflow_id = workflowId;
        this.body.start_step = startStep;
        this.body.application_version = appVer;
        this.body.new_workflow_id = newWorkflowId;
    }

    public ForkWorkflowRequest(String requestId, String workflowId, int startStep) {
        this(requestId, workflowId, startStep, null, null);
    }

    public static class ForkWorkflowBody {
        public String workflow_id;
        public Integer start_step;
        public String application_version; // optional
        public String new_workflow_id; // optional
    }
}
