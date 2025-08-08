package dev.dbos.transact.conductor.protocol;

public class ForkWorkflowRequest extends BaseMessage {
    public ForkWorkflowBody body;

    public static class ForkWorkflowBody {
        public String workflow_id;
        public Integer start_step;
        public String application_version; // optional
        public String new_workflow_id; // optional
    }
}
