package dev.dbos.transact.workflow.internal;

public class GetPendingWorkflowsOutput {
    private final String workflowUuid;
    private final String queueName;

    public GetPendingWorkflowsOutput(String workflowUuid, String queueName) {
        this.workflowUuid = workflowUuid;
        this.queueName = queueName;
    }

    public String getWorkflowUuid() {
        return workflowUuid;
    }

    public String getQueueName() {
        return queueName;
    }
}
