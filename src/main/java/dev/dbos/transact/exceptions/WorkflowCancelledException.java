package dev.dbos.transact.exceptions;

public class WorkflowCancelledException extends DBOSException {
    public WorkflowCancelledException(String workflowId) {
        super(ErrorCode.WORKFLOW_CANCELLED.getCode(),
                String.format("Workflow %s has been cancelled", workflowId));
    }
}
