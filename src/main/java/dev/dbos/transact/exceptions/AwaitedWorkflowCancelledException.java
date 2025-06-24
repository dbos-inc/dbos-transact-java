package dev.dbos.transact.exceptions;

public class AwaitedWorkflowCancelledException extends DBOSException {
    public AwaitedWorkflowCancelledException(String workflowId) {
        super(ErrorCode.WORKFLOW_CONFLICT.getCode(),
                String.format("Awaited workflow %s was cancelled.", workflowId));
    }
}
