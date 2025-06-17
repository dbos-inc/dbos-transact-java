package dev.dbos.transact.exceptions;

public class DBOSWorkflowConflictException extends DBOSException {
    public DBOSWorkflowConflictException(String workflowId, String msg) {
        super(ErrorCode.WORKFLOW_CONFLICT.getCode(),
                String.format("Conflicting workflow invocation with same ID %s : %s", workflowId, msg));
    }
}
