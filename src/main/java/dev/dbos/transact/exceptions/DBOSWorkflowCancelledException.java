package dev.dbos.transact.exceptions;

public class DBOSWorkflowCancelledException extends DBOSException {
  public DBOSWorkflowCancelledException(String workflowId) {
    super(
        ErrorCode.WORKFLOW_CANCELLED.getCode(),
        String.format("Workflow %s has been cancelled", workflowId));
  }
}
