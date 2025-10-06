package dev.dbos.transact.exceptions;

public class DBOSAwaitedWorkflowCancelledException extends DBOSException {
  private String workflowId;

  public DBOSAwaitedWorkflowCancelledException(String workflowId) {
    super(
        ErrorCode.WORKFLOW_CONFLICT.getCode(),
        String.format("Awaited workflow %s was cancelled.", workflowId));
    this.workflowId = workflowId;
  }

  public String getWorkflowId() {
    return workflowId;
  }
}
