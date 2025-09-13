package dev.dbos.transact.exceptions;

public class AwaitedWorkflowCancelledException extends DBOSException {
  private String workflowId;

  public AwaitedWorkflowCancelledException(String workflowId) {
    super(
        ErrorCode.WORKFLOW_CONFLICT.getCode(),
        String.format("Awaited workflow %s was cancelled.", workflowId));
    this.workflowId = workflowId;
  }

  public String getWorkflowId() {
    return workflowId;
  }
}
