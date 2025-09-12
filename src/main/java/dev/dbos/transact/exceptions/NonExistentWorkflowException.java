package dev.dbos.transact.exceptions;

public class NonExistentWorkflowException extends DBOSException {
  private String workflowId;

  public NonExistentWorkflowException(String workflowId) {
    super(
        ErrorCode.NONEXISTENT_WORKFLOW.getCode(),
        String.format("Workflow does not exist %s", workflowId));
    this.workflowId = workflowId;
  }

  public String getWorkflowId() {
    return workflowId;
  }
}
