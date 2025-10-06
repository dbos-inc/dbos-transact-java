package dev.dbos.transact.exceptions;

public class DBOSNonExistentWorkflowException extends RuntimeException {
  private String workflowId;

  public DBOSNonExistentWorkflowException(String workflowId) {
    super(String.format("Workflow does not exist %s", workflowId));
    this.workflowId = workflowId;
  }

  public String getWorkflowId() {
    return workflowId;
  }
}
