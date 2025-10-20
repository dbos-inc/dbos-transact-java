package dev.dbos.transact.exceptions;

public class DBOSWorkflowCancelledException extends RuntimeException {
  private final String workflowId;

  public DBOSWorkflowCancelledException(String workflowId) {
    super(String.format("Workflow %s has been cancelled", workflowId));
    this.workflowId = workflowId;
  }

  public String workflowId() { return this.workflowId; }
}
