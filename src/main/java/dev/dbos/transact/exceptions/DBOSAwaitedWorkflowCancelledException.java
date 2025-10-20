package dev.dbos.transact.exceptions;

public class DBOSAwaitedWorkflowCancelledException extends RuntimeException {
  private String workflowId;

  public DBOSAwaitedWorkflowCancelledException(String workflowId) {
    super(String.format("Awaited workflow %s was cancelled.", workflowId));
    this.workflowId = workflowId;
  }

  public String workflowId() {
    return workflowId;
  }
}
