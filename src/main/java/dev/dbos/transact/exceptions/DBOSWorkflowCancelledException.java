package dev.dbos.transact.exceptions;

public class DBOSWorkflowCancelledException extends RuntimeException {
  public DBOSWorkflowCancelledException(String workflowId) {
    super(String.format("Workflow %s has been cancelled", workflowId));
  }
}
