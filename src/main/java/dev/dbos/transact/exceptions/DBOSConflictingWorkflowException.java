package dev.dbos.transact.exceptions;

public class DBOSConflictingWorkflowException extends RuntimeException {
  private final String workflowId;

  public DBOSConflictingWorkflowException(String workflowId, String msg) {
    super(String.format("Conflicting workflow invocation with same ID %s : %s", workflowId, msg));
    this.workflowId = workflowId;
  }

  public String workflowId() { return this.workflowId; }
}
