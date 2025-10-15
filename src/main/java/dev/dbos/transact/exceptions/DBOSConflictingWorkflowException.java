package dev.dbos.transact.exceptions;

public class DBOSConflictingWorkflowException extends RuntimeException {
  public DBOSConflictingWorkflowException(String workflowId, String msg) {
    super(String.format("Conflicting workflow invocation with same ID %s : %s", workflowId, msg));
  }
}
