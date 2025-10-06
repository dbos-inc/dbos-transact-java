package dev.dbos.transact.exceptions;

public class DBOSWorkflowConflictException extends RuntimeException {
  public DBOSWorkflowConflictException(String workflowId, String msg) {
    super(String.format("Conflicting workflow invocation with same ID %s : %s", workflowId, msg));
  }
}
