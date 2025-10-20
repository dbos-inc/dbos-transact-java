package dev.dbos.transact.exceptions;

public class DBOSWorkflowExecutionConflictException extends RuntimeException {
  private String workflowId;

  public DBOSWorkflowExecutionConflictException(String workflowId) {
    super("Conflicting workflow ID %s".formatted(workflowId));
    this.workflowId = workflowId;
  }

  public String workflowId() {
    return workflowId;
  }
}
