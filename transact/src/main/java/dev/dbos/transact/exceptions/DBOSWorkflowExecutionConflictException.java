package dev.dbos.transact.exceptions;

/**
 * {@code DBOSWorkflowExecutionConflictException} is thrown when DBOS detects two concurrent
 * executions running under the same workflow ID. All but one of the concurrent executions will be
 * terminated by throwing DBOSWorkflowExecutionConflictException. As this exception indicates that
 * execution should be abandoned, this exception should not be caught or otherwise handled.
 */
public class DBOSWorkflowExecutionConflictException extends RuntimeException {
  private final String workflowId;

  public DBOSWorkflowExecutionConflictException(String workflowId) {
    super("Conflicting workflow ID %s".formatted(workflowId));
    this.workflowId = workflowId;
  }

  /** ID of the workflow for which concurrent executions were noticed */
  public String workflowId() {
    return workflowId;
  }
}
