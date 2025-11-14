package dev.dbos.transact.exceptions;

/**
 * {@code DBOSWorkflowCancelledException} is thrown when DBOS determines that a workflow has
 * exceeded its timeout and should be terminated. As this exception indicates that execution should
 * be stopped abruptly, exception handling (if any) should take no further durable actions.
 */
public class DBOSWorkflowCancelledException extends RuntimeException {
  private final String workflowId;

  public DBOSWorkflowCancelledException(String workflowId) {
    super(String.format("Workflow %s has been cancelled", workflowId));
    this.workflowId = workflowId;
  }

  /** ID of the workflow that is being cancelled */
  public String workflowId() {
    return this.workflowId;
  }
}
