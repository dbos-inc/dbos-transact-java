package dev.dbos.transact.exceptions;

/**
 * {@code DBOSNonExistentWorkflowException} is thrown by DBOS functions such as `send` or
 * `executeWorkflowById` that require a workflow to exist prior to invocation.
 *
 * <p>Unless the workflow ID was taken from the user, receipt of this error generally indicates a
 * programmer error.
 */
public class DBOSNonExistentWorkflowException extends RuntimeException {
  private String workflowId;

  public DBOSNonExistentWorkflowException(String workflowId) {
    super(String.format("Workflow does not exist %s", workflowId));
    this.workflowId = workflowId;
  }

  /** ID of the workflow that was targeted, but did not exist */
  public String workflowId() {
    return workflowId;
  }
}
