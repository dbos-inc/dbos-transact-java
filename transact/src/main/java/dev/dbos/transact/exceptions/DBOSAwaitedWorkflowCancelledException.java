package dev.dbos.transact.exceptions;

/**
 * {@code DBOSAwaitedWorkflowCancelledException} is thrown by DBOS when a call requests the return
 * value of a workflow that was cancelled.
 *
 * <p>Calls such as {@code DBOS.getResult} and {@code WorkflowHandle.getResult} may throw this
 * exception.
 */
public class DBOSAwaitedWorkflowCancelledException extends RuntimeException {
  private String workflowId;

  public DBOSAwaitedWorkflowCancelledException(String workflowId) {
    super(String.format("Awaited workflow %s was cancelled.", workflowId));
    this.workflowId = workflowId;
  }

  /** The workflow ID of the awaited, cancelled workflow */
  public String workflowId() {
    return workflowId;
  }
}
