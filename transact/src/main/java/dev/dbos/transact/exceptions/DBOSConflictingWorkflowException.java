package dev.dbos.transact.exceptions;

/**
 * {@code DBOSConflictingWorkflowException} is thrown by DBOS when an attempt is made to start a
 * workflow, a workflow with that ID that already exists, and the existing workflow is incompatible
 * with the attempted workflow initiation.
 *
 * <p>{@code DBOSConflictingWorkflowException} generally indicates a programmer error, such as
 * attempting to invoke two entirely different workflow functions with the same workflow ID.
 *
 * <p>Direct workflow invocation, workflow enqueue, and {@code DBOS.startWorkflow} may throw this
 * exception.
 */
public class DBOSConflictingWorkflowException extends RuntimeException {
  private final String workflowId;

  public DBOSConflictingWorkflowException(String workflowId, String msg) {
    super(String.format("Conflicting workflow invocation with same ID %s : %s", workflowId, msg));
    this.workflowId = workflowId;
  }

  /** The workflow ID that already exists, with a conflicting invocation */
  public String workflowId() {
    return this.workflowId;
  }
}
