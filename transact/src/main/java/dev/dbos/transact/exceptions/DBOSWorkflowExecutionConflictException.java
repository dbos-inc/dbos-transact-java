package dev.dbos.transact.exceptions;

/**
 * {@code DBOSWorkflowExecutionConflictException} is thrown when DBOS detects two concurrent
 * executions running under the same workflow ID. All but one of the concurrent executions will be
 * terminated by throwing DBOSWorkflowExecutionConflictException. As this exception indicates that
 * execution should be abandoned, this exception should not be caught or otherwise handled.
 *
 * <p>Every checkpoint a workflow writes is checked against the execution that currently owns it. An
 * execution that has lost the workflow -- to a cancel, a resume, a recovery or another claim --
 * gets this exception at its next write, writes nothing more, and waits for the workflow's recorded
 * outcome instead.
 */
public class DBOSWorkflowExecutionConflictException extends RuntimeException {
  private final String workflowId;

  public DBOSWorkflowExecutionConflictException(String workflowId) {
    this(workflowId, "Conflicting workflow ID %s".formatted(workflowId));
  }

  private DBOSWorkflowExecutionConflictException(String workflowId, String message) {
    super(message);
    this.workflowId = workflowId;
  }

  /** The conflict an execution gets when it writes to a workflow it no longer owns. */
  public static DBOSWorkflowExecutionConflictException notOwned(String workflowId) {
    return new DBOSWorkflowExecutionConflictException(
        workflowId, "Workflow %s is no longer owned by this execution".formatted(workflowId));
  }

  /** ID of the workflow for which concurrent executions were noticed */
  public String workflowId() {
    return workflowId;
  }
}
