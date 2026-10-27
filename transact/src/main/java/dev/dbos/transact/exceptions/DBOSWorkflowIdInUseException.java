package dev.dbos.transact.exceptions;

import dev.dbos.transact.workflow.WorkflowState;

/**
 * {@code DBOSWorkflowIdInUseException} is thrown when a workflow is started or enqueued with {@link
 * dev.dbos.transact.workflow.WorkflowIdReusePolicy#REJECT} and a workflow with its ID already
 * exists, in any status. Nothing is written.
 *
 * <p>A child workflow refused this way is recorded at its parent's step, so a replay of the parent
 * throws it again.
 */
public class DBOSWorkflowIdInUseException extends RuntimeException {
  // Pinned: this exception is recorded in the system database, and a replay must read it back.
  private static final long serialVersionUID = 1L;

  private final String workflowId;
  private final WorkflowState workflowStatus;
  private final String workflowName;

  public DBOSWorkflowIdInUseException(
      String workflowId, WorkflowState workflowStatus, String workflowName) {
    super(
        "Workflow ID %s is already in use by workflow %s with status %s."
            .formatted(workflowId, workflowName, workflowStatus));
    this.workflowId = workflowId;
    this.workflowStatus = workflowStatus;
    this.workflowName = workflowName;
  }

  /** The ID the refused call asked for. */
  public String workflowId() {
    return workflowId;
  }

  /** The status of the workflow that already holds the ID. */
  public WorkflowState workflowStatus() {
    return workflowStatus;
  }

  /** The name of the workflow that already holds the ID. */
  public String workflowName() {
    return workflowName;
  }
}
