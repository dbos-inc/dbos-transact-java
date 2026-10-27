package dev.dbos.transact.workflow;

/**
 * What starting or enqueueing a workflow does when a workflow with its ID already exists, whatever
 * that workflow's status.
 */
public enum WorkflowIdReusePolicy {
  /**
   * Attach to the existing workflow: the call returns a handle to it rather than starting another.
   * The default.
   */
  RETURN_EXISTING,

  /**
   * Refuse the call with {@link dev.dbos.transact.exceptions.DBOSWorkflowIdInUseException}, and
   * write nothing. A retried call whose first attempt created the workflow is not refused.
   */
  REJECT,
}
