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
   * Refuse the call with {@link dev.dbos.transact.exceptions.DBOSWorkflowIdInUseException}, leaving
   * the existing workflow as it was. The SDK's own retry of an insert whose first attempt committed
   * is not refused; calling again with the same ID is.
   */
  REJECT,
}
