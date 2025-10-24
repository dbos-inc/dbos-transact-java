package dev.dbos.transact.workflow;

public interface WorkflowHandle<T, E extends Exception> {

  /**
   * Return the handle's workflow ID
   *
   * @return The workflow ID.
   */
  String workflowId();

  /**
   * Return the result of the workflow function invocation, waiting if necessary. This method blocks
   * until the result is available.
   *
   * @return The result of the workflow invocation.
   */
  T getResult() throws E;

  /**
   * Return the current workflow function invocation status as `WorkflowStatus`.
   *
   * @return The current status of the workflow.
   */
  WorkflowStatus getStatus();
}
