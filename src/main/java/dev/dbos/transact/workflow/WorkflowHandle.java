package dev.dbos.transact.workflow;

public interface WorkflowHandle<T> {

    /**
     * Return the applicable workflow ID. Corresponds to the 'workflow_id' attribute
     * and 'get_workflow_id' method in Python.
     *
     * @return The workflow ID.
     */
    String getWorkflowId();

    /**
     * Return the result of the workflow function invocation, waiting if necessary.
     * This method blocks until the result is available.
     *
     * @return The result of the workflow invocation.
     */
    T getResult();

    /**
     * Return the current workflow function invocation status as `WorkflowStatus`.
     *
     * @return The current status of the workflow.
     */
    WorkflowStatus getStatus();
}
