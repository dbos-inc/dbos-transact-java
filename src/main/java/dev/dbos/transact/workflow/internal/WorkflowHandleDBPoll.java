package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;

public class WorkflowHandleDBPoll<T, E extends Exception> implements WorkflowHandle<T, E> {
  private String workflowId;
  private SystemDatabase systemDatabase;

  public WorkflowHandleDBPoll(String workflowId, SystemDatabase sysdb) {
    this.workflowId = workflowId;
    this.systemDatabase = sysdb;
  }

  @Override
  public String getWorkflowId() {
    return workflowId;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T getResult() throws E {
    try {
      return (T) systemDatabase.awaitWorkflowResult(workflowId);
    } catch (Exception e) {
      throw (E) e;
    }
  }

  @Override
  public WorkflowStatus getStatus() {
    return systemDatabase.getWorkflowStatus(workflowId)
        .orElseThrow(() -> new java.util.NoSuchElementException(
            "Workflow status not found for workflowId: " + workflowId));
  }
}
