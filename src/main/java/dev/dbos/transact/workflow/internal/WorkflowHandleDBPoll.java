package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;

public class WorkflowHandleDBPoll<T, E extends Exception> implements WorkflowHandle<T, E> {
  private String workflowId;

  public WorkflowHandleDBPoll(String workflowId) {
    this.workflowId = workflowId;
  }

  @Override
  public String getWorkflowId() {
    return workflowId;
  }

  @Override
  public T getResult() throws E {
    return DBOS.getResult(this.workflowId);
  }

  @Override
  public WorkflowStatus getStatus() {
    return DBOS.getWorkflowStatus(workflowId);
  }
}
