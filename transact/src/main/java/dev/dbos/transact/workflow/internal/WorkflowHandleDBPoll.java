package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;

public class WorkflowHandleDBPoll<T, E extends Exception> implements WorkflowHandle<T, E> {
  private final DBOSExecutor executor;
  private final String workflowId;

  public WorkflowHandleDBPoll(DBOSExecutor executor, String workflowId) {
    this.executor = executor;
    this.workflowId = workflowId;
  }

  @Override
  public String workflowId() {
    return workflowId;
  }

  @Override
  public T getResult() throws E {
    return executor.getResult(this.workflowId);
  }

  @Override
  public WorkflowStatus getStatus() {
    return executor.getWorkflowStatus(workflowId);
  }
}
