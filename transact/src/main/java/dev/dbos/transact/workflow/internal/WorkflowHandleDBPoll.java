package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;

public class WorkflowHandleDBPoll<T, E extends Exception> implements WorkflowHandle<T, E> {
  private final DBOSExecutor executor;
  private final String workflowId;
  private final boolean failIfMissing;

  public WorkflowHandleDBPoll(DBOSExecutor executor, String workflowId) {
    this(executor, workflowId, false);
  }

  // failIfMissing is for handles built from a workflow_status row that was just read: a
  // missing row means it was deleted, so getResult fails fast with
  // DBOSNonExistentWorkflowException instead of polling for a row that will never reappear.
  public WorkflowHandleDBPoll(DBOSExecutor executor, String workflowId, boolean failIfMissing) {
    this.executor = executor;
    this.workflowId = workflowId;
    this.failIfMissing = failIfMissing;
  }

  @Override
  public String workflowId() {
    return workflowId;
  }

  @Override
  public T getResult() throws E {
    return executor.getResult(this.workflowId, failIfMissing);
  }

  @Override
  public WorkflowStatus getStatus() {
    return executor.getWorkflowStatus(workflowId);
  }
}
