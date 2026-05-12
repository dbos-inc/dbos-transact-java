package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.util.concurrent.Future;

public class WorkflowHandleFuture<T, E extends Exception> implements WorkflowHandle<T, E> {

  private final DBOSExecutor executor;
  private final String workflowId;
  private final Future<T> futureResult;

  public WorkflowHandleFuture(DBOSExecutor executor, String workflowId, Future<T> future) {
    this.workflowId = workflowId;
    this.futureResult = future;
    this.executor = executor;
  }

  @Override
  public String workflowId() {
    return workflowId;
  }

  @Override
  public T getResult() throws E {
    return executor.getResult(workflowId, futureResult);
  }

  @Override
  public WorkflowStatus getStatus() {
    return executor.getWorkflowStatus(workflowId);
  }
}
