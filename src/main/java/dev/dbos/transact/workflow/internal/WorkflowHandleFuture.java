package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.util.concurrent.Future;

public class WorkflowHandleFuture<T, E extends Exception> implements WorkflowHandle<T, E> {

  private String workflowId;
  private Future<T> futureResult;
  private SystemDatabase systemDatabase;

  public WorkflowHandleFuture(String workflowId, Future<T> future, SystemDatabase sysdb) {
    this.workflowId = workflowId;
    this.futureResult = future;
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
      return futureResult.get();
    } catch (Exception e) {
      throw (E) e;
    }
  }

  @Override
  public WorkflowStatus getStatus() {
    return systemDatabase.getWorkflowStatus(workflowId);
  }
}
