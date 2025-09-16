package dev.dbos.transact.workflow.internal;

import static dev.dbos.transact.exceptions.ErrorCode.UNEXPECTED;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSException;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class WorkflowHandleFuture<T> implements WorkflowHandle<T> {

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

  @Override
  public T getResult() {
    try {
      return futureResult.get();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    } catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public WorkflowStatus getStatus() {
    return systemDatabase.getWorkflowStatus(workflowId);
  }
}
