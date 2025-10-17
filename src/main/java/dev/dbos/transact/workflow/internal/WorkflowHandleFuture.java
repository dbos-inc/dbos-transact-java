package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class WorkflowHandleFuture<T, E extends Exception> implements WorkflowHandle<T, E> {

  private String workflowId;
  private Future<T> futureResult;
  private DBOSExecutor executor;

  public WorkflowHandleFuture(String workflowId, Future<T> future, DBOSExecutor executor) {
    this.workflowId = workflowId;
    this.futureResult = future;
    this.executor = executor;
  }

  @Override
  public String getWorkflowId() {
    return workflowId;
  }

  @SuppressWarnings("unchecked")
  @Override
  public T getResult() throws E {
    return executor.<T, E>callFunctionAsStep(
        () -> {
          try {
            return futureResult.get();
          } catch (DBOSWorkflowExecutionConflictException e) {
            return (T) executor.awaitWorkflowResult(workflowId);
          } catch (ExecutionException ee) {
            if (ee.getCause() instanceof Exception) {
              throw (E) ee.getCause();
            }
            throw new RuntimeException("Future threw non-exception", ee.getCause());
          } catch (Exception e) {
            throw (E) e;
          }
        },
        "DBOS.getResult",
        workflowId);
  }

  @Override
  public WorkflowStatus getStatus() {
    return DBOS.getWorkflowStatus(workflowId);
  }
}
