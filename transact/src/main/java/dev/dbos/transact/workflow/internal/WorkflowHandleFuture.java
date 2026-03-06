package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.exceptions.DBOSAwaitedWorkflowCancelledException;
import dev.dbos.transact.exceptions.DBOSWorkflowExecutionConflictException;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
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

  @SuppressWarnings("unchecked")
  @Override
  public T getResult() throws E {
    return executor.<T, E>callFunctionAsStep(
        () -> {
          try {
            return futureResult.get();
          } catch (DBOSWorkflowExecutionConflictException e) {
            return (T) executor.awaitWorkflowResult(workflowId);
          } catch (CancellationException e) {
            throw new DBOSAwaitedWorkflowCancelledException(workflowId);
          } catch (ExecutionException ee) {
            if (ee.getCause() instanceof Exception) {
              var re = ee.getCause();
              if (re instanceof DBOSWorkflowExecutionConflictException) {
                return (T) executor.awaitWorkflowResult(workflowId);
              }
              throw (E) re;
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
    return executor.getWorkflowStatus(workflowId);
  }
}
