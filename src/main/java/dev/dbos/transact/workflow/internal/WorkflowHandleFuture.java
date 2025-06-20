package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSException;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;

import java.util.concurrent.Future;

import static dev.dbos.transact.exceptions.ErrorCode.UNEXPECTED;

public class WorkflowHandleFuture<T> implements WorkflowHandle<T> {

    private String workflowId;
    private Future<T> futureResult;
    private SystemDatabase systemDatabase;

    public WorkflowHandleFuture(String workflowId, Future<T> future, SystemDatabase sysdb) {
        this.workflowId = workflowId;
        this.futureResult = future ;
        this.systemDatabase = sysdb;
    }

    @Override
    public String getWorkflowId() {
        return workflowId;
    }

    @Override
    public T getResult()  {
        try {
            return futureResult.get();
        } catch (Exception e) {
            throw new DBOSException(UNEXPECTED.getCode(), e.getMessage()) ;
        }
    }

    @Override
    public WorkflowStatus getStatus() {
        return systemDatabase.getWorkflow(workflowId) ;
    }
}
