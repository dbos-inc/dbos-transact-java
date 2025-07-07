package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;

public class WorkflowHandleDBPoll<T> implements WorkflowHandle<T> {
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

    @Override
    public T getResult()  {

        T result = (T)systemDatabase.awaitWorkflowResult(workflowId);
        return result ;

    }

    @Override
    public WorkflowStatus getStatus() {
        return systemDatabase.getWorkflowStatus(workflowId) ;
    }
}
