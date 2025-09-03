package dev.dbos.transact;

import java.util.List;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.queue.ListQueuedWorkflowsInput;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.WorkflowHandleDBPoll;

public class DBOSClient implements AutoCloseable {

    private final SystemDatabase systemDatabase;


    public DBOSClient(String url, String userName, String password) {
        var config = new DBOSConfig.Builder().url(url).dbUser(userName).dbPassword(password).build();
        systemDatabase = new SystemDatabase(config);
    }

    @Override
    public void close() throws Exception {
        systemDatabase.close();
    }


    public void enqueueWorkflow(String workflowName, String targetClassName,
            Object[] args, String queueName) {
                
            }

    public void send(String workflowId, int functionId, String destinationId, Object message, String topic) {
        // 
    }

    public Object getEvent(String targetId, String key, double timeoutSeconds) {
        return systemDatabase.getEvent(targetId, key, timeoutSeconds, null);
    }

    public <R> WorkflowHandle<R> retrieveWorkflow(String workflowId) {
        return new WorkflowHandleDBPoll<R>(workflowId, systemDatabase);
    }

    public void cancelWorkflow(String workflowId) {
        systemDatabase.cancelWorkflow(workflowId);
    }

    public void resumeWorkflow(String workflowId) {
        systemDatabase.resumeWorkflow(workflowId);
    }

    public String forkWorkflow(String originalWorkflowId, int startStep, ForkOptions options) {
        return systemDatabase.forkWorkflow(originalWorkflowId, startStep, options);
    }

    public WorkflowStatus getWorkflowStatus(String workflowId) {
        return systemDatabase.getWorkflowStatus(workflowId);
    }

    
    public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
        return systemDatabase.listWorkflows(input);
    }
    

    public List<WorkflowStatus> listQueuedWorkflows(ListQueuedWorkflowsInput input, boolean loadInput) {
        return systemDatabase.listQueuedWorkflows(input, loadInput);
    }

    public List<StepInfo> listWorkflowSteps(String workflowId) {
        return systemDatabase.listWorkflowSteps(workflowId);
    }

    // readStream

}
