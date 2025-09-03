package dev.dbos.transact;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.queue.ListQueuedWorkflowsInput;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.WorkflowHandleDBPoll;

import java.util.List;

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

    public record EnqueueOptions(
            String workflowName,
            String queueName,
            String targetClassName,
            String workflowId,
            String appVersion,
            Long timeoutMS
    // String deduplicationId,
    // Integer priority
    ) {
    }

    public static class EnqueueOptionsBuilder {
        String workflowName;
        String queueName;
        String targetClassName;
        String workflowId;
        String appVersion;
        Long timeoutMS;
        // String deduplicationId;
        // Integer priority;

        public EnqueueOptionsBuilder(String workflowName, String queueName) {
            this.workflowName = workflowName;
            this.queueName = queueName;
        }

        public EnqueueOptionsBuilder targetClassName(String targetClassName) {
            this.targetClassName = targetClassName;
            return this;
        }

        public EnqueueOptionsBuilder workflowId(String workflowId) {
            this.workflowId = workflowId;
            return this;
        }

        public EnqueueOptionsBuilder appVersion(String appVersion) {
            this.appVersion = appVersion;
            return this;
        }

        public EnqueueOptionsBuilder timeoutMS(Long timeoutMS) {
            this.timeoutMS = timeoutMS;
            return this;
        }

        // public EnqueueOptionsBuilder deduplicationId(String deduplicationId) {
        // this.deduplicationId = deduplicationId;
        // return this;
        // }

        // public EnqueueOptionsBuilder priority(Integer priority) {
        // this.priority = priority;
        // return this;
        // }

        public EnqueueOptions build() {
            return new EnqueueOptions(
                    workflowName,
                    queueName,
                    targetClassName,
                    workflowId,
                    appVersion,
                    timeoutMS
            // deduplicationId,
            // priority
            );
        }
    }

    public void enqueueWorkflow(EnqueueOptions options, Object[] args) throws Throwable {
        DBOSExecutor.enqueueWorkflow(
                this.systemDatabase,
                options.workflowId,
                options.workflowName,
                options.targetClassName,
                args,
                options.queueName,
                null,
                options.appVersion,
                null,
                options.timeoutMS != null ? options.timeoutMS : 0L);
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
