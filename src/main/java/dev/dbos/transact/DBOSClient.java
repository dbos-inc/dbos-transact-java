package dev.dbos.transact;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.queue.ListQueuedWorkflowsInput;
import dev.dbos.transact.workflow.ForkOptions;
import dev.dbos.transact.workflow.ListWorkflowsInput;
import dev.dbos.transact.workflow.StepInfo;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;
import dev.dbos.transact.workflow.internal.WorkflowHandleDBPoll;
import dev.dbos.transact.workflow.internal.WorkflowStatusInternal;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

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

    // TODO: add priority + deduplicationId options
    // (https://github.com/dbos-inc/dbos-transact-java/issues/67)
    public record EnqueueOptions(
            String workflowName,
            String queueName,
            String targetClassName,
            String workflowId,
            String appVersion,
            Long timeoutMS) {
    }

    public static class EnqueueOptionsBuilder {
        String workflowName;
        String queueName;
        String targetClassName;
        String workflowId;
        String appVersion;
        Long timeoutMS;

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

        public EnqueueOptions build() {
            return new EnqueueOptions(
                    workflowName,
                    queueName,
                    targetClassName,
                    workflowId,
                    appVersion,
                    timeoutMS);
        }
    }

    public <T> WorkflowHandle<T> enqueueWorkflow(EnqueueOptions options, Object[] args) throws Throwable {
        Objects.requireNonNull(options.workflowName);
        Objects.requireNonNull(options.queueName);

        var wfid = DBOSExecutor.enqueueWorkflow(
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
        return retrieveWorkflow(wfid);
    }

    public void send(String destinationId, Object message, String topic, String idempotencyKey) throws SQLException {
        var workflowId = "%s-%s".formatted(destinationId, idempotencyKey);
        var now = System.currentTimeMillis();
        if (idempotencyKey == null) {
            idempotencyKey = UUID.randomUUID().toString();
        }

        var status = new WorkflowStatusInternal(workflowId, WorkflowState.SUCCESS, "temp_workflow-send-client", null,
                null, null, null, null, null, null, now, now, null, null, null, null, 0, null, null, null, 0, null);
        systemDatabase.initWorkflowStatus(status, null);
        systemDatabase.send(status.getWorkflowUUID(), 0, destinationId, message, topic);
    }

    public Object getEvent(String targetId, String key, double timeoutSeconds) {
        return systemDatabase.getEvent(targetId, key, timeoutSeconds, null);
    }

    public <T> WorkflowHandle<T> retrieveWorkflow(String workflowId) {
        return new WorkflowHandleDBPoll<T>(workflowId, systemDatabase);
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
