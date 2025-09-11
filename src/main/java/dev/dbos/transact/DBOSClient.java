package dev.dbos.transact;

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
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class DBOSClient implements AutoCloseable {

    private final SystemDatabase systemDatabase;

    public DBOSClient(String url, String user, String password) {
        var dataSource = SystemDatabase.createDataSource(url, user, password, 0, 0);
        systemDatabase = new SystemDatabase(dataSource);
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
            Duration timeout) {
        public static Builder builder(String workflowName, String queueName) {
            return new Builder(workflowName, queueName);
        }

        public static class Builder {
            String workflowName;
            String queueName;
            String targetClassName;
            String workflowId;
            String appVersion;
            Duration timeout;

        public Builder(String workflowName, String queueName) {
            this.workflowName = Objects.requireNonNull(workflowName);
            this.queueName = Objects.requireNonNull(queueName);
        }

        public Builder targetClassName(String targetClassName) {
            this.targetClassName = Objects.requireNonNull(targetClassName);
            return this;
        }

        public Builder workflowId(String workflowId) {
            this.workflowId = Objects.requireNonNull(workflowId);
            return this;
        }

        public Builder appVersion(String appVersion) {
            this.appVersion = Objects.requireNonNull(appVersion);
            return this;
        }

        public Builder timeoutMS(Duration timeout) {
            this.timeout = Objects.requireNonNull(timeout);
            return this;
        }

        public EnqueueOptions build() {
            return new EnqueueOptions(
                    workflowName,
                    queueName,
                    targetClassName,
                    workflowId,
                    appVersion,
                    timeout);
        }
    }
    }



    public <T> WorkflowHandle<T> enqueueWorkflow(EnqueueOptions options, Object[] args) throws Throwable {
        Objects.requireNonNull(options.workflowName);
        Objects.requireNonNull(options.queueName);

        var workflowId = DBOSExecutor.enqueueWorkflow(
                this.systemDatabase,
                options.workflowId,
                options.workflowName,
                options.targetClassName,
                args,
                options.queueName,
                null,
                options.appVersion,
                null,
                options.timeout);
        return retrieveWorkflow(workflowId);
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

    public <T> WorkflowHandle<T> resumeWorkflow(String workflowId) {
        systemDatabase.resumeWorkflow(workflowId);
        return retrieveWorkflow(workflowId);
    }

    public <T> WorkflowHandle<T> forkWorkflow(String originalWorkflowId, int startStep, ForkOptions options) {
        var forkedWorkflowId = systemDatabase.forkWorkflow(originalWorkflowId, startStep, options);
        return retrieveWorkflow(forkedWorkflowId);
    }

    public WorkflowStatus getWorkflowStatus(String workflowId) {
        return systemDatabase.getWorkflowStatus(workflowId);
    }

    public List<WorkflowStatus> listWorkflows(ListWorkflowsInput input) {
        try {
            return systemDatabase.listWorkflows(input);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<WorkflowStatus> listQueuedWorkflows(ListQueuedWorkflowsInput input, boolean loadInput) {
        try {
            return systemDatabase.listQueuedWorkflows(input, loadInput);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<StepInfo> listWorkflowSteps(String workflowId) {
        try {
            return systemDatabase.listWorkflowSteps(workflowId);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // readStream

}
