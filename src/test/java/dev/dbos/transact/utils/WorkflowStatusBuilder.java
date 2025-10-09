package dev.dbos.transact.utils;

import java.util.Objects;

import dev.dbos.transact.json.JSONUtil;
import dev.dbos.transact.workflow.ErrorResult;
import dev.dbos.transact.workflow.WorkflowState;
import dev.dbos.transact.workflow.WorkflowStatus;

public class WorkflowStatusBuilder {
    private String workflowId;
    private String status;
    private String name;
    private String className;
    private String instanceName;
    private String authenticatedUser;
    private String assumedRole;
    private String[] authenticatedRoles;
    private Object[] input;
    private Object output;
    private ErrorResult error;
    private String executorId;
    private Long createdAt;
    private Long updatedAt;
    private String appVersion;
    private String appId;
    private Integer recoveryAttempts;
    private String queueName;
    private Long timeoutMs;
    private Long deadlineEpochMs;
    private Long startedAtEpochMs;
    private String deduplicationId;
    private Integer priority;

    public WorkflowStatus build() {
        return new WorkflowStatus(
            workflowId,
            status,
            name,
            className,
            instanceName,
            authenticatedUser,
            assumedRole,
            authenticatedRoles,
            input == null ?  new Object[0] : input,
            output,
            error,
            executorId,
            createdAt,
            updatedAt,
            appVersion,
            appId,
            recoveryAttempts,
            queueName,
            timeoutMs,
            deadlineEpochMs,
            startedAtEpochMs,
            deduplicationId,
            priority
        );
    }

    public WorkflowStatusBuilder(String workflowId) {
        this.workflowId = Objects.requireNonNull(workflowId);
    }

    public WorkflowStatusBuilder status(String status) {
        this.status = status;
        return this;
    }

    public WorkflowStatusBuilder status(WorkflowState state) {
        this.status = state.toString();
        return this;
    }

    public WorkflowStatusBuilder name(String name) {
        this.name = name;
        return this;
    }

    public WorkflowStatusBuilder className(String className) {
        this.className = className;
        return this;
    }

    public WorkflowStatusBuilder instanceName(String instanceName) {
        this.instanceName = instanceName;
        return this;
    }

    public WorkflowStatusBuilder authenticatedUser(String authenticatedUser) {
        this.authenticatedUser = authenticatedUser;
        return this;
    }

    public WorkflowStatusBuilder assumedRole(String assumedRole) {
        this.assumedRole = assumedRole;
        return this;
    }

    public WorkflowStatusBuilder authenticatedRoles(String[] authenticatedRoles) {
        this.authenticatedRoles = authenticatedRoles;
        return this;
    }

    public WorkflowStatusBuilder input(Object[] input) {
        this.input = input;
        return this;
    }

    public WorkflowStatusBuilder output(Object output) {
        this.output = output;
        return this;
    }

    public WorkflowStatusBuilder error(Throwable error) {
        String errorString = JSONUtil.serializeAppException(error);
        var wrapper = JSONUtil.deserializeAppExceptionWrapper(errorString);
        Throwable throwable = null;
        try {
            throwable = JSONUtil.deserializeAppException(errorString);
        } catch (Exception e) {
        }
        this.error = new ErrorResult(wrapper.type, wrapper.message, errorString, throwable);
        return this;
    }

    public WorkflowStatusBuilder executorId(String executorId) {
        this.executorId = executorId;
        return this;
    }

    public WorkflowStatusBuilder createdAt(Long createdAt) {
        this.createdAt = createdAt;
        return this;
    }

    public WorkflowStatusBuilder updatedAt(Long updatedAt) {
        this.updatedAt = updatedAt;
        return this;
    }

    public WorkflowStatusBuilder appVersion(String appVersion) {
        this.appVersion = appVersion;
        return this;
    }

    public WorkflowStatusBuilder appId(String appId) {
        this.appId = appId;
        return this;
    }

    public WorkflowStatusBuilder recoveryAttempts(Integer recoveryAttempts) {
        this.recoveryAttempts = recoveryAttempts;
        return this;
    }

    public WorkflowStatusBuilder queueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    public WorkflowStatusBuilder timeoutMs(Long timeoutMs) {
        this.timeoutMs = timeoutMs;
        return this;
    }

    public WorkflowStatusBuilder deadlineEpochMs(Long deadlineEpochMs) {
        this.deadlineEpochMs = deadlineEpochMs;
        return this;
    }

    public WorkflowStatusBuilder startedAtEpochMs(Long startedAtEpochMs) {
        this.startedAtEpochMs = startedAtEpochMs;
        return this;
    }

    public WorkflowStatusBuilder deduplicationId(String deduplicationId) {
        this.deduplicationId = deduplicationId;
        return this;
    }

    public WorkflowStatusBuilder priority(Integer priority) {
        this.priority = priority;
        return this;
    }

}
