package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.workflow.WorkflowState;

public class WorkflowStatusInternal {

    private String workflowUUID;
    private WorkflowState status;
    private String name;
    private String className;
    private String configName;
    private String authenticatedUser;
    private String assumedRole;
    private String authenticatedRoles;
    private String output;
    private String error;
    private Long createdAt;
    private Long updatedAt;
    private String queueName;
    private String executorId;
    private String appVersion;
    private String appId;
    private Integer recoveryAttempts;
    private Long workflowTimeoutMs;
    private Long workflowDeadlineEpochMs;
    private String deduplicationId;
    private int priority; // default 0 = highest priority
    private String inputs; // serialized workflow inputs

    public WorkflowStatusInternal(String workflowUUID, WorkflowState status, String name,
            String className, String configName, String authenticatedUser,
            String assumedRole, String authenticatedRoles, String output, String error,
            Long createdAt, Long updatedAt, String queueName, String executorId,
            String appVersion, String appId, Integer recoveryAttempts,
            Long workflowTimeoutMs, Long workflowDeadlineEpochMs, String deduplicationId,
            int priority, String inputs) {
        this.workflowUUID = workflowUUID;
        this.status = status;
        this.name = name;
        this.className = className;
        this.configName = configName;
        this.authenticatedUser = authenticatedUser;
        this.assumedRole = assumedRole;
        this.authenticatedRoles = authenticatedRoles;
        this.output = output;
        this.error = error;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.queueName = queueName;
        this.executorId = executorId;
        this.appVersion = appVersion;
        this.appId = appId;
        this.recoveryAttempts = recoveryAttempts;
        this.workflowTimeoutMs = workflowTimeoutMs;
        this.workflowDeadlineEpochMs = workflowDeadlineEpochMs;
        this.deduplicationId = deduplicationId;
        this.priority = priority;
        this.inputs = inputs;
    }

    public String getWorkflowUUID() {
        return workflowUUID;
    }

    public void setWorkflowUUID(String id) {
        workflowUUID = id;
    }

    public WorkflowState getStatus() {
        return status;
    }

    public void setStatus(WorkflowState state) {
        this.status = state;
    }

    public String getName() {
        return name;
    }

    public String getClassName() {
        return className;
    }

    public String getConfigName() {
        return configName;
    }

    public String getAuthenticatedUser() {
        return authenticatedUser;
    }

    public String getAssumedRole() {
        return assumedRole;
    }

    public String getAuthenticatedRoles() {
        return authenticatedRoles;
    }

    public String getOutput() {
        return output;
    }

    public String getError() {
        return error;
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public Long getUpdatedAt() {
        return updatedAt;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getExecutorId() {
        return executorId;
    }

    public void setExecutorId(String eid) {
        this.executorId = eid;
    }

    public String getAppVersion() {
        return appVersion;
    }

    public String getAppId() {
        return appId;
    }

    public Integer getRecoveryAttempts() {
        return recoveryAttempts;
    }

    public Long getWorkflowTimeoutMs() {
        return workflowTimeoutMs;
    }

    public Long getWorkflowDeadlineEpochMs() {
        return workflowDeadlineEpochMs;
    }

    public String getDeduplicationId() {
        return deduplicationId;
    }

    public void setDeduplicationId(String dedupId) {
        this.deduplicationId = dedupId;
    }

    public int getPriority() {
        return priority;
    }

    public String getInputs() {
        return inputs;
    }
}
