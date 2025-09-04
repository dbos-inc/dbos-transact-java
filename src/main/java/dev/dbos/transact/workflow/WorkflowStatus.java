package dev.dbos.transact.workflow;

import java.util.Objects;

public class WorkflowStatus {
    private String workflowId;
    private String status;
    private String name;
    private String className;
    private String configName;
    private String authenticatedUser;
    private String assumedRole;
    private String[] authenticatedRoles;
    private Object[] input;
    private Object output;
    // TODO fix private Throwable error;
    private String error;
    private Long createdAt;
    private Long updatedAt;
    private String queueName;
    private String executorId;
    private String appVersion;
    private Long workflowTimeoutMs;
    private Long workflowDeadlineEpochMs;

    private String appId;
    private Integer recoveryAttempts;

    public WorkflowStatus() {
    }

    public WorkflowStatus(String workflowId, String status, String name, String className,
            String configName, String authenticatedUser, String assumedRole,
            String[] authenticatedRoles, Object[] input, Object output, String error,
            Long createdAt, Long updatedAt, String queueName, String executorId, String appVersion,
            Long workflowTimeoutMs, Long workflowDeadlineEpochMs, String appId,
            Integer recoveryAttempts) {
        this.workflowId = workflowId;
        this.status = status;
        this.name = name;
        this.className = className;
        this.configName = configName;
        this.authenticatedUser = authenticatedUser;
        this.assumedRole = assumedRole;
        this.authenticatedRoles = authenticatedRoles;
        this.input = input;
        this.output = output;
        this.error = error;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.queueName = queueName;
        this.executorId = executorId;
        this.appVersion = appVersion;
        this.workflowTimeoutMs = workflowTimeoutMs;
        this.workflowDeadlineEpochMs = workflowDeadlineEpochMs;
        this.appId = appId;
        this.recoveryAttempts = recoveryAttempts;
    }

    public String getWorkflowId() {
        return workflowId;
    }

    public void setWorkflowId(String workflowId) {
        this.workflowId = workflowId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public String getConfigName() {
        return configName;
    }

    public void setConfigName(String configName) {
        this.configName = configName;
    }

    public String getAuthenticatedUser() {
        return authenticatedUser;
    }

    public void setAuthenticatedUser(String authenticatedUser) {
        this.authenticatedUser = authenticatedUser;
    }

    public String getAssumedRole() {
        return assumedRole;
    }

    public void setAssumedRole(String assumedRole) {
        this.assumedRole = assumedRole;
    }

    public String[] getAuthenticatedRoles() {
        return authenticatedRoles;
    }

    public void setAuthenticatedRoles(String[] authenticatedRoles) {
        this.authenticatedRoles = authenticatedRoles;
    }

    public Object[] getInput() {
        return input;
    }

    public void setInput(Object[] input) {
        this.input = input;
    }

    public Object getOutput() {
        return output;
    }

    public void setOutput(Object output) {
        this.output = output;
    }

    public String getError() {
        return error;
    }

    public void setError(String error) {
        this.error = error;
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    public Long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Long updatedAt) {
        this.updatedAt = updatedAt;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getExecutorId() {
        return executorId;
    }

    public void setExecutorId(String executorId) {
        this.executorId = executorId;
    }

    public String getAppVersion() {
        return appVersion;
    }

    public void setAppVersion(String appVersion) {
        this.appVersion = appVersion;
    }

    public Long getWorkflowTimeoutMs() {
        return workflowTimeoutMs;
    }

    public void setWorkflowTimeoutMs(Long workflowTimeoutMs) {
        this.workflowTimeoutMs = workflowTimeoutMs;
    }

    public Long getWorkflowDeadlineEpochMs() {
        return workflowDeadlineEpochMs;
    }

    public void setWorkflowDeadlineEpochMs(Long workflowDeadlineEpochMs) {
        this.workflowDeadlineEpochMs = workflowDeadlineEpochMs;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public Integer getRecoveryAttempts() {
        return recoveryAttempts;
    }

    public void setRecoveryAttempts(Integer recoveryAttempts) {
        this.recoveryAttempts = recoveryAttempts;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        WorkflowStatus that = (WorkflowStatus) o;
        return Objects.equals(workflowId, that.workflowId) && Objects.equals(status, that.status)
                && Objects.equals(name, that.name) && Objects.equals(className, that.className)
                && Objects.equals(configName, that.configName)
                && Objects.equals(authenticatedUser, that.authenticatedUser)
                && Objects.equals(assumedRole, that.assumedRole)
                && Objects.equals(authenticatedRoles, that.authenticatedRoles)
                && Objects.equals(input, that.input) && Objects.equals(output, that.output)
                && Objects.equals(error, that.error) && Objects.equals(createdAt, that.createdAt)
                && Objects.equals(updatedAt, that.updatedAt)
                && Objects.equals(queueName, that.queueName)
                && Objects.equals(executorId, that.executorId)
                && Objects.equals(appVersion, that.appVersion)
                && Objects.equals(workflowTimeoutMs, that.workflowTimeoutMs)
                && Objects.equals(workflowDeadlineEpochMs, that.workflowDeadlineEpochMs)
                && Objects.equals(appId, that.appId)
                && Objects.equals(recoveryAttempts, that.recoveryAttempts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(workflowId,
                status,
                name,
                className,
                configName,
                authenticatedUser,
                assumedRole,
                authenticatedRoles,
                input,
                output,
                error,
                createdAt,
                updatedAt,
                queueName,
                executorId,
                appVersion,
                workflowTimeoutMs,
                workflowDeadlineEpochMs,
                appId,
                recoveryAttempts);
    }

    @Override
    public String toString() {
        return String.format(
                "WorkflowStatus{workflowId='%s', status='%s', name='%s', className='%s', configName='%s', authenticatedUser='%s', assumedRole='%s', authenticatedRoles=%s, input=%s, output=%s, error=%s, createdAt=%s, updatedAt=%s, queueName='%s', executorId='%s', appVersion='%s', workflowTimeoutMs=%s, workflowDeadlineEpochMs=%s, appId='%s', recoveryAttempts=%s}",
                workflowId,
                status,
                name,
                className,
                configName,
                authenticatedUser,
                assumedRole,
                java.util.Arrays.toString(authenticatedRoles),
                java.util.Arrays.toString(input),
                output,
                error,
                createdAt,
                updatedAt,
                queueName,
                executorId,
                appVersion,
                workflowTimeoutMs,
                workflowDeadlineEpochMs,
                appId,
                recoveryAttempts);
    }
}
