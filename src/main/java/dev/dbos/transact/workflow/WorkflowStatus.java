package dev.dbos.transact.workflow;

import java.time.Duration;
import java.time.Instant;

public record WorkflowStatus(
    String workflowId,
    String status,
    String name,
    String className,
    String configName,
    String authenticatedUser,
    String assumedRole,
    String[] authenticatedRoles,
    Object[] input,
    Object output,
    ErrorResult error,
    Long createdAt,
    Long updatedAt,
    String queueName,
    String executorId,
    String appVersion,
    Long workflowTimeoutMs,
    Long workflowDeadlineEpochMs,
    String appId,
    Integer recoveryAttempts) {

  public Instant getDeadline() {
    if (workflowDeadlineEpochMs != null) {
      return Instant.ofEpochMilli(workflowDeadlineEpochMs);
    }
    return null;
  }

  public Duration getTimeout() {
    if (workflowTimeoutMs != null) {
      return Duration.ofMillis(workflowTimeoutMs);
    }

    return null;
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
