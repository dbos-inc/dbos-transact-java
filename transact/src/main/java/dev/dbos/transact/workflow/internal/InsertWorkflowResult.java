package dev.dbos.transact.workflow.internal;

public record InsertWorkflowResult(
    int recoveryAttempts,
    String status,
    String name,
    String className,
    String instanceName,
    String queueName,
    Long timeoutMs,
    Long deadlineEpochMs) {}
