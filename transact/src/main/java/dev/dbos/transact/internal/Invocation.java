package dev.dbos.transact.internal;

import dev.dbos.transact.execution.DBOSExecutor;

public record Invocation(
    DBOSExecutor executor,
    String workflowName,
    String className,
    String instanceName,
    Object[] args) {}
