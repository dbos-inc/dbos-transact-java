package dev.dbos.transact.internal;

import dev.dbos.transact.execution.DBOSExecutor;

public record Invocation(
    DBOSExecutor executor,
    String className,
    String instanceName,
    String workflowName,
    Object[] args) {}
