package dev.dbos.transact.internal;

public record Invocation(
    String className, String instanceName, String workflowName, Object[] args) {}
