package dev.dbos.transact.workflow;

public record StepInfo(
    int functionId,
    String functionName,
    Object output,
    ErrorResult error,
    String childWorkflowId) {}
