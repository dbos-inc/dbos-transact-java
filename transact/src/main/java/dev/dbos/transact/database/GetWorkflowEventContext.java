package dev.dbos.transact.database;

public record GetWorkflowEventContext(String workflowId, int functionId, int timeoutFunctionId) {}
