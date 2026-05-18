package dev.dbos.transact.database;

public record GetEventCaller(String workflowId, int stepId, int timeoutStepId) {}
