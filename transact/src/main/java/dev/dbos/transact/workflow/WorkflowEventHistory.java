package dev.dbos.transact.workflow;

public record WorkflowEventHistory(String key, String value, int stepId, String serialization) {}
