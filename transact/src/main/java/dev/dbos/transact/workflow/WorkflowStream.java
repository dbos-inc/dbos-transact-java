package dev.dbos.transact.workflow;

public record WorkflowStream(
    String key, String value, int offset, int stepId, String serialization) {}
