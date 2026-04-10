package dev.dbos.transact.database;

public record WorkflowInitResult(
    String status,
    Long deadlineEpochMS,
    boolean shouldExecuteOnThisExecutor,
    String serialization) {}
