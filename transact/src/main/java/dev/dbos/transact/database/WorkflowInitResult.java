package dev.dbos.transact.database;

import dev.dbos.transact.workflow.WorkflowState;

public record WorkflowInitResult(
    WorkflowState status,
    Long deadlineEpochMS,
    boolean shouldExecuteOnThisExecutor,
    String serialization) {}
