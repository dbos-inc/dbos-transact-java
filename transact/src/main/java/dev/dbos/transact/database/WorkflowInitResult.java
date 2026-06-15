package dev.dbos.transact.database;

import dev.dbos.transact.workflow.WorkflowState;

import java.time.Instant;

public record WorkflowInitResult(
    WorkflowState status,
    Instant deadline,
    boolean shouldExecuteOnThisExecutor,
    String serialization) {}
