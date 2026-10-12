package dev.dbos.transact.database;

import dev.dbos.transact.workflow.WorkflowState;

import java.time.Instant;

/**
 * @param clock the database's clock as the row was written or read, which the run's deadline is
 *     measured against
 */
public record WorkflowInitResult(
    WorkflowState status,
    Instant deadline,
    boolean shouldExecuteOnThisExecutor,
    String serialization,
    DatabaseTime clock) {}
