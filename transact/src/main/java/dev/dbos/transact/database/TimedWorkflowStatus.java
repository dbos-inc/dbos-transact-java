package dev.dbos.transact.database;

import dev.dbos.transact.workflow.WorkflowStatus;

/**
 * A workflow's status row, and the database's clock as it was read.
 *
 * @param status the workflow's status
 * @param readAt the database's clock as the row was read, which a claimed workflow's deadline is
 *     measured against
 */
public record TimedWorkflowStatus(WorkflowStatus status, DatabaseTime readAt) {}
