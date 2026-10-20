package dev.dbos.transact.database;

import org.jspecify.annotations.Nullable;

/**
 * A workflow the system database cancelled, with the token of the execution that owned it at that
 * moment.
 *
 * @param workflowId the workflow cancelled
 * @param ownerXid the token of the execution that owned it when it was cancelled, or null if no
 *     execution did (it was enqueued, delayed, or not yet claimed)
 */
public record CancelledWorkflow(String workflowId, @Nullable String ownerXid) {}
