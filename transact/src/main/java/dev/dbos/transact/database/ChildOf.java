package dev.dbos.transact.database;

/**
 * The parent a child workflow is started from: the parent's step that records the child, and the
 * token of the parent's execution, which must still own the parent for the child to be started.
 *
 * @param workflowId the parent workflow
 * @param functionId the parent's step that records the child
 * @param functionName the name that step is recorded under
 * @param ownerXid the token of the parent's execution
 */
public record ChildOf(String workflowId, int functionId, String functionName, String ownerXid) {}
