package dev.dbos.transact.context;

/**
 * Contains identifying information about the parent of a workflow.
 *
 * <p>If provided, this record encapsulates the parent's workflow id and the calling step number
 * that started the workflow.
 *
 * @param workflowId The unique id for the parent workflow.
 * @param functionId The step number within the parent workflow that created the child.
 * @param ownerXid The token of the parent's execution, which the child's start is checked against:
 *     a parent that no longer owns its workflow must not start or record a child.
 */
public record WorkflowInfo(String workflowId, int functionId, String ownerXid) {}
