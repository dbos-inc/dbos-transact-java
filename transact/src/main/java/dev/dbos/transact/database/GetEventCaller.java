package dev.dbos.transact.database;

/**
 * The workflow step a getEvent runs as, when it is called from a workflow.
 *
 * @param workflowId the calling workflow
 * @param stepId the step that records the value read
 * @param timeoutStepId the step that records the timeout's deadline
 * @param ownerXid the token of the calling workflow's execution, which must still own the workflow
 *     for either step to be recorded
 */
public record GetEventCaller(String workflowId, int stepId, int timeoutStepId, String ownerXid) {}
