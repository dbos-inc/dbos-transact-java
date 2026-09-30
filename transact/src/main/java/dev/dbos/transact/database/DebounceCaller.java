package dev.dbos.transact.database;

/**
 * The step a bounce runs as, when it runs inside a workflow: the DAO returns what that step
 * recorded if it already ran, and otherwise records the bounce's outcome with the bounce itself, in
 * one transaction.
 *
 * @param workflowId the calling workflow
 * @param stepId the function id the step replays under
 * @param stepName the step's name
 */
public record DebounceCaller(String workflowId, int stepId, String stepName) {}
