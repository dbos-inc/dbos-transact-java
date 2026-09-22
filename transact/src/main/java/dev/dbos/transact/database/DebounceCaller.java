package dev.dbos.transact.database;

import dev.dbos.transact.workflow.Debouncer;

import org.jspecify.annotations.Nullable;

/**
 * The step a bounce runs as, when it runs inside a workflow: the DAO returns what that step
 * recorded if it already ran, and otherwise records the bounce's outcome with the bounce itself, in
 * one transaction.
 *
 * @param workflowId the calling workflow
 * @param stepId the function id the step replays under
 * @param stepName the step's name
 * @param ids when the step is the debouncer's first, the ids it assigns; they are recorded with the
 *     outcome, as that step has always recorded them. Null records the outcome alone.
 */
public record DebounceCaller(
    String workflowId, int stepId, String stepName, Debouncer.@Nullable DebounceIds ids) {}
