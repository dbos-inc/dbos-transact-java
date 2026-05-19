package dev.dbos.transact.workflow.internal;

/**
 * Internal interface for the debouncer service workflow. Registered automatically by DBOS during
 * construction so users do not need to declare it.
 *
 * <p>Not part of the public API.
 */
public interface DebouncerService {

  /**
   * The debouncer service workflow.
   *
   * @param options identifies the user workflow to start and the absolute timeout
   * @param ctx caller context forwarded to the user workflow
   * @param initial initial debounce message from the first caller
   * @return the user workflow id (the same value carried in {@code ctx.userWorkflowId()})
   */
  String debouncerWorkflow(
      DebouncerOptions options, DebouncerContextOptions ctx, DebouncerMessage initial);
}
