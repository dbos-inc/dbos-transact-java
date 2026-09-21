package dev.dbos.transact.workflow;

import org.jspecify.annotations.Nullable;

/**
 * The outcome of a bounce: an attempt to extend a debounced DELAYED workflow's delay and replace
 * its inputs.
 *
 * @param bouncedWorkflowId the workflow that was extended, or null if nothing matched
 * @param holder when nothing matched, the workflow currently holding the {@code (queue_name,
 *     deduplication_id)} pair, or null if the pair is unheld
 */
public record DebounceResult(
    @Nullable String bouncedWorkflowId, @Nullable DeduplicationHolder holder) {

  /** Whether an existing debounced workflow was extended. */
  public boolean bounced() {
    return bouncedWorkflowId != null;
  }
}
