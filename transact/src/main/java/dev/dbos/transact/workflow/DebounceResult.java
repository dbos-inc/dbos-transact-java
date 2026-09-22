package dev.dbos.transact.workflow;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * The outcome of a bounce: an attempt to extend a debounced DELAYED workflow's delay and replace
 * its inputs. Either the workflow was extended, or it was not and the pair's current holder, if
 * any, is reported so the caller can decide what to do about it.
 */
public sealed interface DebounceResult permits DebounceResult.Bounced, DebounceResult.NotBounced {

  /**
   * An existing debounced workflow was extended.
   *
   * @param bouncedWorkflowId the workflow that now carries the caller's delay and inputs
   */
  record Bounced(@NonNull String bouncedWorkflowId) implements DebounceResult {}

  /**
   * Nothing was extended.
   *
   * @param holder the workflow currently holding the {@code (queue_name, deduplication_id)} pair,
   *     or null if the pair is unheld
   */
  record NotBounced(@Nullable DeduplicationHolder holder) implements DebounceResult {}
}
