package dev.dbos.transact.workflow;

import org.jspecify.annotations.Nullable;

/**
 * The workflow currently holding a {@code (queue_name, deduplication_id)} pair, and the application
 * that owns it.
 *
 * <p>The deduplication index is global across the applications sharing a system database, so a
 * holder may belong to a peer. Callers that mean to coordinate with the holder — the debouncers —
 * must check before they do, since a peer's holder answers to that peer alone.
 *
 * @param workflowId the holding workflow
 * @param applicationName the application that owns it, or null if the row is unclaimed
 */
public record DeduplicationHolder(String workflowId, @Nullable String applicationName) {

  /**
   * Whether this holder answers to some other application than {@code appName}. An unclaimed holder
   * belongs to everyone, and a caller acting for no application in particular is in no position to
   * call anything foreign, so neither case is foreign.
   */
  public boolean isForeignTo(@Nullable String appName) {
    return appName != null && applicationName != null && !applicationName.equals(appName);
  }
}
