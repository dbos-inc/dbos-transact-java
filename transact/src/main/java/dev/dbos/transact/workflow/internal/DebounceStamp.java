package dev.dbos.transact.workflow.internal;

import org.jspecify.annotations.Nullable;

/**
 * Marks an enqueue as a debounced workflow: the row is written DELAYED until {@code
 * delayUntilEpochMs}, flagged {@code is_debounced}, and later bounces never push it past {@code
 * deadlineEpochMs}. Only the debouncers set it; no public option exposes it.
 *
 * <p>Not part of the public API.
 *
 * @param delayUntilEpochMs when the row leaves DELAYED, already capped at the deadline
 * @param deadlineEpochMs the latest a bounce may extend the delay to, or null for no cap
 */
public record DebounceStamp(long delayUntilEpochMs, @Nullable Long deadlineEpochMs) {}
