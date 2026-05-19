package dev.dbos.transact.workflow.internal;

import org.jspecify.annotations.NonNull;

/**
 * Message sent from a {@code Debouncer} caller to the debouncer service workflow each time the
 * debounce key fires. The debouncer service workflow uses the most recently received message's args
 * when it eventually starts the user workflow.
 *
 * <p>Not part of the public API — the debouncer infrastructure consumes this directly.
 */
public record DebouncerMessage(
    @NonNull String messageId, @NonNull Object[] args, long debouncePeriodMs) {}
