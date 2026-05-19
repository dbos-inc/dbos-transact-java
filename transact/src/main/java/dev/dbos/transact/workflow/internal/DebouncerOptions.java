package dev.dbos.transact.workflow.internal;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Inputs to the debouncer service workflow that identify the user workflow to be eventually started
 * and the optional absolute timeout cap.
 *
 * <p>Not part of the public API.
 */
public record DebouncerOptions(
    @NonNull String workflowName,
    @NonNull String className,
    @Nullable String instanceName,
    @Nullable String queueName,
    @Nullable Long debounceTimeoutMs) {}
