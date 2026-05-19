package dev.dbos.transact.workflow.internal;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Context captured from the caller of {@code Debouncer.debounce()} and forwarded to the user
 * workflow when it is eventually started. The {@code userWorkflowId} is pre-assigned by the caller
 * so that the caller can return a handle pointing to the future workflow.
 *
 * <p>Not part of the public API.
 */
public record DebouncerContextOptions(
    @NonNull String userWorkflowId,
    @Nullable String deduplicationId,
    @Nullable Integer priority,
    @Nullable String appVersion,
    @Nullable Long workflowTimeoutMs) {}
