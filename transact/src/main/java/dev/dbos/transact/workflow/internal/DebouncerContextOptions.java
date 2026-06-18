package dev.dbos.transact.workflow.internal;

import java.time.Duration;
import java.util.Map;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Per-call context forwarded from the caller of {@code Debouncer.debounce()} to the debouncer
 * service workflow. Contains only values that are inherently call-specific: the pre-assigned user
 * workflow ID and the caller's active timeout (if any).
 *
 * <p>Enqueue-time options (appVersion, priority, deduplicationId) are carried in {@link
 * DebouncerOptions} instead, where they are set explicitly via the Debouncer builder.
 *
 * <p>Not part of the public API.
 */
public record DebouncerContextOptions(
    @NonNull String userWorkflowId,
    @Nullable Duration workflowTimeout,
    @Nullable Map<String, Object> workflowAttributes) {}
