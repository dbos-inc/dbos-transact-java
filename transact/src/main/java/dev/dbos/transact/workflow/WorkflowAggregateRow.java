package dev.dbos.transact.workflow;

import java.util.Map;

/**
 * One row returned by {@code getWorkflowAggregates}. {@code group} contains an entry for each
 * dimension that was requested via a {@code groupBy*} flag in {@link GetWorkflowAggregatesInput};
 * absent dimensions are not present as keys. Values may be {@code null} when the underlying column
 * is NULL for that bucket.
 *
 * <p>Metric fields ({@code count}, {@code minCreatedAt}, {@code maxQueueWaitMs}, {@code
 * maxTotalLatencyMs}) are {@code null} when the corresponding {@code select*} flag was not set in
 * the input.
 */
public record WorkflowAggregateRow(
    Map<String, String> group,
    Long count,
    Long minCreatedAt,
    Long maxQueueWaitMs,
    Long maxTotalLatencyMs) {}
