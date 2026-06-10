package dev.dbos.transact.workflow;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;

/**
 * One row returned by {@code getWorkflowAggregates}. {@code group} contains an entry for each
 * dimension that was requested via a {@code groupBy*} flag in {@link GetWorkflowAggregatesInput};
 * absent dimensions are not present as keys. Values may be {@code null} when the underlying column
 * is NULL for that bucket.
 *
 * <p>Metric fields ({@code count}, {@code minCreatedAt}, {@code maxQueueWait}, {@code
 * maxTotalLatency}) are {@code null} when the corresponding {@code select*} flag was not set in the
 * input.
 */
public record WorkflowAggregateRow(
    Map<String, String> group,
    Long count,
    Instant minCreatedAt,
    Duration maxQueueWait,
    Duration maxTotalLatency) {}
