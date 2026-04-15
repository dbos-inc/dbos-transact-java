package dev.dbos.transact.workflow;

import java.util.Map;

/**
 * One row returned by {@code getWorkflowAggregates}. {@code group} contains an entry for each
 * dimension that was requested via a {@code groupBy*} flag in {@link GetWorkflowAggregatesInput};
 * absent dimensions are not present as keys. Values may be {@code null} when the underlying column
 * is NULL for that bucket.
 */
public record WorkflowAggregateRow(Map<String, String> group, long count) {}
