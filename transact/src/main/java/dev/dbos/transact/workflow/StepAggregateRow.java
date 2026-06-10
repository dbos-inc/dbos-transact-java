package dev.dbos.transact.workflow;

import java.time.Duration;
import java.util.Map;

/**
 * One row returned by {@code getStepAggregates}. {@code group} contains an entry for each dimension
 * requested via {@code groupBy*} flags. Values may be {@code null} when the underlying expression
 * is NULL for that bucket (e.g. a derived status column).
 *
 * <p>Metric fields ({@code count}, {@code maxDuration}) are {@code null} when the corresponding
 * {@code select*} flag was not set in the input.
 */
public record StepAggregateRow(Map<String, String> group, Long count, Duration maxDuration) {}
