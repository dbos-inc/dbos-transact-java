package dev.dbos.transact.workflow;

import java.time.Instant;
import java.util.List;

/**
 * Input for {@code getStepAggregates}.
 *
 * <p>At least one {@code groupBy*} flag must be {@code true}, or the call will throw {@link
 * IllegalArgumentException}. At least one {@code select*} flag must be {@code true}, or the call
 * will throw {@link IllegalArgumentException}.
 *
 * <p>Status is derived from the step's {@code error} column: {@code error IS NULL} → "SUCCESS",
 * {@code error IS NOT NULL} → "ERROR".
 *
 * <p>Time bucket: when {@code timeBucketSizeMs} is set, an additional {@code "time_bucket"}
 * dimension is added (bucketed on {@code completed_at_epoch_ms}). Must be {@code > 0}.
 */
public record GetStepAggregatesInput(
    boolean groupByFunctionName,
    boolean groupByStatus,
    boolean selectCount,
    boolean selectMaxDurationMs,
    Long timeBucketSizeMs,
    List<String> status,
    List<String> functionName,
    List<String> workflowIdPrefix,
    Instant completedAfter,
    Instant completedBefore) {

  public GetStepAggregatesInput() {
    this(false, false, true, false, null, null, null, null, null, null);
  }

  public GetStepAggregatesInput withGroupByFunctionName(boolean groupByFunctionName) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDurationMs,
        timeBucketSizeMs,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore);
  }

  public GetStepAggregatesInput withGroupByStatus(boolean groupByStatus) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDurationMs,
        timeBucketSizeMs,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore);
  }

  public GetStepAggregatesInput withSelectCount(boolean selectCount) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDurationMs,
        timeBucketSizeMs,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore);
  }

  public GetStepAggregatesInput withSelectMaxDurationMs(boolean selectMaxDurationMs) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDurationMs,
        timeBucketSizeMs,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore);
  }

  public GetStepAggregatesInput withTimeBucketSizeMs(Long timeBucketSizeMs) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDurationMs,
        timeBucketSizeMs,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore);
  }

  public GetStepAggregatesInput withStatus(List<String> status) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDurationMs,
        timeBucketSizeMs,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore);
  }

  public GetStepAggregatesInput withFunctionName(List<String> functionName) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDurationMs,
        timeBucketSizeMs,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore);
  }

  public GetStepAggregatesInput withWorkflowIdPrefix(List<String> workflowIdPrefix) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDurationMs,
        timeBucketSizeMs,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore);
  }

  public GetStepAggregatesInput withCompletedAfter(Instant completedAfter) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDurationMs,
        timeBucketSizeMs,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore);
  }

  public GetStepAggregatesInput withCompletedBefore(Instant completedBefore) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDurationMs,
        timeBucketSizeMs,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore);
  }
}
