package dev.dbos.transact.workflow;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
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
 * <p>Time bucket: when {@code timeBucketSize} is set, an additional {@code "time_bucket"} dimension
 * is added (bucketed on {@code completed_at_epoch_ms}). Must be {@code > 0}.
 */
public record GetStepAggregatesInput(
    boolean groupByFunctionName,
    boolean groupByStatus,
    boolean selectCount,
    boolean selectMaxDuration,
    Duration timeBucketSize,
    List<String> status,
    List<String> functionName,
    List<String> workflowIdPrefix,
    Instant completedAfter,
    Instant completedBefore,
    /**
     * Aggregate only steps owned by these applications, plus unclaimed ones. Unset covers this
     * application's own; explicitly empty covers every application's.
     */
    List<String> applicationName) {

  public GetStepAggregatesInput {
    if (timeBucketSize != null && (timeBucketSize.isNegative() || timeBucketSize.isZero())) {
      throw new IllegalArgumentException("timeBucketSize must be > 0");
    }
  }

  /**
   * Constructs an input with no application filter, which covers this application's own steps plus
   * unclaimed ones.
   */
  public GetStepAggregatesInput(
      boolean groupByFunctionName,
      boolean groupByStatus,
      boolean selectCount,
      boolean selectMaxDuration,
      Duration timeBucketSize,
      List<String> status,
      List<String> functionName,
      List<String> workflowIdPrefix,
      Instant completedAfter,
      Instant completedBefore) {
    this(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDuration,
        timeBucketSize,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore,
        null);
  }

  public GetStepAggregatesInput() {
    this(false, false, true, false, null, null, null, null, null, null, null);
  }

  public GetStepAggregatesInput withGroupByFunctionName(boolean groupByFunctionName) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDuration,
        timeBucketSize,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore,
        applicationName);
  }

  public GetStepAggregatesInput withGroupByStatus(boolean groupByStatus) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDuration,
        timeBucketSize,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore,
        applicationName);
  }

  public GetStepAggregatesInput withSelectCount(boolean selectCount) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDuration,
        timeBucketSize,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore,
        applicationName);
  }

  public GetStepAggregatesInput withSelectMaxDuration(boolean selectMaxDuration) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDuration,
        timeBucketSize,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore,
        applicationName);
  }

  public GetStepAggregatesInput withTimeBucketSize(Duration timeBucketSize) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDuration,
        timeBucketSize,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore,
        applicationName);
  }

  public GetStepAggregatesInput withStatus(List<String> status) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDuration,
        timeBucketSize,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore,
        applicationName);
  }

  public GetStepAggregatesInput withStatus(String... status) {
    return withStatus(Arrays.asList(status));
  }

  public GetStepAggregatesInput withFunctionName(List<String> functionName) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDuration,
        timeBucketSize,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore,
        applicationName);
  }

  public GetStepAggregatesInput withWorkflowIdPrefix(List<String> workflowIdPrefix) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDuration,
        timeBucketSize,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore,
        applicationName);
  }

  public GetStepAggregatesInput withCompletedAfter(Instant completedAfter) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDuration,
        timeBucketSize,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore,
        applicationName);
  }

  public GetStepAggregatesInput withApplicationName(List<String> applicationName) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDuration,
        timeBucketSize,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore,
        applicationName);
  }

  public GetStepAggregatesInput withCompletedBefore(Instant completedBefore) {
    return new GetStepAggregatesInput(
        groupByFunctionName,
        groupByStatus,
        selectCount,
        selectMaxDuration,
        timeBucketSize,
        status,
        functionName,
        workflowIdPrefix,
        completedAfter,
        completedBefore,
        applicationName);
  }
}
