package dev.dbos.transact.workflow;

import static dev.dbos.transact.internal.Validation.validateAttributes;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Input for {@code getWorkflowAggregates}.
 *
 * <p>At least one {@code groupBy*} flag must be {@code true}, or the call will throw {@link
 * IllegalArgumentException}. At least one {@code select*} flag must be {@code true}, or the call
 * will throw {@link IllegalArgumentException}.
 *
 * <p>The {@code workflowIdPrefix} list is OR'd: a row matches if its workflow UUID starts with any
 * of the supplied prefixes.
 *
 * <p>Time bucket: when {@code timeBucketSize} is set, an additional {@code "time_bucket"} dimension
 * is added to every group, containing the bucket start (ms since epoch, aligned to the bucket
 * size). {@code timeBucketSize} must be {@code > 0}.
 */
public record GetWorkflowAggregatesInput(
    // group-by dimension flags
    boolean groupByStatus,
    boolean groupByName,
    boolean groupByQueueName,
    boolean groupByExecutorId,
    boolean groupByApplicationVersion,
    // select metric flags
    boolean selectCount,
    boolean selectMinCreatedAt,
    boolean selectMaxQueueWait,
    boolean selectMaxTotalLatency,
    // optional time bucketing; null = no bucketing
    Duration timeBucketSize,
    // filters
    List<String> workflowName,
    List<String> status,
    List<String> queueName,
    List<String> executorIds,
    List<String> applicationVersion,
    List<String> workflowIdPrefix,
    Instant startTime,
    Instant endTime,
    Instant completedAfter,
    Instant completedBefore,
    Instant dequeuedAfter,
    Instant dequeuedBefore,
    Map<String, Object> attributes) {

  public GetWorkflowAggregatesInput {
    if (timeBucketSize != null && (timeBucketSize.isNegative() || timeBucketSize.isZero())) {
      throw new IllegalArgumentException("timeBucketSize must be > 0");
    }
    attributes = validateAttributes(attributes);
  }

  /** Constructs a default input with {@code selectCount=true} and no group-by or filter flags. */
  public GetWorkflowAggregatesInput() {
    this(
        false, false, false, false, false, true, false, false, false, null, null, null, null, null,
        null, null, null, null, null, null, null, null, null);
  }

  public GetWorkflowAggregatesInput withGroupByStatus(boolean groupByStatus) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withGroupByName(boolean groupByName) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withGroupByQueueName(boolean groupByQueueName) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withGroupByExecutorId(boolean groupByExecutorId) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withGroupByApplicationVersion(
      boolean groupByApplicationVersion) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withSelectCount(boolean selectCount) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withSelectMinCreatedAt(boolean selectMinCreatedAt) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withSelectMaxQueueWait(boolean selectMaxQueueWait) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withSelectMaxTotalLatency(boolean selectMaxTotalLatency) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withTimeBucketSize(Duration timeBucketSize) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withWorkflowName(List<String> workflowName) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withStatus(List<String> status) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withStatus(String... status) {
    return withStatus(Arrays.asList(status));
  }

  public GetWorkflowAggregatesInput withQueueName(List<String> queueName) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withExecutorIds(List<String> executorIds) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withApplicationVersion(List<String> applicationVersion) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withWorkflowIdPrefix(List<String> workflowIdPrefix) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withStartTime(Instant startTime) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withEndTime(Instant endTime) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withCompletedAfter(Instant completedAfter) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withCompletedBefore(Instant completedBefore) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withDequeuedAfter(Instant dequeuedAfter) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withDequeuedBefore(Instant dequeuedBefore) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }

  public GetWorkflowAggregatesInput withAttributes(Map<String, Object> attributes) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        selectCount,
        selectMinCreatedAt,
        selectMaxQueueWait,
        selectMaxTotalLatency,
        timeBucketSize,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime,
        completedAfter,
        completedBefore,
        dequeuedAfter,
        dequeuedBefore,
        attributes);
  }
}
