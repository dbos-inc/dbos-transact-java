package dev.dbos.transact.workflow;

import java.time.Instant;
import java.util.List;

/**
 * Input for {@code getWorkflowAggregates}. At least one {@code groupBy*} flag must be {@code true}
 * or the call will throw {@link IllegalArgumentException}.
 *
 * <p>The {@code workflowIdPrefix} list is OR'd: a row matches if its workflow UUID starts with any
 * of the supplied prefixes.
 */
public record GetWorkflowAggregatesInput(
    // group-by dimension flags
    boolean groupByStatus,
    boolean groupByName,
    boolean groupByQueueName,
    boolean groupByExecutorId,
    boolean groupByApplicationVersion,
    // filters
    List<String> workflowName,
    List<String> status,
    List<String> queueName,
    List<String> executorIds,
    List<String> applicationVersion,
    List<String> workflowIdPrefix,
    Instant startTime,
    Instant endTime) {

  public GetWorkflowAggregatesInput() {
    this(false, false, false, false, false, null, null, null, null, null, null, null, null);
  }

  public GetWorkflowAggregatesInput withGroupByStatus(boolean groupByStatus) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime);
  }

  public GetWorkflowAggregatesInput withGroupByName(boolean groupByName) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime);
  }

  public GetWorkflowAggregatesInput withGroupByQueueName(boolean groupByQueueName) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime);
  }

  public GetWorkflowAggregatesInput withGroupByExecutorId(boolean groupByExecutorId) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime);
  }

  public GetWorkflowAggregatesInput withGroupByApplicationVersion(
      boolean groupByApplicationVersion) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime);
  }

  public GetWorkflowAggregatesInput withWorkflowName(List<String> workflowName) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime);
  }

  public GetWorkflowAggregatesInput withStatus(List<String> status) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime);
  }

  public GetWorkflowAggregatesInput withQueueName(List<String> queueName) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime);
  }

  public GetWorkflowAggregatesInput withExecutorIds(List<String> executorIds) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime);
  }

  public GetWorkflowAggregatesInput withApplicationVersion(List<String> applicationVersion) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime);
  }

  public GetWorkflowAggregatesInput withWorkflowIdPrefix(List<String> workflowIdPrefix) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime);
  }

  public GetWorkflowAggregatesInput withStartTime(Instant startTime) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime);
  }

  public GetWorkflowAggregatesInput withEndTime(Instant endTime) {
    return new GetWorkflowAggregatesInput(
        groupByStatus,
        groupByName,
        groupByQueueName,
        groupByExecutorId,
        groupByApplicationVersion,
        workflowName,
        status,
        queueName,
        executorIds,
        applicationVersion,
        workflowIdPrefix,
        startTime,
        endTime);
  }
}
