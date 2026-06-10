package dev.dbos.transact.workflow;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.Nullable;

/**
 * Options for forking a workflow. This includes: Specified ID for the new workflow Application
 * version to use for executing the new workflow Timeout to apply for the new workflow execution
 */
public record ForkOptions(
    @Nullable String forkedWorkflowId,
    @Nullable String applicationVersion,
    @Nullable Duration timeout,
    @Nullable String queueName,
    @Nullable String queuePartitionKey) {

  public ForkOptions {
    if (timeout != null && (timeout.isNegative() || timeout.isZero())) {
      throw new IllegalArgumentException(
          "ForkOptions timeout must be a positive non-zero duration");
    }
  }

  public ForkOptions() {
    this(null, null, null, null, null);
  }

  /** Assign the workflow ID for the new workflow */
  public ForkOptions(String forkedWorkflowId) {
    this(forkedWorkflowId, null, null, null, null);
  }

  /**
   * Returns a copy of this object with the given forkedWorkflowId.
   *
   * @param forkedWorkflowId ID to assign to the forked workflow.
   */
  public ForkOptions withForkedWorkflowId(@Nullable String forkedWorkflowId) {
    return new ForkOptions(
        forkedWorkflowId,
        this.applicationVersion,
        this.timeout,
        this.queueName,
        this.queuePartitionKey);
  }

  /**
   * Returns a copy of this object with the given applicationVersion.
   *
   * @param applicationVersion Application version to use for the new fork of the workflow
   */
  public ForkOptions withApplicationVersion(@Nullable String applicationVersion) {
    return new ForkOptions(
        this.forkedWorkflowId,
        applicationVersion,
        this.timeout,
        this.queueName,
        this.queuePartitionKey);
  }

  /**
   * Returns a copy of this object with the given timeout.
   *
   * @param timeout Duration to allow for the workflow to run, before canceling the workflow
   */
  public ForkOptions withTimeout(@Nullable Duration timeout) {
    return new ForkOptions(
        this.forkedWorkflowId,
        this.applicationVersion,
        timeout,
        this.queueName,
        this.queuePartitionKey);
  }

  public ForkOptions withTimeout(long value, TimeUnit unit) {
    return withTimeout(Duration.ofNanos(unit.toNanos(value)));
  }

  /**
   * Returns a copy of this object with the given queueName.
   *
   * @param queue Queue to assign to the forked workflow
   */
  public ForkOptions withQueue(Queue queue) {
    return new ForkOptions(
        this.forkedWorkflowId,
        this.applicationVersion,
        this.timeout,
        queue.name(),
        this.queuePartitionKey);
  }

  /**
   * Returns a copy of this object with the given queueName.
   *
   * @param queueName Queue name to assign to the forked workflow
   */
  public ForkOptions withQueue(@Nullable String queueName) {
    return new ForkOptions(
        this.forkedWorkflowId,
        this.applicationVersion,
        this.timeout,
        queueName,
        this.queuePartitionKey);
  }

  /**
   * Returns a copy of this object with the given queuePartitionKey.
   *
   * @param queuePartitionKey Queue partition key to assign to the forked workflow
   */
  public ForkOptions withQueuePartitionKey(@Nullable String queuePartitionKey) {
    return new ForkOptions(
        this.forkedWorkflowId,
        this.applicationVersion,
        this.timeout,
        this.queueName,
        queuePartitionKey);
  }
}
