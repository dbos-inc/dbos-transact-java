package dev.dbos.transact.workflow;

import org.jspecify.annotations.Nullable;

/**
 * Options for forking a workflow from a failure point. Exactly one of fromLastFailure,
 * fromLastStep, fromStep, or fromStepName must be specified via the with* builder methods.
 */
public record ForkFromFailureOptions(
    @Nullable String applicationVersion,
    @Nullable String queueName,
    @Nullable String queuePartitionKey,
    @Nullable Boolean fromLastFailure,
    @Nullable Boolean fromLastStep,
    @Nullable Integer fromStep,
    @Nullable String fromStepName) {

  public ForkFromFailureOptions() {
    this(null, null, null, null, null, null, null);
  }

  /**
   * Returns a copy of this object with the given applicationVersion.
   *
   * @param applicationVersion Application version to use for the new fork of the workflow
   */
  public ForkFromFailureOptions withApplicationVersion(String applicationVersion) {
    return new ForkFromFailureOptions(
        applicationVersion,
        this.queueName,
        this.queuePartitionKey,
        this.fromLastFailure,
        this.fromLastStep,
        this.fromStep,
        this.fromStepName);
  }

  /**
   * Returns a copy of this object with the given queue.
   *
   * @param queue Queue to assign to the forked workflow
   */
  public ForkFromFailureOptions withQueue(Queue queue) {
    return withQueue(queue.name());
  }

  /**
   * Returns a copy of this object with the given queueName.
   *
   * @param queueName Queue name to assign to the forked workflow
   */
  public ForkFromFailureOptions withQueue(String queueName) {
    return new ForkFromFailureOptions(
        this.applicationVersion,
        queueName,
        this.queuePartitionKey,
        this.fromLastFailure,
        this.fromLastStep,
        this.fromStep,
        this.fromStepName);
  }

  /**
   * Returns a copy of this object with the given queuePartitionKey.
   *
   * @param queuePartitionKey Queue partition key to assign to the forked workflow
   */
  public ForkFromFailureOptions withQueuePartitionKey(String queuePartitionKey) {
    return new ForkFromFailureOptions(
        this.applicationVersion,
        this.queueName,
        queuePartitionKey,
        this.fromLastFailure,
        this.fromLastStep,
        this.fromStep,
        this.fromStepName);
  }

  /**
   * Fork from the last step that recorded an error. If no step has an error, falls back to the last
   * step executed.
   */
  public ForkFromFailureOptions withFromLastFailure() {
    return new ForkFromFailureOptions(
        this.applicationVersion,
        this.queueName,
        this.queuePartitionKey,
        true,
        this.fromLastStep,
        this.fromStep,
        this.fromStepName);
  }

  /** Fork from the last step executed, regardless of success or failure. */
  public ForkFromFailureOptions withFromLastStep() {
    return new ForkFromFailureOptions(
        this.applicationVersion,
        this.queueName,
        this.queuePartitionKey,
        this.fromLastFailure,
        true,
        this.fromStep,
        this.fromStepName);
  }

  /**
   * Fork from a specific step number. Steps before this step are copied; execution starts at this
   * step.
   *
   * @param step The step number (0-indexed function_id) to start execution from
   */
  public ForkFromFailureOptions withFromStep(int step) {
    return new ForkFromFailureOptions(
        this.applicationVersion,
        this.queueName,
        this.queuePartitionKey,
        this.fromLastFailure,
        this.fromLastStep,
        step,
        this.fromStepName);
  }

  /**
   * Fork from the last occurrence of a step with the given function name.
   *
   * @param stepName The function name of the step to fork from
   */
  public ForkFromFailureOptions withFromStepName(String stepName) {
    return new ForkFromFailureOptions(
        this.applicationVersion,
        this.queueName,
        this.queuePartitionKey,
        this.fromLastFailure,
        this.fromLastStep,
        this.fromStep,
        stepName);
  }
}
