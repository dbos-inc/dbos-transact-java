package dev.dbos.transact.workflow;

import java.util.Objects;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Mode selection for fork-from-failure operations. Construct one of the four permitted subtypes,
 * then optionally chain {@code with*} calls for common options.
 *
 * <ul>
 *   <li>{@link FromLastFailure} – re-execute from the last step that recorded an error (falls back
 *       to the last step if no error exists)
 *   <li>{@link FromLastStep} – re-execute from the last step executed
 *   <li>{@link FromStep} – re-execute from a specific step number
 *   <li>{@link FromStepName} – re-execute from the last occurrence of a named step
 * </ul>
 */
public sealed interface ForkFromFailureOptions
    permits ForkFromFailureOptions.FromLastFailure,
        ForkFromFailureOptions.FromLastStep,
        ForkFromFailureOptions.FromStep,
        ForkFromFailureOptions.FromStepName {

  @Nullable String applicationVersion();

  @Nullable String queueName();

  @Nullable String queuePartitionKey();

  ForkFromFailureOptions withApplicationVersion(@Nullable String applicationVersion);

  /** Shorthand for {@link #withQueue(String)}. */
  default ForkFromFailureOptions withQueue(Queue queue) {
    return withQueue(queue.name());
  }

  ForkFromFailureOptions withQueue(@Nullable String queueName);

  ForkFromFailureOptions withQueuePartitionKey(@Nullable String queuePartitionKey);

  /**
   * Fork from the last step that recorded an error. Falls back to the last step executed if no step
   * has an error.
   */
  record FromLastFailure(
      @Nullable String applicationVersion,
      @Nullable String queueName,
      @Nullable String queuePartitionKey)
      implements ForkFromFailureOptions {

    public FromLastFailure() {
      this(null, null, null);
    }

    @Override
    public ForkFromFailureOptions withApplicationVersion(@Nullable String applicationVersion) {
      return new FromLastFailure(applicationVersion, queueName, queuePartitionKey);
    }

    @Override
    public ForkFromFailureOptions withQueue(@Nullable String queueName) {
      return new FromLastFailure(applicationVersion, queueName, queuePartitionKey);
    }

    @Override
    public ForkFromFailureOptions withQueuePartitionKey(@Nullable String queuePartitionKey) {
      return new FromLastFailure(applicationVersion, queueName, queuePartitionKey);
    }
  }

  /** Fork from the last step executed, regardless of success or failure. */
  record FromLastStep(
      @Nullable String applicationVersion,
      @Nullable String queueName,
      @Nullable String queuePartitionKey)
      implements ForkFromFailureOptions {

    public FromLastStep() {
      this(null, null, null);
    }

    @Override
    public ForkFromFailureOptions withApplicationVersion(@Nullable String applicationVersion) {
      return new FromLastStep(applicationVersion, queueName, queuePartitionKey);
    }

    @Override
    public ForkFromFailureOptions withQueue(@Nullable String queueName) {
      return new FromLastStep(applicationVersion, queueName, queuePartitionKey);
    }

    @Override
    public ForkFromFailureOptions withQueuePartitionKey(@Nullable String queuePartitionKey) {
      return new FromLastStep(applicationVersion, queueName, queuePartitionKey);
    }
  }

  /**
   * Fork from a specific step number (0-indexed {@code function_id}). Steps before this are copied;
   * execution starts at this step.
   */
  record FromStep(
      int step,
      @Nullable String applicationVersion,
      @Nullable String queueName,
      @Nullable String queuePartitionKey)
      implements ForkFromFailureOptions {

    public FromStep(int step) {
      this(step, null, null, null);
    }

    @Override
    public ForkFromFailureOptions withApplicationVersion(@Nullable String applicationVersion) {
      return new FromStep(step, applicationVersion, queueName, queuePartitionKey);
    }

    @Override
    public ForkFromFailureOptions withQueue(@Nullable String queueName) {
      return new FromStep(step, applicationVersion, queueName, queuePartitionKey);
    }

    @Override
    public ForkFromFailureOptions withQueuePartitionKey(@Nullable String queuePartitionKey) {
      return new FromStep(step, applicationVersion, queueName, queuePartitionKey);
    }
  }

  /** Fork from the last occurrence of a step with the given function name. */
  record FromStepName(
      @NonNull String stepName,
      @Nullable String applicationVersion,
      @Nullable String queueName,
      @Nullable String queuePartitionKey)
      implements ForkFromFailureOptions {

    public FromStepName {
      Objects.requireNonNull(stepName, "stepName must not be null");
    }

    public FromStepName(String stepName) {
      this(stepName, null, null, null);
    }

    @Override
    public ForkFromFailureOptions withApplicationVersion(@Nullable String applicationVersion) {
      return new FromStepName(stepName, applicationVersion, queueName, queuePartitionKey);
    }

    @Override
    public ForkFromFailureOptions withQueue(@Nullable String queueName) {
      return new FromStepName(stepName, applicationVersion, queueName, queuePartitionKey);
    }

    @Override
    public ForkFromFailureOptions withQueuePartitionKey(@Nullable String queuePartitionKey) {
      return new FromStepName(stepName, applicationVersion, queueName, queuePartitionKey);
    }
  }
}
