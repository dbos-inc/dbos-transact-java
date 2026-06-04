package dev.dbos.transact.execution;

import static dev.dbos.transact.internal.Validation.nullableIsEmpty;
import static dev.dbos.transact.internal.Validation.nullableIsNotPositive;

import dev.dbos.transact.workflow.Timeout;

import java.time.Duration;
import java.time.Instant;

// Internal execution options record. External API specific records such as StartWorkflowOptions,
// WorkflowOptions and DBOSClient.EnqueueOptions are converted to ExecutionOptions before execution.

public record ExecutionOptions(
    String workflowId,
    Timeout timeout,
    Instant deadline,
    String queueName,
    String deduplicationId,
    Integer priority,
    String queuePartitionKey,
    Duration delay,
    String appVersion,
    String serialization,
    // True when re-executing a workflow that previously crashed; skips re-enqueue guards.
    boolean isRecoveryRequest,
    // True when the workflow was pulled off a queue by this executor, not started directly.
    boolean isDequeuedRequest) {
  public ExecutionOptions {
    if (nullableIsEmpty(workflowId)) {
      throw new IllegalArgumentException("workflowId must not be empty");
    }

    if (timeout instanceof Timeout.Explicit explicit && nullableIsNotPositive(explicit.value())) {
      throw new IllegalArgumentException("explicit timeout must be a positive non-zero duration");
    }

    if (nullableIsEmpty(queueName)) {
      throw new IllegalArgumentException("queueName must not be empty");
    }

    if (nullableIsEmpty(deduplicationId)) {
      throw new IllegalArgumentException("deduplicationId must not be empty");
    }

    if (nullableIsEmpty(queuePartitionKey)) {
      throw new IllegalArgumentException("queuePartitionKey must not be empty");
    }

    if (nullableIsNotPositive(delay)) {
      throw new IllegalArgumentException("delay must be a positive non-zero duration");
    }

    if (nullableIsEmpty(appVersion)) {
      throw new IllegalArgumentException("appVersion must not be empty");
    }

    if (nullableIsEmpty(serialization)) {
      throw new IllegalArgumentException("serialization must not be empty");
    }
  }

  public ExecutionOptions(
      String workflowId,
      Timeout timeout,
      Instant deadline,
      String queueName,
      String deduplicationId,
      Integer priority,
      String queuePartitionKey,
      Duration delay,
      String appVersion,
      String serialization) {
    this(
        workflowId,
        timeout,
        deadline,
        queueName,
        deduplicationId,
        priority,
        queuePartitionKey,
        delay,
        appVersion,
        serialization,
        false,
        false);
  }

  public ExecutionOptions(String workflowId) {
    this(workflowId, null, null, null, null, null, null, null, null, null);
  }

  public ExecutionOptions(String workflowId, Duration timeout, Instant deadline) {
    this(workflowId, Timeout.of(timeout), deadline, null, null, null, null, null, null, null);
  }

  public ExecutionOptions asRecoveryRequest() {
    return new ExecutionOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.appVersion,
        this.serialization,
        true,
        false);
  }

  public ExecutionOptions asDequeuedRequest(String queueName, String partitionKey) {
    return new ExecutionOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        queueName,
        this.deduplicationId,
        this.priority,
        queuePartitionKey,
        this.delay,
        this.appVersion,
        this.serialization,
        false,
        true);
  }

  public ExecutionOptions withSerialization(String serialization) {
    return new ExecutionOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.appVersion,
        serialization,
        this.isRecoveryRequest,
        this.isDequeuedRequest);
  }

  public ExecutionOptions withPriority(Integer priority) {
    return new ExecutionOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        priority,
        this.queuePartitionKey,
        this.delay,
        this.appVersion,
        this.serialization,
        this.isRecoveryRequest,
        this.isDequeuedRequest);
  }

  public ExecutionOptions withAppVersion(String appVersion) {
    return new ExecutionOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        appVersion,
        this.serialization,
        this.isRecoveryRequest,
        this.isDequeuedRequest);
  }

  public Duration timeoutDuration() {
    if (timeout instanceof Timeout.Explicit e) {
      return e.value();
    }
    return null;
  }
}
