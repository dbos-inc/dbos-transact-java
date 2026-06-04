package dev.dbos.transact.execution;

import static dev.dbos.transact.internal.Validation.nullableIsEmpty;
import static dev.dbos.transact.internal.Validation.nullableIsNotPositive;

import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.StartWorkflowOptions;
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
    boolean isRecoveryRequest,
    boolean isDequeuedRequest,
    String serialization,
    String authenticatedUser,
    String assumedRole,
    String[] authenticatedRoles) {
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

  public ExecutionOptions(String workflowId) {
    this(
        workflowId,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        false,
        false,
        null,
        null,
        null,
        null);
  }

  public ExecutionOptions(String workflowId, Duration timeout, Instant deadline) {
    this(
        workflowId,
        Timeout.of(timeout),
        deadline,
        null,
        null,
        null,
        null,
        null,
        null,
        false,
        false,
        null,
        null,
        null,
        null);
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
        true,
        false,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  public ExecutionOptions asDequeuedRequest() {
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
        false,
        true,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  public ExecutionOptions withOptions(DBOSClient.EnqueueOptions options) {
    return new ExecutionOptions(
        this.workflowId,
        Timeout.of(options.timeout()),
        options.deadline(),
        options.queueName(),
        options.deduplicationId(),
        options.priority(),
        options.queuePartitionKey(),
        options.delay(),
        options.appVersion(),
        this.isRecoveryRequest,
        this.isDequeuedRequest,
        this.serialization,
        options.authenticatedUser(),
        options.assumedRole(),
        options.authenticatedRoles());
  }

  public ExecutionOptions withOptions(StartWorkflowOptions options) {
    if (options == null) {
      return this;
    }
    return new ExecutionOptions(
        this.workflowId,
        options.timeout(),
        options.deadline(),
        options.queueName(),
        options.deduplicationId(),
        options.priority(),
        options.queuePartitionKey(),
        options.delay(),
        options.appVersion(),
        this.isRecoveryRequest,
        this.isDequeuedRequest,
        this.serialization,
        options.authenticatedUser(),
        options.assumedRole(),
        options.authenticatedRoles());
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
        this.isRecoveryRequest,
        this.isDequeuedRequest,
        serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
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
        this.isRecoveryRequest,
        this.isDequeuedRequest,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
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
        this.isRecoveryRequest,
        this.isDequeuedRequest,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  public ExecutionOptions withAuthenticatedUser(String authenticatedUser) {
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
        this.isRecoveryRequest,
        this.isDequeuedRequest,
        this.serialization,
        authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  public ExecutionOptions withAssumedRole(String assumedRole) {
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
        this.isRecoveryRequest,
        this.isDequeuedRequest,
        this.serialization,
        this.authenticatedUser,
        assumedRole,
        this.authenticatedRoles);
  }

  public ExecutionOptions withAuthenticatedRoles(String[] authenticatedRoles) {
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
        this.isRecoveryRequest,
        this.isDequeuedRequest,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        authenticatedRoles);
  }

  public ExecutionOptions withQueueName(String queueName) {
    return new ExecutionOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.appVersion,
        this.isRecoveryRequest,
        this.isDequeuedRequest,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  public ExecutionOptions withTimeout(Duration timeout) {
    return new ExecutionOptions(
        this.workflowId,
        Timeout.of(timeout),
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.appVersion,
        this.isRecoveryRequest,
        this.isDequeuedRequest,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  public ExecutionOptions withTimeout(Timeout timeout) {
    return new ExecutionOptions(
        this.workflowId,
        timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.appVersion,
        this.isRecoveryRequest,
        this.isDequeuedRequest,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  public ExecutionOptions withDeadline(Instant deadline) {
    return new ExecutionOptions(
        this.workflowId,
        this.timeout,
        deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.appVersion,
        this.isRecoveryRequest,
        this.isDequeuedRequest,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  public Duration timeoutDuration() {
    if (timeout instanceof Timeout.Explicit e) {
      return e.value();
    }
    return null;
  }
}
