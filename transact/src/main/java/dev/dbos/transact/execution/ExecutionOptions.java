package dev.dbos.transact.execution;

import static dev.dbos.transact.internal.Validation.nullableIsEmpty;
import static dev.dbos.transact.internal.Validation.nullableIsNotPositive;

import dev.dbos.transact.DBOSClient;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.workflow.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

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
    String authenticatedUser,
    String assumedRole,
    List<String> authenticatedRoles,
    // True when re-executing a workflow that previously crashed; skips re-enqueue guards.
    boolean isRecoveryRequest,
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

    authenticatedRoles = authenticatedRoles != null ? List.copyOf(authenticatedRoles) : null;
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
        null,
        null,
        null,
        false,
        false);
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
        null,
        null,
        null,
        null,
        false,
        false);
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
        null,
        null,
        null,
        null,
        false,
        false);
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
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
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
        partitionKey,
        this.delay,
        this.appVersion,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        false,
        true);
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
        this.serialization,
        options.authenticatedUser(),
        options.assumedRole(),
        options.authenticatedRoles(),
        this.isRecoveryRequest,
        this.isDequeuedRequest);
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
        this.serialization,
        options.authenticatedUser(),
        options.assumedRole(),
        options.authenticatedRoles(),
        this.isRecoveryRequest,
        this.isDequeuedRequest);
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
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
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
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
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
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.isRecoveryRequest,
        this.isDequeuedRequest);
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
        this.serialization,
        authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.isRecoveryRequest,
        this.isDequeuedRequest);
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
        this.serialization,
        this.authenticatedUser,
        assumedRole,
        this.authenticatedRoles,
        this.isRecoveryRequest,
        this.isDequeuedRequest);
  }

  public ExecutionOptions withAuthenticatedRoles(List<String> authenticatedRoles) {
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
        this.authenticatedUser,
        this.assumedRole,
        authenticatedRoles,
        this.isRecoveryRequest,
        this.isDequeuedRequest);
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
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.isRecoveryRequest,
        this.isDequeuedRequest);
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
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.isRecoveryRequest,
        this.isDequeuedRequest);
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
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.isRecoveryRequest,
        this.isDequeuedRequest);
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
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
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
