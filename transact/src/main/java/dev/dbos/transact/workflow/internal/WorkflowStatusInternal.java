package dev.dbos.transact.workflow.internal;

import static dev.dbos.transact.internal.Validation.nullableIsEmpty;
import static dev.dbos.transact.internal.Validation.nullableIsNotPositive;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;

public record WorkflowStatusInternal(
    String workflowId,
    String workflowName,
    String className,
    String instanceName,
    String queueName,
    String deduplicationId,
    Integer priority,
    String queuePartitionKey,
    Duration delay,
    String authenticatedUser,
    String assumedRole,
    List<String> authenticatedRoles,
    String inputs,
    String executorId,
    String appVersion,
    String appId,
    Duration timeout,
    Instant deadline,
    String parentWorkflowId,
    String serialization,
    /** Custom JSON-serializable key-value attributes attached to the workflow at creation. */
    Map<String, Object> attributes,
    /** Name of the schedule that triggered this workflow, if any. Set only by the scheduler. */
    String scheduleName,
    /**
     * The application that owns this workflow, and whose executors therefore dequeue and recover
     * it. Null takes the writing handle's own application, which is what every path but a
     * cross-application enqueue wants.
     */
    String applicationName,
    /**
     * Whether this is a debounced workflow, whose deduplication ID is a debounce key cleared when
     * it leaves DELAYED. Set only by the debouncers.
     */
    boolean isDebounced,
    /**
     * The latest a debounced workflow's delay may be extended to, by its first delay or by any
     * later bounce; null for no cap. Set only by the debouncers.
     */
    Instant debounceDeadline) {

  public WorkflowStatusInternal {
    if (nullableIsEmpty(workflowId)) {
      throw new IllegalArgumentException("workflowId must not be empty");
    }
    if (nullableIsEmpty(workflowName)) {
      throw new IllegalArgumentException("workflowName must not be empty");
    }
    if (nullableIsEmpty(className)) {
      throw new IllegalArgumentException("className must not be empty");
    }
    if (nullableIsEmpty(instanceName)) {
      throw new IllegalArgumentException("instanceName must not be empty");
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
    if (isDebounced && (queueName == null || delay == null)) {
      throw new IllegalArgumentException("a debounced workflow needs a queue and a delay");
    }
    if (debounceDeadline != null && !isDebounced) {
      throw new IllegalArgumentException("only a debounced workflow takes a debounce deadline");
    }
    // Normalize empty strings to null for auth fields — other SDKs (TypeScript, Go) send ""
    // rather than null when auth context is absent, so we treat them equivalently.
    authenticatedUser =
        (authenticatedUser != null && authenticatedUser.isEmpty()) ? null : authenticatedUser;
    assumedRole = (assumedRole != null && assumedRole.isEmpty()) ? null : assumedRole;
    authenticatedRoles = authenticatedRoles != null ? List.copyOf(authenticatedRoles) : null;

    if (nullableIsEmpty(inputs)) {
      throw new IllegalArgumentException("inputs must not be empty");
    }
    if (nullableIsEmpty(appVersion)) {
      throw new IllegalArgumentException("appVersion must not be empty");
    }
    // Note, appId can be empty
    if (nullableIsNotPositive(timeout)) {
      throw new IllegalArgumentException("timeout must be a positive non-zero duration");
    }
    if (nullableIsEmpty(parentWorkflowId)) {
      throw new IllegalArgumentException("parentWorkflowId must not be empty");
    }
    if (nullableIsEmpty(serialization)) {
      throw new IllegalArgumentException("serialization must not be empty");
    }
  }

  @JsonIgnore
  public Long timeoutMs() {
    return timeout == null ? null : timeout.toMillis();
  }

  @JsonIgnore
  public Long delayMs() {
    return delay == null ? null : delay.toMillis();
  }

  @JsonIgnore
  public Long debounceDeadlineEpochMs() {
    return debounceDeadline == null ? null : debounceDeadline.toEpochMilli();
  }

  @JsonIgnore
  public Long deadlineEpochMs() {
    return deadline == null ? null : deadline.toEpochMilli();
  }
}
