package dev.dbos.transact;

import static dev.dbos.transact.internal.Validation.nullableIsEmpty;
import static dev.dbos.transact.internal.Validation.nullableIsNotPositive;

import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Options for starting a workflow instance.
 *
 * <p>This record encapsulates configuration for workflow execution, including:
 *
 * <ul>
 *   <li>Idempotency and tracking via a unique workflow ID
 *   <li>Timeout and deadline management
 *   <li>Queue assignment, deduplication, priority, and partitioning
 *   <li>Optional execution delay and application version targeting
 *   <li>Authentication context (user, assumed role, roles)
 * </ul>
 *
 * @param workflowId The unique identifier for the workflow instance. Used for idempotency and
 *     tracking. May be null.
 * @param timeout The timeout configuration specifying how long the workflow may run before
 *     expiring. Promoted to a deadline at execution time. May be null.
 * @param deadline The absolute time by which the workflow must start or complete before being
 *     canceled. If both timeout and deadline are set, the earlier is used. May be null.
 * @param queueName Optional name of the queue to which the workflow should be enqueued for
 *     execution. May be null.
 * @param deduplicationId If {@code queueName} is specified, an optional ID used to prevent
 *     duplicate enqueued workflows. May be null.
 * @param priority If {@code queueName} is specified and refers to a queue with priority enabled,
 *     the priority to assign. May be null.
 * @param queuePartitionKey If {@code queueName} is specified, an optional partition key used to
 *     distribute workflows across queue partitions for load balancing and ordered processing. May
 *     be null.
 * @param delay Optional delay before the workflow starts executing. May be null.
 * @param appVersion Optional application version to target for workflow execution. May be null.
 * @param authenticatedUser Optional authenticated user to associate with the workflow. May be null.
 * @param assumedRole Optional assumed role to associate with the workflow. May be null.
 * @param authenticatedRoles Optional authenticated roles to associate with the workflow. May be
 *     null.
 */
public record StartWorkflowOptions(
    @Nullable String workflowId,
    @Nullable Timeout timeout,
    @Nullable Instant deadline,
    @Nullable String queueName,
    @Nullable String deduplicationId,
    @Nullable Integer priority,
    @Nullable String queuePartitionKey,
    @Nullable Duration delay,
    @Nullable String appVersion,
    @Nullable String authenticatedUser,
    @Nullable String assumedRole,
    @Nullable String[] authenticatedRoles) {

  public StartWorkflowOptions {
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
  }

  /** Construct with default options (all fields null). */
  public StartWorkflowOptions() {
    this(null, null, null, null, null, null, null, null, null, null, null, null);
  }

  /**
   * Construct with a specified workflow ID.
   *
   * @param workflowId the workflow ID to assign
   */
  public StartWorkflowOptions(String workflowId) {
    this(workflowId, null, null, null, null, null, null, null, null, null, null, null);
  }

  /**
   * Construct with a specified queue.
   *
   * @param queue the queue to assign the workflow to
   */
  public StartWorkflowOptions(@NonNull Queue queue) {
    this(null, null, null, queue.name(), null, null, null, null, null, null, null, null);
  }

  /**
   * Returns a new StartWorkflowOptions with the specified workflow ID.
   *
   * @param workflowId the workflow ID to assign
   * @return a new StartWorkflowOptions with the updated workflow ID
   */
  public @NonNull StartWorkflowOptions withWorkflowId(@Nullable String workflowId) {
    return new StartWorkflowOptions(
        workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.appVersion,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  /**
   * Returns a new StartWorkflowOptions with the specified timeout.
   *
   * @param timeout the timeout to assign
   * @return a new StartWorkflowOptions with the updated timeout
   */
  public @NonNull StartWorkflowOptions withTimeout(@Nullable Timeout timeout) {
    return new StartWorkflowOptions(
        this.workflowId,
        timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.appVersion,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  /**
   * Returns a new StartWorkflowOptions with the specified timeout duration.
   *
   * @param timeout the timeout duration to assign
   * @return a new StartWorkflowOptions with the updated timeout
   */
  public @NonNull StartWorkflowOptions withTimeout(@NonNull Duration timeout) {
    return withTimeout(Timeout.of(timeout));
  }

  /**
   * Returns a new StartWorkflowOptions with the specified timeout value and unit.
   *
   * @param value the timeout value
   * @param unit the time unit for the timeout
   * @return a new StartWorkflowOptions with the updated timeout
   */
  public @NonNull StartWorkflowOptions withTimeout(long value, @NonNull TimeUnit unit) {
    return withTimeout(Duration.ofNanos(unit.toNanos(value)));
  }

  /**
   * Returns a new StartWorkflowOptions with no timeout (disables timeout behavior).
   *
   * @return a new StartWorkflowOptions with timeout disabled
   */
  public @NonNull StartWorkflowOptions withNoTimeout() {
    return withTimeout(Timeout.none());
  }

  /**
   * Returns a new StartWorkflowOptions with the specified deadline.
   *
   * @param deadline the absolute deadline to assign
   * @return a new StartWorkflowOptions with the updated deadline
   */
  public @NonNull StartWorkflowOptions withDeadline(@Nullable Instant deadline) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.appVersion,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  /**
   * Returns a new StartWorkflowOptions with the specified queue name.
   *
   * @param queue the queue name to assign
   * @return a new StartWorkflowOptions with the updated queue name
   */
  public @NonNull StartWorkflowOptions withQueue(@Nullable String queue) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        queue,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.appVersion,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  /**
   * Returns a new StartWorkflowOptions with the specified queue.
   *
   * @param queue the queue to assign
   * @return a new StartWorkflowOptions with the updated queue name
   */
  public @NonNull StartWorkflowOptions withQueue(@NonNull Queue queue) {
    return withQueue(queue.name());
  }

  /**
   * Returns a new StartWorkflowOptions with the specified queue deduplication ID. Note: The queue
   * must also be specified for deduplication to take effect.
   *
   * @param deduplicationId the deduplication ID to assign
   * @return a new StartWorkflowOptions with the updated deduplication ID
   */
  public @NonNull StartWorkflowOptions withDeduplicationId(@Nullable String deduplicationId) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.appVersion,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  /**
   * Returns a new StartWorkflowOptions with the specified queue priority. Note: The queue must be
   * specified and have prioritization enabled.
   *
   * @param priority the priority to assign
   * @return a new StartWorkflowOptions with the updated priority
   */
  public @NonNull StartWorkflowOptions withPriority(@Nullable Integer priority) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        priority,
        this.queuePartitionKey,
        this.delay,
        this.appVersion,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  /**
   * Returns a new StartWorkflowOptions with the specified queue partition key.
   *
   * @param queuePartitionKey the partition key to assign
   * @return a new StartWorkflowOptions with the updated partition key
   */
  public @NonNull StartWorkflowOptions withQueuePartitionKey(@Nullable String queuePartitionKey) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        queuePartitionKey,
        this.delay,
        this.appVersion,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  /**
   * Returns a new StartWorkflowOptions with the specified delay before execution.
   *
   * @param delay the delay duration to assign
   * @return a new StartWorkflowOptions with the updated delay
   */
  public @NonNull StartWorkflowOptions withDelay(@Nullable Duration delay) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        delay,
        this.appVersion,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  /**
   * Returns a new StartWorkflowOptions with the specified application version.
   *
   * @param appVersion the application version to assign
   * @return a new StartWorkflowOptions with the updated application version
   */
  public @NonNull StartWorkflowOptions withAppVersion(@Nullable String appVersion) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        appVersion,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles);
  }

  /**
   * Returns a new StartWorkflowOptions with the specified authenticated user.
   *
   * @param authenticatedUser the authenticated user to assign
   * @return a new StartWorkflowOptions with the updated authenticated user
   */
  public @NonNull StartWorkflowOptions withAuthenticatedUser(@Nullable String authenticatedUser) {
    return withAuthentication(authenticatedUser, this.authenticatedRoles);
  }

  /**
   * Returns a new StartWorkflowOptions with the specified assumed role.
   *
   * @param assumedRole the assumed role to assign
   * @return a new StartWorkflowOptions with the updated assumed role
   */
  public @NonNull StartWorkflowOptions withAssumedRole(@Nullable String assumedRole) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.appVersion,
        this.authenticatedUser,
        assumedRole,
        this.authenticatedRoles);
  }

  /**
   * Returns a new StartWorkflowOptions with the specified authenticated roles.
   *
   * @param authenticatedRoles the authenticated roles to assign
   * @return a new StartWorkflowOptions with the updated authenticated roles
   */
  public @NonNull StartWorkflowOptions withAuthenticatedRoles(
      @Nullable String... authenticatedRoles) {
    return withAuthentication(this.authenticatedUser, authenticatedRoles);
  }

  /**
   * Returns a new StartWorkflowOptions with the specified authenticated user and roles.
   *
   * @param authenticatedUser the authenticated user to assign
   * @param authenticatedRoles the authenticated roles to assign
   * @return a new StartWorkflowOptions with the updated authenticated user and roles
   */
  public @NonNull StartWorkflowOptions withAuthentication(
      @Nullable String authenticatedUser, @Nullable String... authenticatedRoles) {
    return new StartWorkflowOptions(
        this.workflowId,
        this.timeout,
        this.deadline,
        this.queueName,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.appVersion,
        authenticatedUser,
        this.assumedRole,
        authenticatedRoles);
  }

  /**
   * Get the assigned workflow ID, replacing empty with null.
   *
   * @return the workflow ID, or null if empty or not set
   */
  @Override
  public @Nullable String workflowId() {
    return workflowId != null && workflowId.isEmpty() ? null : workflowId;
  }
}
