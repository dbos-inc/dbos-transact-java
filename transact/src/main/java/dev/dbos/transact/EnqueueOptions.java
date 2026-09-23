package dev.dbos.transact;

import static dev.dbos.transact.internal.Validation.nullableIsEmpty;
import static dev.dbos.transact.internal.Validation.nullableIsNotPositive;
import static dev.dbos.transact.internal.Validation.validateAttributes;

import dev.dbos.transact.workflow.SerializationStrategy;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Options for enqueuing a workflow by name.
 *
 * <p>Used both by {@link DBOS#enqueueWorkflow} inside a DBOS application and by {@link
 * DBOSClient#enqueueWorkflow} outside one. The workflow is identified by name rather than by a
 * reference to its function, so it may be implemented by another process, another application, or
 * another language, as long as it shares the system database.
 *
 * <p>This record encapsulates all configuration required to enqueue a workflow, including:
 *
 * <ul>
 *   <li>Workflow name and (optionally) class and instance name
 *   <li>Target queue and queue-related options (priority, partitioning, deduplication, delay)
 *   <li>Workflow idempotency and versioning
 *   <li>Timeout and deadline management
 *   <li>Serialization strategy for workflow arguments
 *   <li>The application that owns the enqueued workflow
 * </ul>
 *
 * <p>Required fields: {@code workflowName}, {@code queueName}. All other fields are optional and
 * can be set using the provided {@code with} methods.
 *
 * @param workflowName The name of the workflow function to enqueue. Required.
 * @param className The Java class containing the workflow function. Optional.
 * @param instanceName The instance name for object-based workflows. Optional.
 * @param queueName The name of the queue to enqueue the workflow to. Required.
 * @param workflowId The idempotency key for the workflow instance. Optional; if not set, a random
 *     UUID will be generated.
 * @param appVersion The application version to target for execution. Optional.
 * @param timeout The maximum duration the workflow may run before being canceled. Optional.
 * @param deadline The absolute time by which the workflow must start or complete. Optional.
 * @param deduplicationId An optional ID to prevent duplicate enqueued workflows. Optional.
 * @param priority The priority to assign; lower values are dequeued first, and the default is 0.
 *     Must not be negative. Optional.
 * @param queuePartitionKey The partition key for distributing workflows across queue partitions.
 *     Optional.
 * @param delay The delay before the workflow starts executing. Optional.
 * @param serialization The serialization strategy for workflow arguments. Optional.
 * @param authenticatedUser The authenticated user to associate with the workflow. Optional.
 * @param assumedRole The assumed role to associate with the workflow. Optional.
 * @param authenticatedRoles The authenticated roles to associate with the workflow. Optional.
 * @param attributes Custom JSON-serializable attributes to attach to the workflow at creation.
 *     Optional.
 * @param applicationName The application that owns the enqueued workflow, and whose executors
 *     therefore dequeue and run it. Optional; defaults to the enqueueing application. Set it to
 *     enqueue work for a peer sharing this system database.
 */
public record EnqueueOptions(
    @NonNull String workflowName,
    @Nullable String className,
    @Nullable String instanceName,
    @NonNull String queueName,
    @Nullable String workflowId,
    @Nullable String appVersion,
    @Nullable Duration timeout,
    @Nullable Instant deadline,
    @Nullable String deduplicationId,
    @Nullable Integer priority,
    @Nullable String queuePartitionKey,
    @Nullable Duration delay,
    @Nullable SerializationStrategy serialization,
    @Nullable String authenticatedUser,
    @Nullable String assumedRole,
    @Nullable List<String> authenticatedRoles,
    @Nullable Map<String, Object> attributes,
    @Nullable String applicationName) {

  public EnqueueOptions {
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

    if (nullableIsEmpty(workflowId)) {
      throw new IllegalArgumentException("workflowId must not be empty");
    }

    if (nullableIsEmpty(appVersion)) {
      throw new IllegalArgumentException("appVersion must not be empty");
    }

    if (nullableIsNotPositive(timeout)) {
      throw new IllegalArgumentException("timeout must be positive, non-zero duration");
    }

    if (nullableIsEmpty(deduplicationId)) {
      throw new IllegalArgumentException("deduplicationId must not be empty");
    }

    if (nullableIsEmpty(queuePartitionKey)) {
      throw new IllegalArgumentException("queuePartitionKey must not be empty");
    }

    if (nullableIsNotPositive(delay)) {
      throw new IllegalArgumentException("delay must be positive, non-zero duration");
    }

    if (nullableIsEmpty(applicationName)) {
      throw new IllegalArgumentException("applicationName must not be empty");
    }

    // 0 is the default, so a negative priority would dequeue ahead of every workflow that set
    // none.
    if (priority != null && priority < 0) {
      throw new IllegalArgumentException("priority must not be negative");
    }

    authenticatedRoles = authenticatedRoles != null ? List.copyOf(authenticatedRoles) : null;

    attributes = validateAttributes(attributes);
  }

  /** Construct `EnqueueOptions` with a minimum set of required options */
  public EnqueueOptions(@NonNull String workflowName, @NonNull String queueName) {
    this(
        workflowName,
        null,
        null,
        queueName,
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
        null,
        null);
  }

  /**
   * Specify the Java classname for the class containing the workflow to enqueue
   *
   * @param className Class containing the workflow to enqueue
   * @return New `EnqueueOptions` with the class name set
   */
  public @NonNull EnqueueOptions withClassName(@Nullable String className) {
    return new EnqueueOptions(
        this.workflowName,
        className,
        this.instanceName,
        this.queueName,
        this.workflowId,
        this.appVersion,
        this.timeout,
        this.deadline,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes,
        this.applicationName);
  }

  /**
   * Specify the workflow ID for the workflow to be enqueued. This is an idempotency key for running
   * the workflow.
   *
   * @param workflowId Workflow idempotency ID to use
   * @return New `EnqueueOptions` with the workflow ID set
   */
  public @NonNull EnqueueOptions withWorkflowId(@Nullable String workflowId) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        this.instanceName,
        this.queueName,
        workflowId,
        this.appVersion,
        this.timeout,
        this.deadline,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes,
        this.applicationName);
  }

  /**
   * Specify the app version for the workflow to be enqueued. The workflow will be executed by an
   * executor with this app version. If not specified, the current app version will be used.
   *
   * @param appVersion Application version to use for executing the workflow
   * @return New `EnqueueOptions` with the app version set
   */
  public @NonNull EnqueueOptions withAppVersion(@Nullable String appVersion) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        this.instanceName,
        this.queueName,
        this.workflowId,
        appVersion,
        this.timeout,
        this.deadline,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes,
        this.applicationName);
  }

  /**
   * Specify a timeout for the workflow to be enqueued. Timeout begins once the workflow is running;
   * if it exceeds this it will be canceled.
   *
   * @param timeout Duration of time, from start, before the workflow is canceled.
   * @return New `EnqueueOptions` with the timeout set
   */
  public @NonNull EnqueueOptions withTimeout(@Nullable Duration timeout) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        this.instanceName,
        this.queueName,
        this.workflowId,
        this.appVersion,
        timeout,
        this.deadline,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes,
        this.applicationName);
  }

  /**
   * Specify a deadline for the workflow. This is an absolute time, regardless of when the workflow
   * starts.
   *
   * @param deadline Instant after which the workflow will be canceled.
   * @return New `EnqueueOptions` with the deadline set
   */
  public @NonNull EnqueueOptions withDeadline(@Nullable Instant deadline) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        this.instanceName,
        this.queueName,
        this.workflowId,
        this.appVersion,
        this.timeout,
        deadline,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes,
        this.applicationName);
  }

  /**
   * Specify a queue deduplication ID for the workflow to be enqueued. Queue requests with the same
   * deduplication ID will be rejected.
   *
   * @param deduplicationId Queue deduplication ID
   * @return New `EnqueueOptions` with the deduplication ID set
   */
  public @NonNull EnqueueOptions withDeduplicationId(@Nullable String deduplicationId) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        this.instanceName,
        this.queueName,
        this.workflowId,
        this.appVersion,
        this.timeout,
        this.deadline,
        deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes,
        this.applicationName);
  }

  /**
   * Specify an object instance name to execute the workflow. If workflow objects are named, this
   * must be specified to direct processing to the correct instance.
   *
   * @param instName Instance name registered within `DBOS.registerWorkflows`
   * @return New `EnqueueOptions` with the target instance name set
   */
  public @NonNull EnqueueOptions withInstanceName(@Nullable String instName) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        instName,
        this.queueName,
        this.workflowId,
        this.appVersion,
        this.timeout,
        this.deadline,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes,
        this.applicationName);
  }

  /**
   * Specify priority. Lower values are dequeued first; every queue dispatches in priority order.
   *
   * @param priority Queue priority; must not be negative. If `null`, priority '0' will be used.
   * @return New `EnqueueOptions` with the priority set
   */
  public @NonNull EnqueueOptions withPriority(@Nullable Integer priority) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        this.instanceName,
        this.queueName,
        this.workflowId,
        this.appVersion,
        this.timeout,
        this.deadline,
        this.deduplicationId,
        priority,
        this.queuePartitionKey,
        this.delay,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes,
        this.applicationName);
  }

  /**
   * Creates a new EnqueueOptions instance with the specified queue partition key. The partition key
   * is used to determine which partition of the queue the workflow should be enqueued to, allowing
   * for better load distribution and ordering guarantees.
   *
   * @param partitionKey the partition key to use for queue partitioning, can be null
   * @return a new EnqueueOptions instance with the specified partition key
   */
  public @NonNull EnqueueOptions withQueuePartitionKey(@Nullable String partitionKey) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        this.instanceName,
        this.queueName,
        this.workflowId,
        this.appVersion,
        this.timeout,
        this.deadline,
        this.deduplicationId,
        this.priority,
        partitionKey,
        this.delay,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes,
        this.applicationName);
  }

  /**
   * Specify a delay before the workflow starts executing. The workflow will remain in the queue
   * until the delay has elapsed.
   *
   * @param delay Duration to wait before the workflow begins execution.
   * @return New `EnqueueOptions` with the delay set
   */
  public @NonNull EnqueueOptions withDelay(@Nullable Duration delay) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        this.instanceName,
        this.queueName,
        this.workflowId,
        this.appVersion,
        this.timeout,
        this.deadline,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        delay,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes,
        this.applicationName);
  }

  /**
   * Specify the serialization strategy for the workflow arguments.
   *
   * @param serialization The serialization strategy ({@link SerializationStrategy#PORTABLE} for
   *     cross-language compatibility, {@link SerializationStrategy#NATIVE} for Java-specific, or
   *     {@link SerializationStrategy#DEFAULT} for the default behavior)
   * @return New `EnqueueOptions` with the serialization strategy set
   */
  public @NonNull EnqueueOptions withSerialization(@Nullable SerializationStrategy serialization) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        this.instanceName,
        this.queueName,
        this.workflowId,
        this.appVersion,
        this.timeout,
        this.deadline,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes,
        this.applicationName);
  }

  /**
   * Specify the authenticated user to associate with the workflow.
   *
   * @param authenticatedUser the authenticated user
   * @return New `EnqueueOptions` with the authenticated user set
   */
  public @NonNull EnqueueOptions withAuthenticatedUser(@Nullable String authenticatedUser) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        this.instanceName,
        this.queueName,
        this.workflowId,
        this.appVersion,
        this.timeout,
        this.deadline,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.serialization,
        authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes,
        this.applicationName);
  }

  /**
   * Specify the assumed role to associate with the workflow.
   *
   * @param assumedRole the assumed role
   * @return New `EnqueueOptions` with the assumed role set
   */
  public @NonNull EnqueueOptions withAssumedRole(@Nullable String assumedRole) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        this.instanceName,
        this.queueName,
        this.workflowId,
        this.appVersion,
        this.timeout,
        this.deadline,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.serialization,
        this.authenticatedUser,
        assumedRole,
        this.authenticatedRoles,
        this.attributes,
        this.applicationName);
  }

  /**
   * Specify the authenticated roles to associate with the workflow.
   *
   * @param authenticatedRoles the authenticated roles
   * @return New `EnqueueOptions` with the authenticated roles set
   */
  public @NonNull EnqueueOptions withAuthenticatedRoles(@Nullable String... authenticatedRoles) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        this.instanceName,
        this.queueName,
        this.workflowId,
        this.appVersion,
        this.timeout,
        this.deadline,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        authenticatedRoles != null ? List.of(authenticatedRoles) : null,
        this.attributes,
        this.applicationName);
  }

  /**
   * Specify the authenticated user and roles to associate with the workflow.
   *
   * @param authenticatedUser the authenticated user
   * @param authenticatedRoles the authenticated roles
   * @return New `EnqueueOptions` with the authenticated user and roles set
   */
  public @NonNull EnqueueOptions withAuthentication(
      @Nullable String authenticatedUser, @Nullable String... authenticatedRoles) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        this.instanceName,
        this.queueName,
        this.workflowId,
        this.appVersion,
        this.timeout,
        this.deadline,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.serialization,
        authenticatedUser,
        this.assumedRole,
        authenticatedRoles != null ? List.of(authenticatedRoles) : null,
        this.attributes,
        this.applicationName);
  }

  /**
   * Specify custom JSON-serializable attributes to attach to the workflow at creation. Attributes
   * are recorded in the workflow status and can be used to filter workflows in {@code
   * listWorkflows}.
   *
   * @param attributes the custom attributes, or an empty map to clear
   * @return New `EnqueueOptions` with the attributes set
   */
  public @NonNull EnqueueOptions withAttributes(@Nullable Map<String, Object> attributes) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        this.instanceName,
        this.queueName,
        this.workflowId,
        this.appVersion,
        this.timeout,
        this.deadline,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        attributes,
        this.applicationName);
  }

  /**
   * Specify the application that owns the enqueued workflow. Only executors running that
   * application dequeue it, so this is how one application enqueues work for a peer sharing its
   * system database. Left unset, the workflow belongs to the enqueueing application — or to no
   * application at all, when the enqueuer has no name of its own.
   *
   * @param applicationName the owning application, or null for the enqueueing application's
   * @return New `EnqueueOptions` with the application name set
   */
  public @NonNull EnqueueOptions withApplicationName(@Nullable String applicationName) {
    return new EnqueueOptions(
        this.workflowName,
        this.className,
        this.instanceName,
        this.queueName,
        this.workflowId,
        this.appVersion,
        this.timeout,
        this.deadline,
        this.deduplicationId,
        this.priority,
        this.queuePartitionKey,
        this.delay,
        this.serialization,
        this.authenticatedUser,
        this.assumedRole,
        this.authenticatedRoles,
        this.attributes,
        applicationName);
  }
}
