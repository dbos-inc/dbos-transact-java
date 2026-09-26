package dev.dbos.transact;

import static dev.dbos.transact.internal.Validation.nullableIsEmpty;
import static dev.dbos.transact.internal.Validation.nullableIsNotPositive;
import static dev.dbos.transact.internal.Validation.validateAttributes;

import dev.dbos.transact.workflow.QueueName;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

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
 * <p>The constructors fix what to run and where: the required {@code workflowName} and queue, and
 * optionally {@code className} and {@code instanceName}. Every other field is optional and set with
 * the {@code with} methods.
 *
 * @param workflowName The name of the workflow function to enqueue. Required.
 * @param className The Java class containing the workflow function. Required to target a Java
 *     workflow; omit it only for a workflow not registered on a class, such as a Python function.
 * @param instanceName The instance name for object-based workflows. Optional.
 * @param queueName The name of the queue to enqueue the workflow to. Required.
 * @param workflowId The idempotency key for the workflow instance. Optional; if not set, a random
 *     UUID will be generated.
 * @param appVersion The application version to target for execution. Optional.
 * @param timeout How long the workflow may run, from when it is dequeued, before being canceled:
 *     {@link Timeout#of} an explicit duration, {@link Timeout#none()}, or {@link Timeout#inherit()}
 *     the running workflow's. Inheriting outside a workflow, or from a client, means no timeout.
 *     Optional; unset behaves as {@code DBOS.startWorkflow} does, taking an ambient timeout set
 *     with {@code WorkflowOptions}, else inheriting.
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
    @Nullable Timeout timeout,
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
    Objects.requireNonNull(workflowName, "workflowName must not be null");
    if (workflowName.isBlank()) {
      throw new IllegalArgumentException("workflowName must not be blank");
    }

    // The same rule QueueName applies, so every constructor accepts the same queues.
    Objects.requireNonNull(queueName, "queueName must not be null");
    if (queueName.isBlank()) {
      throw new IllegalArgumentException("queueName must not be blank");
    }

    if (nullableIsEmpty(className)) {
      throw new IllegalArgumentException("className must not be empty");
    }

    if (nullableIsEmpty(instanceName)) {
      throw new IllegalArgumentException("instanceName must not be empty");
    }

    if (nullableIsEmpty(workflowId)) {
      throw new IllegalArgumentException("workflowId must not be empty");
    }

    if (nullableIsEmpty(appVersion)) {
      throw new IllegalArgumentException("appVersion must not be empty");
    }

    if (timeout instanceof Timeout.Explicit explicit && nullableIsNotPositive(explicit.value())) {
      throw new IllegalArgumentException("explicit timeout must be a positive non-zero duration");
    }

    // Two bounds for one workflow contradict each other. No timeout, or an inherited one, does not:
    // the deadline then acts alone.
    if (timeout instanceof Timeout.Explicit && deadline != null) {
      throw new IllegalArgumentException("Can't set both an explicit timeout and a deadline");
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

  /**
   * Options for enqueuing the named workflow on {@code queue}.
   *
   * <p>With no class name the workflow can only be run by a target that looks workflows up by name
   * alone, such as a Python application. Java workflows are registered by class, so a Java executor
   * that dequeues it cannot run it: it logs a {@link
   * dev.dbos.transact.exceptions.DBOSWorkflowFunctionNotFoundException} and leaves the workflow
   * {@code PENDING}, as for any workflow it has no registration for. Use a constructor that takes a
   * class name to target a Java workflow.
   *
   * @param workflowName name of the workflow to enqueue
   * @param queue name of the queue to enqueue on
   */
  public EnqueueOptions(@NonNull String workflowName, @NonNull QueueName queue) {
    this(workflowName, null, null, queue);
  }

  /**
   * Options for enqueuing the named workflow of {@code className} on {@code queue}.
   *
   * @param workflowName name of the workflow to enqueue
   * @param className class containing the workflow, or its {@code @WorkflowClassName}; required to
   *     target a Java workflow, and {@code null} only for a workflow not registered on a class,
   *     such as a Python function
   * @param queue name of the queue to enqueue on
   */
  public EnqueueOptions(
      @NonNull String workflowName, @Nullable String className, @NonNull QueueName queue) {
    this(workflowName, className, null, queue);
  }

  /**
   * Options for enqueuing the named workflow of {@code className}, on the instance registered as
   * {@code instanceName}, on {@code queue}.
   *
   * @param workflowName name of the workflow to enqueue
   * @param className class containing the workflow, or its {@code @WorkflowClassName}; required to
   *     target a Java workflow, and {@code null} only for a workflow not registered on a class,
   *     such as a Python function
   * @param instanceName name the target instance was registered under
   * @param queue name of the queue to enqueue on
   */
  public EnqueueOptions(
      @NonNull String workflowName,
      @Nullable String className,
      @Nullable String instanceName,
      @NonNull QueueName queue) {
    this(
        workflowName,
        className,
        instanceName,
        Objects.requireNonNull(queue, "queue must not be null").value(),
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
   * executor with this app version. If not specified, the workflow is left without one and is
   * dequeued only by an executor running the owning application's latest registered version -- not
   * the enqueuer's version, which means nothing to a peer application.
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
   * Specify the workflow's timeout: {@link Timeout#of} an explicit duration, {@link
   * Timeout#none()}, or {@link Timeout#inherit()} the running workflow's. The clock starts when the
   * workflow is dequeued; if it runs longer it is canceled.
   *
   * @param timeout the timeout, or null to leave it unset
   * @return New `EnqueueOptions` with the timeout set
   */
  public @NonNull EnqueueOptions withTimeout(@Nullable Timeout timeout) {
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
   * Specify an explicit timeout for the workflow. The clock starts when the workflow is dequeued;
   * if it runs longer it is canceled.
   *
   * @param timeout Duration of time, from start, before the workflow is canceled
   * @return New `EnqueueOptions` with the timeout set
   */
  public @NonNull EnqueueOptions withTimeout(@NonNull Duration timeout) {
    return withTimeout(Timeout.of(timeout));
  }

  /**
   * Specify an explicit timeout for the workflow.
   *
   * @param value timeout amount
   * @param unit unit of {@code value}
   * @return New `EnqueueOptions` with the timeout set
   */
  public @NonNull EnqueueOptions withTimeout(long value, @NonNull TimeUnit unit) {
    return withTimeout(Duration.ofNanos(unit.toNanos(value)));
  }

  /**
   * Run the workflow with no timeout, rather than inheriting one from the enqueuing workflow.
   *
   * @return New `EnqueueOptions` with no timeout
   */
  public @NonNull EnqueueOptions withNoTimeout() {
    return withTimeout(Timeout.none());
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
   * @param applicationName the owning application, or null to leave the workflow with the
   *     enqueueing application
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
