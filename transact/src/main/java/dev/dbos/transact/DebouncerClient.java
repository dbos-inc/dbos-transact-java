package dev.dbos.transact;

import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.internal.Validation;
import dev.dbos.transact.workflow.DebounceResult;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.QueueName;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.internal.DebouncerContextOptions;
import dev.dbos.transact.workflow.internal.DebouncerMessage;
import dev.dbos.transact.workflow.internal.DebouncerOptions;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Debounces repeated workflow invocations from an external client into a single execution using the
 * most recent arguments, without requiring a running {@link DBOS} instance on the caller's side.
 *
 * <p>Create instances via {@link DBOSClient#debouncer(String)}.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var client = new DBOSClient(url, user, password);
 *
 * var debouncer = client.<String>debouncer("process")
 *     .withClassName(MyServiceImpl.class.getName())
 *     .withDebounceTimeout(Duration.ofMinutes(5));
 *
 * WorkflowHandle<String, ?> handle =
 *     debouncer.debounce("user-42", Duration.ofSeconds(2), "payload");
 * String result = handle.getResult();
 * }</pre>
 *
 * @param <R> return type of the debounced workflow
 */
public final class DebouncerClient<R> {

  private static final Logger logger = LoggerFactory.getLogger(DebouncerClient.class);

  private final DBOSClient client;
  private final String workflowName;
  private final @Nullable String className;
  private final @Nullable String instanceName;
  private final @Nullable String userQueueName;
  private final @Nullable Duration debounceTimeout;
  // Context options forwarded to the user workflow
  private final @Nullable String appVersion;
  private final @Nullable Integer priority;
  private final @Nullable String userDeduplicationId;
  private final @Nullable Duration workflowTimeout;
  private final @Nullable Map<String, Object> attributes;
  private final @Nullable SerializationStrategy serialization;

  DebouncerClient(@NonNull DBOSClient client, @NonNull String workflowName) {
    this(client, workflowName, null, null, null, null, null, null, null, null, null, null);
  }

  private DebouncerClient(
      DBOSClient client,
      String workflowName,
      @Nullable String className,
      @Nullable String instanceName,
      @Nullable String userQueueName,
      @Nullable Duration debounceTimeout,
      @Nullable String appVersion,
      @Nullable Integer priority,
      @Nullable String userDeduplicationId,
      @Nullable Duration workflowTimeout,
      @Nullable Map<String, Object> attributes,
      @Nullable SerializationStrategy serialization) {
    this.client = Objects.requireNonNull(client, "client must not be null");
    this.workflowName = Objects.requireNonNull(workflowName, "workflowName must not be null");
    this.className = className;
    this.instanceName = instanceName;
    this.userQueueName = userQueueName;
    this.debounceTimeout = debounceTimeout;
    this.appVersion = appVersion;
    this.priority = priority;
    this.userDeduplicationId = userDeduplicationId;
    this.workflowTimeout = workflowTimeout;
    this.attributes = attributes;
    this.serialization = serialization;
  }

  /** Specify the Java class name of the target workflow implementation. */
  public @NonNull DebouncerClient<R> withClassName(@Nullable String className) {
    return new DebouncerClient<>(
        client,
        workflowName,
        className,
        instanceName,
        userQueueName,
        debounceTimeout,
        appVersion,
        priority,
        userDeduplicationId,
        workflowTimeout,
        attributes,
        serialization);
  }

  /** Specify the DBOS instance name of the target workflow implementation. */
  public @NonNull DebouncerClient<R> withInstanceName(@Nullable String instanceName) {
    return new DebouncerClient<>(
        client,
        workflowName,
        className,
        instanceName,
        userQueueName,
        debounceTimeout,
        appVersion,
        priority,
        userDeduplicationId,
        workflowTimeout,
        attributes,
        serialization);
  }

  /**
   * Set the queue that the user workflow will be enqueued on when the debounce period elapses.
   * {@code null} starts the user workflow directly (not enqueued).
   */
  public @NonNull DebouncerClient<R> withQueue(@Nullable String queueName) {
    if (queueName != null && queueName.isEmpty()) {
      throw new IllegalArgumentException("queueName must not be empty");
    }
    return new DebouncerClient<>(
        client,
        workflowName,
        className,
        instanceName,
        queueName,
        debounceTimeout,
        appVersion,
        priority,
        userDeduplicationId,
        workflowTimeout,
        attributes,
        serialization);
  }

  /**
   * Returns a copy of this debouncer that enqueues the debounced workflow on {@code queue}.
   *
   * @param queue name of the queue to enqueue on
   * @return a copy with the queue set
   */
  public @NonNull DebouncerClient<R> withQueue(@NonNull QueueName queue) {
    return withQueue(queue.value());
  }

  /**
   * @deprecated Use {@link #withQueue(QueueName)}.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public @NonNull DebouncerClient<R> withQueue(@NonNull Queue queue) {
    return withQueue(queue.name());
  }

  /**
   * Set an absolute cap on how long the debouncer may keep absorbing calls for a single key. After
   * this duration the user workflow fires even if more calls keep arriving.
   */
  public @NonNull DebouncerClient<R> withDebounceTimeout(@Nullable Duration debounceTimeout) {
    return new DebouncerClient<>(
        client,
        workflowName,
        className,
        instanceName,
        userQueueName,
        debounceTimeout,
        appVersion,
        priority,
        userDeduplicationId,
        workflowTimeout,
        attributes,
        serialization);
  }

  /** Target a specific application version for the user workflow. */
  public @NonNull DebouncerClient<R> withAppVersion(@Nullable String appVersion) {
    return new DebouncerClient<>(
        client,
        workflowName,
        className,
        instanceName,
        userQueueName,
        debounceTimeout,
        appVersion,
        priority,
        userDeduplicationId,
        workflowTimeout,
        attributes,
        serialization);
  }

  /**
   * Set the priority for the user workflow; lower values are dequeued first. A priority only means
   * something on a queue, so {@link #debounce} rejects one when no queue is configured, and it
   * rejects a negative one.
   */
  public @NonNull DebouncerClient<R> withPriority(@Nullable Integer priority) {
    return new DebouncerClient<>(
        client,
        workflowName,
        className,
        instanceName,
        userQueueName,
        debounceTimeout,
        appVersion,
        priority,
        userDeduplicationId,
        workflowTimeout,
        attributes,
        serialization);
  }

  /**
   * Set a deduplication ID to be forwarded to the user workflow.
   *
   * @deprecated Ignored from the next release, where the debouncer sets the deduplication ID to one
   *     it generates itself. Removed in 2.0.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public @NonNull DebouncerClient<R> withDeduplicationId(@Nullable String deduplicationId) {
    return new DebouncerClient<>(
        client,
        workflowName,
        className,
        instanceName,
        userQueueName,
        debounceTimeout,
        appVersion,
        priority,
        deduplicationId,
        workflowTimeout,
        attributes,
        serialization);
  }

  /** Set a timeout for the user workflow. */
  public @NonNull DebouncerClient<R> withTimeout(@Nullable Duration timeout) {
    return new DebouncerClient<>(
        client,
        workflowName,
        className,
        instanceName,
        userQueueName,
        debounceTimeout,
        appVersion,
        priority,
        userDeduplicationId,
        timeout,
        attributes,
        serialization);
  }

  /** Set JSON-serializable custom attributes to be recorded on the user workflow. */
  public @NonNull DebouncerClient<R> withAttributes(@Nullable Map<String, Object> attributes) {
    return new DebouncerClient<>(
        client,
        workflowName,
        className,
        instanceName,
        userQueueName,
        debounceTimeout,
        appVersion,
        priority,
        userDeduplicationId,
        workflowTimeout,
        Validation.validateAttributes(attributes),
        serialization);
  }

  /**
   * Set the serialization strategy the user workflow's arguments are written with. It should match
   * the strategy the workflow is registered with; a bounce that extends a waiting debounced
   * workflow rewrites its inputs in this format.
   */
  public @NonNull DebouncerClient<R> withSerialization(
      @Nullable SerializationStrategy serialization) {
    return new DebouncerClient<>(
        client,
        workflowName,
        className,
        instanceName,
        userQueueName,
        debounceTimeout,
        appVersion,
        priority,
        userDeduplicationId,
        workflowTimeout,
        attributes,
        serialization);
  }

  /**
   * Debounce a workflow invocation.
   *
   * @param debounceKey key that groups concurrent calls; calls with the same key are coalesced
   * @param debouncePeriod inactivity window before the user workflow runs; each call resets it
   * @param args positional arguments to pass to the user workflow
   * @return handle pointing to the user workflow that will run with the latest arguments. When
   *     another call already holds the key, that is the workflow it named: the child ID published
   *     by a running debouncer service workflow, or a debounced workflow this call extended.
   */
  public @NonNull WorkflowHandle<R, ?> debounce(
      @NonNull String debounceKey, @NonNull Duration debouncePeriod, Object... args) {

    Objects.requireNonNull(debounceKey, "debounceKey must not be null");
    Objects.requireNonNull(debouncePeriod, "debouncePeriod must not be null");
    if (debouncePeriod.isNegative() || debouncePeriod.isZero()) {
      throw new IllegalArgumentException("debouncePeriod must be a positive non-zero duration");
    }
    if (priority != null && userQueueName == null) {
      throw new IllegalArgumentException(
          "a queue must be configured with withQueue to specify a priority");
    }
    // Checked here as well as in the options: those are only built inside the debouncer workflow,
    // where a bad value would fail durably rather than at this call.
    if (priority != null && priority < 0) {
      throw new IllegalArgumentException("priority must not be negative");
    }
    // className is required: the debouncer workflow uses it to look up the registered workflow.
    if (className == null) {
      throw new IllegalStateException(
          "className is required; call withClassName(MyServiceImpl.class.getName()) before debounce()");
    }

    // Not inside a workflow, so UUIDs can be generated directly (no step wrapping needed).
    String userWorkflowId = UUID.randomUUID().toString();
    String messageId = UUID.randomUUID().toString();
    String deduplicationId = workflowName + "-" + debounceKey;

    DebouncerOptions debouncerOpts =
        new DebouncerOptions(
            workflowName,
            className,
            instanceName,
            userQueueName,
            debounceTimeout,
            appVersion,
            priority,
            userDeduplicationId);
    DebouncerContextOptions ctx =
        new DebouncerContextOptions(userWorkflowId, workflowTimeout, attributes);
    DebouncerMessage initial = new DebouncerMessage(messageId, args, debouncePeriod);

    var enqueueOpts =
        new EnqueueOptions(
                Constants.DEBOUNCER_WORKFLOW_NAME,
                Constants.DEBOUNCER_CLASS_NAME,
                QueueName.of(Constants.DBOS_INTERNAL_QUEUE))
            .withDeduplicationId(deduplicationId);

    while (true) {
      // A newer SDK version keeps its debounced workflows waiting DELAYED on the user queue. Try to
      // extend one there first, so a fleet mixing the two keeps coalescing on one key.
      if (userQueueName != null) {
        var bounced =
            client.debounceDelayedWorkflow(
                workflowName,
                className,
                instanceName,
                userQueueName,
                deduplicationId,
                delayUntil(debouncePeriod),
                args,
                serialization);
        if (bounced instanceof DebounceResult.Bounced b) {
          return client.retrieveWorkflow(b.bouncedWorkflowId());
        }
        // A miss drops the holder rather than classifying it, unlike the internal-queue bounce
        // below. This release writes no debounce key onto the user queue -- the service workflow
        // enqueues its child with the caller's own deduplication ID, never this one -- so whoever
        // holds the key there cannot collide with anything this call goes on to create. The cost
        // is the mixed-fleet gap: the two shapes hold the key on different queues, so a key hit by
        // both kinds of node within a few milliseconds runs twice. Once the enqueue puts the key
        // on the user queue (#538), a holder found here has to be classified exactly as the one
        // below is.
      }
      try {
        client.enqueueWorkflow(enqueueOpts, new Object[] {debouncerOpts, ctx, initial});
        return client.retrieveWorkflow(userWorkflowId);
      } catch (DBOSQueueDuplicatedException dup) {
        // Something already holds this key on the internal queue. If it is a debounced workflow
        // waiting there, this bounce extends it and the conflict is resolved in one round trip.
        // Otherwise the result reports the holder so we can coordinate with it or refuse.
        var result =
            client.debounceDelayedWorkflow(
                workflowName,
                className,
                instanceName,
                Constants.DBOS_INTERNAL_QUEUE,
                deduplicationId,
                delayUntil(debouncePeriod),
                args,
                serialization);
        if (result instanceof DebounceResult.Bounced b) {
          return client.retrieveWorkflow(b.bouncedWorkflowId());
        }
        var holder = ((DebounceResult.NotBounced) result).holder();
        if (holder == null) {
          logger.debug(
              "Debouncer for dedupId {} not found after conflict; retrying", deduplicationId);
          continue;
        }
        // A peer's holder is not ours to extend: it dequeues on that application's account, so
        // it may never run at all from here, and the retry below would spin forever waiting for an
        // ack. Surface the collision the way a plain deduplicated enqueue would.
        if (holder.isForeignTo(client.applicationName())) {
          throw new DBOSQueueDuplicatedException(
              userWorkflowId, Constants.DBOS_INTERNAL_QUEUE, deduplicationId);
        }
        if (!holder.isDebouncerService()) {
          if (holder.isDebouncedInstanceOf(workflowName, className, instanceName)) {
            // A debounced instance of this workflow holds the key but is no longer DELAYED: it
            // left that state between the enqueue attempt and the bounce, and its key is about to
            // clear. Retry; the next enqueue starts a fresh debouncer.
            logger.debug(
                "Debounced workflow {} for dedupId {} is no longer delayed; retrying",
                holder.workflowId(),
                deduplicationId);
            continue;
          }
          // Held by a workflow this debounce must not touch: one that was deduplicated on its
          // own, or a different workflow whose debounce key collides with ours.
          throw new DBOSQueueDuplicatedException(
              userWorkflowId, Constants.DBOS_INTERNAL_QUEUE, deduplicationId);
        }
        // A debouncer service workflow for this key is running — forward the latest args to it.
        String existingDebouncerId = holder.workflowId();

        DebouncerMessage msg = new DebouncerMessage(messageId, args, debouncePeriod);
        client.send(existingDebouncerId, msg, Constants.DEBOUNCER_TOPIC, messageId);

        var ack = client.getEvent(existingDebouncerId, messageId, Constants.DEBOUNCER_ACK_TIMEOUT);
        if (ack.isEmpty()) {
          logger.debug(
              "Debouncer {} did not ack message {}; retrying", existingDebouncerId, messageId);
          continue;
        }

        // DEBOUNCER_CHILD_ID_KEY is published as the debouncer's first action, before the
        // recv-loop. If the ack arrived the event should be available; retry if not to guard
        // against transient delays.
        var childIdOpt =
            client.getEvent(
                existingDebouncerId,
                Constants.DEBOUNCER_CHILD_ID_KEY,
                Constants.DEBOUNCER_ACK_TIMEOUT);
        if (childIdOpt.isEmpty()) {
          logger.debug(
              "DEBOUNCER_CHILD_ID_KEY not yet available from {}; retrying", existingDebouncerId);
          continue;
        }
        return client.retrieveWorkflow((String) childIdOpt.get());
      }
    }
  }

  private static long delayUntil(Duration debouncePeriod) {
    return Instant.now().plus(debouncePeriod).toEpochMilli();
  }
}
