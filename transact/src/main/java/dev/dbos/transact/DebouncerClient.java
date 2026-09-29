package dev.dbos.transact;

import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.internal.Validation;
import dev.dbos.transact.workflow.DebounceResult;
import dev.dbos.transact.workflow.Debouncer.DebounceIds;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.QueueName;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.Timeout;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.internal.DebouncerMessage;

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
  private final @Nullable Duration workflowTimeout;
  private final @Nullable Map<String, Object> attributes;
  private final @Nullable SerializationStrategy serialization;

  DebouncerClient(@NonNull DBOSClient client, @NonNull String workflowName) {
    this(client, workflowName, null, null, null, null, null, null, null, null, null);
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
        workflowTimeout,
        attributes,
        serialization);
  }

  /**
   * Set the queue that the user workflow waits and runs on. {@code null} uses the internal queue.
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
        workflowTimeout,
        attributes,
        serialization);
  }

  /**
   * Set the priority for the user workflow; lower values are dequeued first. A priority only means
   * something on a queue of your own, so {@link #debounce} rejects one when no queue is configured,
   * and it rejects a negative one.
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
        workflowTimeout,
        attributes,
        serialization);
  }

  /**
   * Ignored: the debounced workflow holds its debounce key as its deduplication ID.
   *
   * @deprecated Ignored since 1.2. To be removed in a future release.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public @NonNull DebouncerClient<R> withDeduplicationId(@Nullable String deduplicationId) {
    return this;
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
   *     another call already holds the key, that is the workflow it named: a debounced workflow
   *     this call extended, or the child ID published by a debouncer service workflow of an older
   *     SDK version.
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
    if (priority != null && priority < 0) {
      throw new IllegalArgumentException("priority must not be negative");
    }
    // className is required: the debounced workflow's row names it, and the bounce matches on it.
    if (className == null) {
      throw new IllegalStateException(
          "className is required; call withClassName(MyServiceImpl.class.getName()) before debounce()");
    }

    String targetQueue = userQueueName != null ? userQueueName : Constants.DBOS_INTERNAL_QUEUE;
    String deduplicationId = workflowName + "-" + debounceKey;

    // Not inside a workflow, so ids can be generated directly and nothing is recorded. The first
    // bounce also looks for a service workflow of an older SDK version holding the key.
    var ids =
        (DebounceIds)
            client.debounceDelayedWorkflow(
                workflowName,
                className,
                instanceName,
                targetQueue,
                deduplicationId,
                delayUntil(debouncePeriod),
                args,
                serialization,
                new DebounceIds(
                    UUID.randomUUID().toString(), UUID.randomUUID().toString(), null, null));
    if (ids.bouncedWorkflowId() != null) {
      return client.retrieveWorkflow(ids.bouncedWorkflowId());
    }

    String userWorkflowId = ids.userWorkflowId();
    String messageId = ids.messageId();
    String serviceWorkflowId = ids.serviceWorkflowId();
    // Set while creating the workflow a stranded service workflow promised, under its id.
    boolean takingOver = false;
    // Consecutive unacknowledged forwards to one service workflow; reset when the holder changes.
    String silentHolderId = null;
    int silentAcks = 0;

    while (true) {
      if (serviceWorkflowId != null) {
        String holderId = serviceWorkflowId;
        serviceWorkflowId = null;
        String childId = forward(holderId, messageId, args, debouncePeriod);
        if (childId != null) {
          return client.retrieveWorkflow(childId);
        }
        silentAcks = holderId.equals(silentHolderId) ? silentAcks + 1 : 1;
        silentHolderId = holderId;
        if (silentAcks < Constants.DEBOUNCER_MAX_SILENT_ACKS) {
          logger.debug("Debouncer {} did not ack message {}; retrying", holderId, messageId);
          if (userQueueName != null) {
            // An enqueue on the user queue would not collide with the service workflow, which
            // holds the key on the internal queue, so look there again. Without a queue the
            // enqueue below collides with it instead.
            var holder =
                client.findDeduplicationHolder(Constants.DBOS_INTERNAL_QUEUE, deduplicationId);
            if (holder != null
                && holder.isDebouncerService()
                && !holder.isForeignTo(client.applicationName())) {
              serviceWorkflowId = holder.workflowId();
              continue;
            }
          }
        } else {
          silentHolderId = null;
          String promisedId = client.takeOverStrandedDebouncer(holderId);
          if (promisedId != null) {
            userWorkflowId = promisedId;
            takingOver = true;
          }
        }
      }

      Instant deadline = debounceTimeout == null ? null : Instant.now().plus(debounceTimeout);
      var enqueueOpts =
          new EnqueueOptions(workflowName, className, instanceName, QueueName.of(targetQueue))
              .withWorkflowId(userWorkflowId)
              .withDeduplicationId(deduplicationId)
              .withDelay(debouncePeriod)
              .withPriority(priority)
              .withAppVersion(appVersion)
              .withTimeout(Timeout.of(workflowTimeout))
              .withAttributes(attributes)
              .withSerialization(serialization);
      try {
        WorkflowHandle<R, ?> handle = client.enqueueDebounced(enqueueOpts, args, deadline);
        if (takingOver && !client.isDebouncedWorkflow(userWorkflowId)) {
          // The service workflow was only slow: it started the promised workflow between the
          // cancel and this enqueue, and this call's arguments went nowhere. Start over under this
          // call's own id.
          logger.debug(
              "Debounced workflow {} was started by its service workflow; retrying",
              userWorkflowId);
          takingOver = false;
          userWorkflowId = ids.userWorkflowId();
          continue;
        }
        return handle;
      } catch (DBOSQueueDuplicatedException dup) {
        // Someone took the key between the first bounce and this enqueue. If it is a debounced
        // workflow waiting there, this bounce extends it; otherwise the result reports the holder.
        var result =
            (DebounceResult)
                client.debounceDelayedWorkflow(
                    workflowName,
                    className,
                    instanceName,
                    targetQueue,
                    deduplicationId,
                    delayUntil(debouncePeriod),
                    args,
                    serialization,
                    null);
        if (result instanceof DebounceResult.Bounced b) {
          return client.retrieveWorkflow(b.bouncedWorkflowId());
        }
        var holder = ((DebounceResult.NotBounced) result).holder();
        if (holder == null) {
          logger.debug(
              "Debounce holder for dedupId {} not found after conflict; retrying", deduplicationId);
          continue;
        }
        // A peer's holder is not ours to extend: it dequeues on that application's account, so
        // it may never run at all from here, and a retry would spin forever. Surface the collision
        // the way a plain deduplicated enqueue would.
        if (holder.isForeignTo(client.applicationName())) {
          throw new DBOSQueueDuplicatedException(userWorkflowId, targetQueue, deduplicationId);
        }
        if (holder.isDebouncerService()) {
          serviceWorkflowId = holder.workflowId();
          continue;
        }
        if (holder.isDebouncedInstanceOf(workflowName, className, instanceName)) {
          // A debounced instance of this workflow holds the key but is no longer DELAYED: it left
          // that state between the enqueue attempt and the bounce, and its key is about to clear.
          logger.debug(
              "Debounced workflow {} for dedupId {} is no longer delayed; retrying",
              holder.workflowId(),
              deduplicationId);
          continue;
        }
        // Held by a workflow this debounce must not touch: one that was deduplicated on its own,
        // or a different workflow whose debounce key collides with ours.
        throw new DBOSQueueDuplicatedException(userWorkflowId, targetQueue, deduplicationId);
      }
    }
  }

  /**
   * Forwards this call's arguments to a debouncer service workflow and returns the user workflow id
   * it publishes, or null if it did not acknowledge in time.
   */
  private @Nullable String forward(
      String serviceWorkflowId, String messageId, Object[] args, Duration debouncePeriod) {
    DebouncerMessage msg = new DebouncerMessage(messageId, args, debouncePeriod);
    client.send(serviceWorkflowId, msg, Constants.DEBOUNCER_TOPIC, messageId);
    var ack = client.getEvent(serviceWorkflowId, messageId, Constants.DEBOUNCER_ACK_TIMEOUT);
    if (ack.isEmpty()) {
      return null;
    }
    // The service workflow publishes the child id as its first action, before its receive loop.
    // If the ack arrived the event should be there; treat a miss as no ack, and send again.
    var childId =
        client.getEvent(
            serviceWorkflowId, Constants.DEBOUNCER_CHILD_ID_KEY, Constants.DEBOUNCER_ACK_TIMEOUT);
    return childId.map(id -> (String) id).orElse(null);
  }

  private static long delayUntil(Duration debouncePeriod) {
    return Instant.now().plus(debouncePeriod).toEpochMilli();
  }
}
