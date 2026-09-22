package dev.dbos.transact.workflow;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.exceptions.DBOSDebouncerUnreachableException;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.execution.ThrowingRunnable;
import dev.dbos.transact.execution.ThrowingSupplier;
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
 * Debounces a series of workflow invocations on the same key into a single execution that uses the
 * most recently supplied arguments.
 *
 * <p>Each unique {@code debounceKey} maintains its own debouncer service workflow that absorbs
 * incoming calls. The service workflow starts the actual user workflow after either {@code
 * debouncePeriod} has elapsed since the last incoming call or the absolute {@code debounceTimeout}
 * has expired.
 *
 * <p>The returned {@link WorkflowHandle} points to the user workflow that will eventually run with
 * the latest arguments; polling it for {@code getResult()} waits for that workflow's outcome.
 *
 * <h2>Example</h2>
 *
 * <pre>{@code
 * var dbos = new DBOS(config);
 * var svc = dbos.registerProxy(MyService.class, new MyServiceImpl());
 * dbos.launch();
 *
 * var debouncer = dbos.<String>debouncer()
 *     .withDebounceTimeout(Duration.ofMinutes(5));
 *
 * WorkflowHandle<String, Exception> handle = debouncer.debounce(
 *     "user-42",
 *     Duration.ofSeconds(2),
 *     () -> svc.process("payload"));
 * String result = handle.getResult();
 * }</pre>
 *
 * @param <R> return type of the debounced workflow
 */
public final class Debouncer<R> {

  private static final Logger logger = LoggerFactory.getLogger(Debouncer.class);

  /**
   * What the first step of a debounce records. The name and the first two components predate the
   * bounce and are kept as they are so that steps recorded by older versions still replay.
   */
  record DebounceIds(String userWorkflowId, String messageId, @Nullable String bouncedWorkflowId) {}

  private final DBOS dbos;
  private final DBOSExecutor executor;
  private final RegisteredWorkflow debouncerWorkflow;
  private final @Nullable String queueName;
  private final @Nullable Duration debounceTimeout;
  private final @Nullable String appVersion;
  private final @Nullable Integer priority;
  private final @Nullable String deduplicationId;

  public Debouncer(
      @NonNull DBOS dbos,
      @NonNull DBOSExecutor executor,
      @NonNull RegisteredWorkflow debouncerWorkflow) {
    this(dbos, executor, debouncerWorkflow, null, null, null, null, null);
  }

  private Debouncer(
      DBOS dbos,
      DBOSExecutor executor,
      RegisteredWorkflow debouncerWorkflow,
      @Nullable String queueName,
      @Nullable Duration debounceTimeout,
      @Nullable String appVersion,
      @Nullable Integer priority,
      @Nullable String deduplicationId) {
    this.dbos = Objects.requireNonNull(dbos, "dbos must not be null");
    this.executor = Objects.requireNonNull(executor, "executor must not be null");
    this.debouncerWorkflow =
        Objects.requireNonNull(debouncerWorkflow, "debouncerWorkflow must not be null");
    this.queueName = queueName;
    this.debounceTimeout = debounceTimeout;
    this.appVersion = appVersion;
    this.priority = priority;
    this.deduplicationId = deduplicationId;
  }

  /**
   * Set the queue that the user workflow will be enqueued on when the debounce period elapses.
   * {@code null} starts the user workflow directly (not enqueued).
   */
  public @NonNull Debouncer<R> withQueue(@Nullable String queueName) {
    if (queueName != null && queueName.isEmpty()) {
      throw new IllegalArgumentException("queueName must not be empty");
    }
    return new Debouncer<>(
        dbos,
        executor,
        debouncerWorkflow,
        queueName,
        debounceTimeout,
        appVersion,
        priority,
        deduplicationId);
  }

  /**
   * Returns a copy of this debouncer that enqueues the debounced workflow on {@code queue}.
   *
   * @param queue name of the queue to enqueue on
   * @return a copy with the queue set
   */
  public @NonNull Debouncer<R> withQueue(@NonNull QueueName queue) {
    return withQueue(queue.value());
  }

  /**
   * @deprecated Use {@link #withQueue(QueueName)}.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public @NonNull Debouncer<R> withQueue(@NonNull Queue queue) {
    return withQueue(queue.name());
  }

  /**
   * Set an absolute cap on how long a debouncer for a single key may keep absorbing calls. After
   * this duration elapses from the first call, the user workflow is started even if more calls keep
   * arriving.
   */
  public @NonNull Debouncer<R> withDebounceTimeout(@Nullable Duration debounceTimeout) {
    return new Debouncer<>(
        dbos,
        executor,
        debouncerWorkflow,
        queueName,
        debounceTimeout,
        appVersion,
        priority,
        deduplicationId);
  }

  /** Target a specific application version for the user workflow. */
  public @NonNull Debouncer<R> withAppVersion(@Nullable String appVersion) {
    return new Debouncer<>(
        dbos,
        executor,
        debouncerWorkflow,
        queueName,
        debounceTimeout,
        appVersion,
        priority,
        deduplicationId);
  }

  /** Set the priority for the user workflow (only applies when a queue is configured). */
  public @NonNull Debouncer<R> withPriority(@Nullable Integer priority) {
    return new Debouncer<>(
        dbos,
        executor,
        debouncerWorkflow,
        queueName,
        debounceTimeout,
        appVersion,
        priority,
        deduplicationId);
  }

  /**
   * Set a deduplication ID to be forwarded to the user workflow.
   *
   * @deprecated A debounced workflow's deduplication ID is its debounce key. A caller-supplied one
   *     cannot be honoured once the debouncer holds that key on the user workflow itself, which the
   *     next release does. From then on the value is ignored; the method is removed in 2.0.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public @NonNull Debouncer<R> withDeduplicationId(@Nullable String deduplicationId) {
    return new Debouncer<>(
        dbos,
        executor,
        debouncerWorkflow,
        queueName,
        debounceTimeout,
        appVersion,
        priority,
        deduplicationId);
  }

  /**
   * Debounce a workflow with no return value.
   *
   * @param debounceKey key that groups concurrent calls; calls with the same key are coalesced
   * @param debouncePeriod inactivity window before the user workflow runs; each call resets it
   * @param wfLambda lambda calling exactly one {@code @Workflow} method
   * @return handle to the future user workflow
   */
  public @NonNull <E extends Exception> WorkflowHandle<Void, E> debounce(
      @NonNull String debounceKey,
      @NonNull Duration debouncePeriod,
      @NonNull ThrowingRunnable<E> wfLambda) {
    return debounceInternal(
        debounceKey,
        debouncePeriod,
        () -> {
          wfLambda.execute();
          return null;
        });
  }

  /**
   * Debounce a workflow with a return value.
   *
   * @param debounceKey key that groups concurrent calls; calls with the same key are coalesced
   * @param debouncePeriod inactivity window before the user workflow runs; each call resets it
   * @param wfLambda lambda calling exactly one {@code @Workflow} method
   * @return handle to the future user workflow
   */
  public @NonNull <E extends Exception> WorkflowHandle<R, E> debounce(
      @NonNull String debounceKey,
      @NonNull Duration debouncePeriod,
      @NonNull ThrowingSupplier<R, E> wfLambda) {
    return debounceInternal(debounceKey, debouncePeriod, wfLambda);
  }

  private <T, E extends Exception> WorkflowHandle<T, E> debounceInternal(
      @NonNull String debounceKey,
      @NonNull Duration debouncePeriod,
      @NonNull ThrowingSupplier<T, E> wfLambda) {

    Objects.requireNonNull(debounceKey, "debounceKey must not be null");
    Objects.requireNonNull(debouncePeriod, "debouncePeriod must not be null");
    Objects.requireNonNull(wfLambda, "wfLambda must not be null");
    if (debouncePeriod.isNegative() || debouncePeriod.isZero()) {
      throw new IllegalArgumentException("debouncePeriod must be a positive non-zero duration");
    }

    DBOSExecutor.Invocation invocation = executor.captureInvocation(wfLambda);
    RegisteredWorkflow userWorkflow =
        executor
            .getRegisteredWorkflow(
                invocation.workflowName(), invocation.className(), invocation.instanceName())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Workflow %s is not registered".formatted(invocation.fqName())));
    String debouncerDeduplicationId = invocation.workflowName() + "-" + debounceKey;

    // The first step assigns the ids and, when a queue is configured, tries to extend a debounced
    // holder waiting on that queue. Inside a workflow it is recorded so replay is deterministic;
    // runDbosFunctionAsStep runs the lambda directly when not in a workflow. Typed as Object so
    // that replay does not cast the recorded value: a serializer that drops Java types hands back
    // a map, and a step recorded before this SDK bounced holds only the two ids.
    Object recordedStart =
        executor.runDbosFunctionAsStep(
            () ->
                (Object)
                    startDebounce(
                        userWorkflow, debouncerDeduplicationId, debouncePeriod, invocation),
            "DBOS.assignDebounceIds",
            null);
    DebounceIds ids = toDebounceIds(recordedStart);
    if (ids.bouncedWorkflowId() != null) {
      // A debounced workflow was already waiting on the configured queue; it now carries our args.
      return dbos.retrieveWorkflow(ids.bouncedWorkflowId());
    }
    String userWorkflowId = ids.userWorkflowId();
    String messageId = ids.messageId();

    DebouncerOptions options =
        new DebouncerOptions(
            invocation.workflowName(),
            invocation.className(),
            invocation.instanceName(),
            queueName,
            debounceTimeout,
            appVersion,
            priority,
            deduplicationId);
    Duration workflowTimeout = DBOS.inWorkflow() ? DBOSContextHolder.get().getTimeout() : null;
    var workflowAttributes = DBOSContextHolder.get().resolveNextAttributes();
    DebouncerContextOptions ctx =
        new DebouncerContextOptions(userWorkflowId, workflowTimeout, workflowAttributes);
    DebouncerMessage initial = new DebouncerMessage(messageId, invocation.args(), debouncePeriod);

    // Consecutive unacknowledged sends to one service workflow; reset when the holder changes.
    String silentHolderId = null;
    int silentAcks = 0;
    while (true) {
      try {
        var startOpts =
            new StartWorkflowOptions()
                .withQueue(Constants.DBOS_INTERNAL_QUEUE)
                .withDeduplicationId(debouncerDeduplicationId);
        executor.startRegisteredWorkflow(
            debouncerWorkflow, new Object[] {options, ctx, initial}, startOpts);
        // Successfully enqueued a fresh debouncer for this key.
        return dbos.retrieveWorkflow(userWorkflowId);
      } catch (DBOSQueueDuplicatedException dup) {
        // Something already holds this key on the internal queue. If it is a debounced workflow
        // waiting there, this bounce extends it and the conflict is resolved in one round trip.
        // Otherwise the result reports the holder so we can coordinate with it or refuse.
        // When called from inside a workflow, record the result as a durable step so that
        // replay returns the same holder and the subsequent send/getEvent steps stay
        // deterministic. Typed as Object so that replay does not cast the recorded value: a step
        // recorded before this SDK bounced holds a holder, or before application names a bare
        // workflow id, and toDebounceResult adapts both.
        //
        // Unless a bounce happened, what is recorded is the holder alone, the shape the previous
        // version reads. With a pinned application version a workflow this node records can be
        // recovered by a node of that version, and a bounce can only happen against a row a
        // newer version wrote, which never shares a fleet with the previous one. So every step
        // recorded in a fleet the previous version is part of stays readable by it.
        Object recorded =
            executor.runDbosFunctionAsStep(
                () -> {
                  var bounce =
                      executor.debounceDelayedWorkflow(
                          userWorkflow,
                          Constants.DBOS_INTERNAL_QUEUE,
                          debouncerDeduplicationId,
                          delayUntil(debouncePeriod),
                          invocation.args());
                  return bounce instanceof DebounceResult.NotBounced not
                      ? (Object) not.holder()
                      : bounce;
                },
                "DBOS.lookupDebouncer",
                null);
        DebounceResult result = toDebounceResult(recorded);
        if (result instanceof DebounceResult.Bounced bounced) {
          return dbos.retrieveWorkflow(bounced.bouncedWorkflowId());
        }
        DeduplicationHolder holder = ((DebounceResult.NotBounced) result).holder();
        if (holder == null) {
          // The existing holder finished between the enqueue attempt and now. Retry from
          // scratch — the next enqueue should succeed.
          logger.debug(
              "Debouncer for dedupId {} not found after conflict; retrying",
              debouncerDeduplicationId);
          continue;
        }
        // A peer's holder is not ours to extend: it dequeues on that application's account, so
        // it may never run at all from here, and the retry below would spin forever waiting for an
        // ack. Surface the collision the way a plain deduplicated enqueue would.
        if (holder.isForeignTo(executor.appName())) {
          throw new DBOSQueueDuplicatedException(
              userWorkflowId, Constants.DBOS_INTERNAL_QUEUE, debouncerDeduplicationId);
        }
        if (!holder.isDebouncerService()) {
          if (holder.isDebouncedInstanceOf(
              invocation.workflowName(), invocation.className(), invocation.instanceName())) {
            // A debounced instance of this workflow holds the key but is no longer DELAYED: it
            // left that state between the enqueue attempt and the bounce, and its key is about to
            // clear. Retry; the next enqueue starts a fresh debouncer.
            logger.debug(
                "Debounced workflow {} for dedupId {} is no longer delayed; retrying",
                holder.workflowId(),
                debouncerDeduplicationId);
            continue;
          }
          // Held by a workflow this debounce must not touch: one that was deduplicated on its
          // own, or a different workflow whose debounce key collides with ours.
          throw new DBOSQueueDuplicatedException(
              userWorkflowId, Constants.DBOS_INTERNAL_QUEUE, debouncerDeduplicationId);
        }
        // A debouncer service workflow for this key is running. Forward the latest args to it.
        String existingDebouncerId = holder.workflowId();
        DebouncerMessage msg = new DebouncerMessage(messageId, invocation.args(), debouncePeriod);
        // messageId is the idempotency key — exactly-once delivery. Internal, because the
        // debouncer reads it back as a DebouncerMessage: it must not inherit a portable format
        // from whatever workflow called debounce().
        executor.sendInternal(existingDebouncerId, msg, Constants.DEBOUNCER_TOPIC, messageId);

        // Wait for the debouncer to acknowledge receipt. If the debouncer exited before
        // processing this message, no ack arrives — start over.
        var ack = dbos.getEvent(existingDebouncerId, messageId, Constants.DEBOUNCER_ACK_TIMEOUT);
        if (ack.isEmpty()) {
          silentAcks = existingDebouncerId.equals(silentHolderId) ? silentAcks + 1 : 1;
          silentHolderId = existingDebouncerId;
          if (silentAcks >= Constants.DEBOUNCER_MAX_SILENT_ACKS) {
            throw new DBOSDebouncerUnreachableException(
                existingDebouncerId, Constants.DBOS_INTERNAL_QUEUE, debouncerDeduplicationId);
          }
          logger.debug(
              "Debouncer {} did not ack message {}; retrying", existingDebouncerId, messageId);
          continue;
        }
        // CHILD_ID_KEY is set as the debouncer workflow's first action, before the recv-loop.
        // If the ack arrived, the debouncer has already published this event — it cannot be empty.
        var childId =
            dbos.<String>getEvent(
                    existingDebouncerId,
                    Constants.DEBOUNCER_CHILD_ID_KEY,
                    Constants.DEBOUNCER_ACK_TIMEOUT)
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Debouncer "
                                + existingDebouncerId
                                + " acked but did not publish "
                                + Constants.DEBOUNCER_CHILD_ID_KEY));
        return dbos.retrieveWorkflow(childId);
      }
    }
  }

  /**
   * The first step of a debounce: assign the user workflow and message ids and, when a queue is
   * configured, try to extend a debounced instance of the workflow waiting DELAYED on that queue.
   * That is where a newer SDK version keeps its debounced workflows, so a fleet mixing the two
   * keeps coalescing on one key.
   */
  private DebounceIds startDebounce(
      RegisteredWorkflow userWorkflow,
      String debouncerDeduplicationId,
      Duration debouncePeriod,
      DBOSExecutor.Invocation invocation) {
    String userWorkflowId = DBOSContextHolder.get().getNextWorkflowId(UUID.randomUUID().toString());
    String messageId = UUID.randomUUID().toString();
    String bouncedWorkflowId = null;
    if (queueName != null
        && executor.debounceDelayedWorkflow(
                userWorkflow,
                queueName,
                debouncerDeduplicationId,
                delayUntil(debouncePeriod),
                invocation.args())
            instanceof DebounceResult.Bounced bounced) {
      bouncedWorkflowId = bounced.bouncedWorkflowId();
    }
    return new DebounceIds(userWorkflowId, messageId, bouncedWorkflowId);
  }

  private static long delayUntil(Duration debouncePeriod) {
    return Instant.now().plus(debouncePeriod).toEpochMilli();
  }

  /**
   * Adapts a recorded {@code DBOS.assignDebounceIds} step to the shape this version expects.
   *
   * <p>Before this SDK bounced, the step recorded only the two ids; a replay of such a step means
   * nothing was extended. A serializer that does not carry Java type information hands back a map
   * rather than the record, so that shape is adapted too.
   */
  static DebounceIds toDebounceIds(Object recorded) {
    if (recorded instanceof DebounceIds ids) {
      return ids;
    }
    if (recorded instanceof Map<?, ?> map
        && map.get("userWorkflowId") instanceof String userWorkflowId
        && map.get("messageId") instanceof String messageId) {
      return new DebounceIds(
          userWorkflowId,
          messageId,
          map.get("bouncedWorkflowId") instanceof String bounced ? bounced : null);
    }
    throw new IllegalStateException(
        "DBOS.assignDebounceIds recorded an unexpected %s"
            .formatted(recorded == null ? "null" : recorded.getClass().getName()));
  }

  /**
   * Adapts a recorded {@code DBOS.lookupDebouncer} step to the shape this version expects.
   *
   * <p>Before this SDK bounced, the step recorded the holder alone; before application names, the
   * holder's workflow id on its own. A workflow that recorded one under those versions and replays
   * under this one still has to resume. That replay only happens when the application version is
   * pinned across the upgrade — patching mode does exactly that — because the SDK version is
   * otherwise hashed into the computed application version, and recovery only claims workflows
   * matching it.
   *
   * <p>Either older shape means nothing was bounced, and the holder it names was a debouncer
   * service workflow: nothing else held a debounce key then. {@link #toDeduplicationHolder} reports
   * such a holder with no name, which {@link DeduplicationHolder#isDebouncerService} reads as
   * exactly that.
   *
   * <p>A serializer that does not carry Java type information hands back a map rather than the
   * record it recorded, so that shape is adapted too.
   */
  static DebounceResult toDebounceResult(@Nullable Object recorded) {
    if (recorded instanceof DebounceResult result) {
      return result;
    }
    if (recorded instanceof Map<?, ?> map) {
      if (map.get("bouncedWorkflowId") instanceof String bounced) {
        return new DebounceResult.Bounced(bounced);
      }
      if (map.isEmpty() || map.containsKey("holder")) {
        // A NotBounced as such: an empty map under a serializer that drops nulls, or its holder
        // wrapped. Only the holder itself is recorded, but the wrapped shape is read too.
        return new DebounceResult.NotBounced(toDeduplicationHolder(map.get("holder")));
      }
    }
    return new DebounceResult.NotBounced(toDeduplicationHolder(recorded));
  }

  /**
   * Adapts a recorded holder to the shape this version expects: a bare workflow id from before
   * application names, a holder from before this SDK bounced, or either as a map from a serializer
   * that does not preserve Java types.
   *
   * <p>A holder from before ownership is reported unclaimed. That is also how it behaved when it
   * was recorded: every application sharing the system database treated it as its own.
   */
  static @Nullable DeduplicationHolder toDeduplicationHolder(@Nullable Object recorded) {
    if (recorded == null) {
      return null;
    }
    if (recorded instanceof DeduplicationHolder holder) {
      return holder;
    }
    if (recorded instanceof String workflowId) {
      return new DeduplicationHolder(workflowId, null, null, null, null, null, false);
    }
    // A serializer that does not preserve Java types -- the portable one, or a custom JSON one --
    // round-trips the record to a map. Everything the record held is still there; only the type
    // was lost.
    if (recorded instanceof Map<?, ?> map && map.get("workflowId") instanceof String workflowId) {
      return new DeduplicationHolder(
          workflowId,
          map.get("applicationName") instanceof String appName ? appName : null,
          map.get("workflowName") instanceof String name ? name : null,
          map.get("className") instanceof String className ? className : null,
          map.get("instanceName") instanceof String instanceName ? instanceName : null,
          map.get("status") instanceof String status ? WorkflowState.valueOf(status) : null,
          map.get("isDebounced") instanceof Boolean debounced && debounced);
    }
    throw new IllegalStateException(
        "DBOS.lookupDebouncer recorded an unexpected %s".formatted(recorded.getClass().getName()));
  }
}
