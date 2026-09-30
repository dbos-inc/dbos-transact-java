package dev.dbos.transact.workflow;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.execution.ThrowingRunnable;
import dev.dbos.transact.execution.ThrowingSupplier;
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
 * Debounces a series of workflow invocations on the same key into a single execution that uses the
 * most recently supplied arguments.
 *
 * <p>The first call on a {@code debounceKey} enqueues the user workflow itself, DELAYED on its
 * queue -- the one set with {@link #withQueue}, or the internal queue -- holding the key as its
 * deduplication ID. Each later call on the key extends that delay by {@code debouncePeriod} and
 * replaces the workflow's arguments, up to the absolute {@code debounceTimeout} measured from the
 * first call. Once the delay expires the workflow is dequeued and runs with the latest arguments,
 * and the key is free: the next call starts a fresh workflow.
 *
 * <p>SDK versions before 1.2 debounced through a debouncer workflow that absorbed calls for the key
 * and started the user workflow when the period elapsed. When a fleet mixes the two, a call here
 * that finds such a debouncer workflow forwards to it, so every call on a key still coalesces into
 * one execution. A debouncer workflow that stops answering -- its SDK version has left the fleet,
 * so nothing will ever run it -- is cancelled, and the user workflow it promised is created here
 * instead, under the id earlier callers were given.
 *
 * <p>The returned {@link WorkflowHandle} points to the user workflow that will eventually run with
 * the latest arguments; polling it for {@code getResult()} waits for that workflow's outcome.
 *
 * <p>The user workflow takes the timeout set with {@link #withTimeout}, or failing that one set
 * with {@code WorkflowOptions} around the {@code debounce} call, timed from when it is dequeued. It
 * inherits neither the calling workflow's timeout nor its deadline, and a deadline set with {@code
 * WorkflowOptions} is ignored: the workflow may start long after the call.
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
   * bounce and are kept as they are so that steps recorded by older versions still replay; a
   * component an older version did not record replays as null.
   *
   * <p>Internal: not part of the public API. It is public only because other DBOS packages use it,
   * and it stays here, under this name, because recorded steps name its class.
   *
   * @param userWorkflowId the pre-assigned id of the workflow this call creates, if it creates one
   * @param messageId the idempotency key of the call, if it is forwarded to a debouncer workflow
   * @param bouncedWorkflowId the debounced workflow the first bounce extended, or null
   * @param debouncerWorkflowId a debouncer workflow of this application found holding the key on
   *     the internal queue when nothing was extended, or null. Only an SDK version before debounced
   *     workflows starts one; the call forwards to it rather than creating a second workflow beside
   *     it.
   */
  public record DebounceIds(
      String userWorkflowId,
      String messageId,
      @Nullable String bouncedWorkflowId,
      @Nullable String debouncerWorkflowId) {

    /** These ids, completed with what a bounce extended, or else the debouncer workflow found. */
    public DebounceIds withBounced(DebounceResult bounce, @Nullable String debouncerWorkflowId) {
      return bounce instanceof DebounceResult.Bounced bounced
          ? new DebounceIds(userWorkflowId, messageId, bounced.bouncedWorkflowId(), null)
          : new DebounceIds(userWorkflowId, messageId, null, debouncerWorkflowId);
    }
  }

  private final DBOS dbos;
  private final DBOSExecutor executor;
  private final @Nullable String queueName;
  private final @Nullable Duration debounceTimeout;
  private final @Nullable String appVersion;
  private final @Nullable Integer priority;
  private final @Nullable Duration workflowTimeout;

  public Debouncer(@NonNull DBOS dbos, @NonNull DBOSExecutor executor) {
    this(dbos, executor, null, null, null, null, null);
  }

  private Debouncer(
      DBOS dbos,
      DBOSExecutor executor,
      @Nullable String queueName,
      @Nullable Duration debounceTimeout,
      @Nullable String appVersion,
      @Nullable Integer priority,
      @Nullable Duration workflowTimeout) {
    this.dbos = Objects.requireNonNull(dbos, "dbos must not be null");
    this.executor = Objects.requireNonNull(executor, "executor must not be null");
    this.queueName = queueName;
    this.debounceTimeout = debounceTimeout;
    this.appVersion = appVersion;
    this.priority = priority;
    this.workflowTimeout = workflowTimeout;
  }

  /**
   * Set the queue that the user workflow waits and runs on. {@code null} uses the internal queue.
   */
  public @NonNull Debouncer<R> withQueue(@Nullable String queueName) {
    if (queueName != null && queueName.isEmpty()) {
      throw new IllegalArgumentException("queueName must not be empty");
    }
    return new Debouncer<>(
        dbos, executor, queueName, debounceTimeout, appVersion, priority, workflowTimeout);
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
        dbos, executor, queueName, debounceTimeout, appVersion, priority, workflowTimeout);
  }

  /** Target a specific application version for the user workflow. */
  public @NonNull Debouncer<R> withAppVersion(@Nullable String appVersion) {
    return new Debouncer<>(
        dbos, executor, queueName, debounceTimeout, appVersion, priority, workflowTimeout);
  }

  /**
   * Set the priority for the user workflow; lower values are dequeued first. A priority only means
   * something on a queue of your own, so {@link #debounce} rejects one when no queue is configured.
   *
   * @throws IllegalArgumentException if {@code priority} is negative
   */
  public @NonNull Debouncer<R> withPriority(@Nullable Integer priority) {
    if (priority != null && priority < 0) {
      throw new IllegalArgumentException("priority must not be negative");
    }
    return new Debouncer<>(
        dbos, executor, queueName, debounceTimeout, appVersion, priority, workflowTimeout);
  }

  /**
   * Set a timeout for every user workflow this debouncer starts, timed from when that workflow is
   * dequeued. It takes precedence over a timeout set with {@code WorkflowOptions} around the {@code
   * debounce} call; {@code null} leaves that one to apply.
   */
  public @NonNull Debouncer<R> withTimeout(@Nullable Duration timeout) {
    if (timeout != null && (timeout.isNegative() || timeout.isZero())) {
      throw new IllegalArgumentException("timeout must be a positive non-zero duration");
    }
    return new Debouncer<>(
        dbos, executor, queueName, debounceTimeout, appVersion, priority, timeout);
  }

  /**
   * Ignored: the debounced workflow holds its debounce key as its deduplication ID.
   *
   * @deprecated Ignored since 1.2. To be removed in a future release.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public @NonNull Debouncer<R> withDeduplicationId(@Nullable String deduplicationId) {
    return this;
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

  // A debounce is a bounce, else a create:
  //
  // 1. The first step tries to extend (bounce) the DELAYED debounced workflow already waiting on
  //    the key. That is every call on a key but the first, and it returns right there.
  // 2. Otherwise the loop enqueues the debounced workflow. If something takes the key first, it
  //    bounces again or classifies the holder, and retries or refuses.
  //
  // The rest is interop with 1.1, marked where it appears. That release debounced through a
  // debouncer workflow on the internal queue instead. A call that finds one forwards to it, and
  // takes it over once it stops answering. All of that can go once neither 1.1 nodes nor workflows
  // it recorded remain.
  //
  // Inside a workflow every database touch below is a step or a child enqueue, and the sequence is
  // the one 1.1 recorded: DBOS.assignDebounceIds first, then the child-enqueue slot, and on a
  // duplicate DBOS.lookupDebouncer followed by the send and the two getEvent steps. A debounce
  // recorded by that release therefore resumes here when the application version is pinned across
  // the upgrade.
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
    if (priority != null && queueName == null) {
      throw new IllegalArgumentException(
          "a queue must be configured with withQueue to specify a priority");
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
    String targetQueue = queueName != null ? queueName : Constants.DBOS_INTERNAL_QUEUE;
    String debounceDeduplicationId = invocation.workflowName() + "-" + debounceKey;
    Object[] args = invocation.args();

    // The debouncer's own timeout, else only one the caller set for this call, as in Python and
    // TypeScript. The running workflow's own timeout is its budget, not the debounced workflow's.
    var callerCtx = DBOSContextHolder.get();
    Duration timeout =
        this.workflowTimeout != null
            ? this.workflowTimeout
            : callerCtx.getNextTimeout() instanceof Timeout.Explicit e ? e.value() : null;
    var workflowAttributes = callerCtx.resolveNextAttributes();

    // Step 1. The first step assigns the ids and tries to extend a debounced workflow already
    // waiting on the target queue. Inside a workflow the bounce and its checkpoint commit in one
    // transaction, so a crash cannot leave a row extended but the step unrecorded, which on replay
    // would bounce again. Typed as Object so that replay does not cast the recorded value: a custom
    // serializer that drops Java types hands back a map.
    //
    // Passing the ids is what makes this the first step, and it does one more thing (1.1 interop):
    // when nothing was extended, the same transaction looks for a debouncer workflow holding the
    // key on the internal queue, returned as ids.debouncerWorkflowId().
    DebounceIds ids =
        toDebounceIds(
            executor.debounceDelayedWorkflow(
                userWorkflow,
                targetQueue,
                debounceDeduplicationId,
                delayUntil(debouncePeriod),
                args,
                "DBOS.assignDebounceIds",
                freshDebounceIds()));
    if (ids.bouncedWorkflowId() != null) {
      // Extended: that workflow now runs with this call's arguments, and nothing is left to do.
      return dbos.retrieveWorkflow(ids.bouncedWorkflowId());
    }

    // Step 2. Nothing was extended, so create the debounced workflow, retrying until this call
    // either creates it, extends one that got there first, or hands its arguments to a debouncer
    // workflow.
    String userWorkflowId = ids.userWorkflowId();
    // Only used to forward to a debouncer workflow (1.1 interop).
    String messageId = ids.messageId();
    // A debouncer workflow to forward this call to before trying to create anything (1.1 interop).
    String debouncerWorkflowId = ids.debouncerWorkflowId();
    // Set while creating the workflow a stranded debouncer workflow promised, under its id.
    boolean takingOver = false;
    // Consecutive unacknowledged forwards to one debouncer workflow; reset when the holder changes.
    String silentHolderId = null;
    int silentAcks = 0;

    while (true) {
      // 1.1 interop: forward this call to the debouncer workflow holding the key. A live one acks
      // and publishes the user workflow it will start. One that stays silent is rechecked, and
      // after enough silence taken over: cancelled, and its promised workflow created below.
      if (debouncerWorkflowId != null) {
        String holderId = debouncerWorkflowId;
        debouncerWorkflowId = null;
        String childId = forwardToDebouncerWorkflow(holderId, messageId, args, debouncePeriod);
        if (childId != null) {
          return dbos.retrieveWorkflow(childId);
        }
        silentAcks = holderId.equals(silentHolderId) ? silentAcks + 1 : 1;
        silentHolderId = holderId;
        if (silentAcks < Constants.DEBOUNCER_MAX_SILENT_ACKS) {
          logger.debug("Debouncer {} did not ack message {}; retrying", holderId, messageId);
          if (queueName != null) {
            // An enqueue on the user queue would not collide with the debouncer workflow, which
            // holds the key on the internal queue, so look there again, as the previous release
            // did at this point. Without a queue the enqueue below collides with it instead.
            DebounceResult recheck =
                toDebounceResult(
                    executor.findDeduplicationHolder(
                        Constants.DBOS_INTERNAL_QUEUE, debounceDeduplicationId));
            if (recheck instanceof DebounceResult.Bounced bounced) {
              // Only on replay of a step 1.1 recorded, when it found a debounced workflow waiting
              // on the internal queue; the lookup itself never bounces.
              return dbos.retrieveWorkflow(bounced.bouncedWorkflowId());
            }
            var holder = ((DebounceResult.NotBounced) recheck).holder();
            if (holder != null
                && holder.isDebouncerWorkflow()
                && !holder.isForeignTo(executor.appName())) {
              debouncerWorkflowId = holder.workflowId();
              continue;
            }
          }
        } else {
          silentHolderId = null;
          String promisedId = executor.takeOverStrandedDebouncer(holderId);
          if (promisedId != null) {
            userWorkflowId = promisedId;
            takingOver = true;
          }
        }
      }

      // Create the debounced workflow, DELAYED by the period and holding the key.
      Instant deadline = debounceTimeout == null ? null : Instant.now().plus(debounceTimeout);
      try {
        WorkflowHandle<T, E> handle =
            executor.enqueueDebounced(
                userWorkflow,
                args,
                userWorkflowId,
                targetQueue,
                debounceDeduplicationId,
                debouncePeriod,
                deadline,
                priority,
                appVersion,
                timeout,
                workflowAttributes);
        if (!handle.workflowId().equals(userWorkflowId)) {
          // 1.1 interop: a replay of that release, which recorded its debouncer workflow in this
          // slot.
          // That workflow started the user workflow under the id the first step assigned.
          return dbos.retrieveWorkflow(userWorkflowId);
        }
        if (takingOver && !executor.isDebouncedWorkflow(userWorkflowId)) {
          // 1.1 interop: the debouncer workflow was only slow: it started the promised workflow
          // between the
          // cancel and this enqueue, with the arguments it had, and this call's arguments went
          // nowhere. Start over under this call's own id, as for any call that arrives after its
          // key's workflow has committed to run.
          logger.debug(
              "Debounced workflow {} was started by its debouncer workflow; retrying",
              userWorkflowId);
          takingOver = false;
          userWorkflowId = ids.userWorkflowId();
          continue;
        }
        return handle;
      } catch (DBOSQueueDuplicatedException dup) {
        // Someone took the key between the first step and this enqueue. If it is a debounced
        // workflow waiting there, this bounce extends it. Otherwise the result reports the holder
        // so we can coordinate with it or refuse. Recorded as a step, as the first one is, and
        // typed as Object for the same reason; a step recorded by a release before the bounce
        // holds a bare workflow id, and toDebounceResult adapts it.
        //
        // A takeover that loses the key here leaves the promised workflow uncreated, and handles
        // earlier callers hold to it wait. That needs a third call to land in the milliseconds
        // between the cancel and the enqueue.
        DebounceResult result =
            toDebounceResult(
                executor.debounceDelayedWorkflow(
                    userWorkflow,
                    targetQueue,
                    debounceDeduplicationId,
                    delayUntil(debouncePeriod),
                    args,
                    "DBOS.lookupDebouncer",
                    null));
        if (result instanceof DebounceResult.Bounced bounced) {
          return dbos.retrieveWorkflow(bounced.bouncedWorkflowId());
        }
        DeduplicationHolder holder = ((DebounceResult.NotBounced) result).holder();
        if (holder == null) {
          // The holder finished between the enqueue attempt and now. Retry from scratch.
          logger.debug(
              "Debounce holder for dedupId {} not found after conflict; retrying",
              debounceDeduplicationId);
          continue;
        }
        // A peer's holder is not ours to extend: it dequeues on that application's account, so
        // it may never run at all from here, and a retry would spin forever. Surface the collision
        // the way a plain deduplicated enqueue would.
        if (holder.isForeignTo(executor.appName())) {
          throw new DBOSQueueDuplicatedException(
              userWorkflowId, targetQueue, debounceDeduplicationId);
        }
        if (holder.isDebouncerWorkflow()) {
          // 1.1 interop: forward to it at the top of the loop.
          debouncerWorkflowId = holder.workflowId();
          continue;
        }
        if (holder.isDebouncedInstanceOf(
            invocation.workflowName(), invocation.className(), invocation.instanceName())) {
          // A debounced instance of this workflow holds the key but is no longer DELAYED: it left
          // that state between the enqueue attempt and the bounce, and its key is about to clear.
          logger.debug(
              "Debounced workflow {} for dedupId {} is no longer delayed; retrying",
              holder.workflowId(),
              debounceDeduplicationId);
          continue;
        }
        // Held by a workflow this debounce must not touch: one that was deduplicated on its own,
        // or a different workflow whose debounce key collides with ours.
        throw new DBOSQueueDuplicatedException(
            userWorkflowId, targetQueue, debounceDeduplicationId);
      }
    }
  }

  /**
   * Forwards this call's arguments to a debouncer workflow and returns the user workflow id it
   * publishes, or null if it did not acknowledge in time.
   */
  private @Nullable String forwardToDebouncerWorkflow(
      String debouncerWorkflowId, String messageId, Object[] args, Duration debouncePeriod) {
    DebouncerMessage msg = new DebouncerMessage(messageId, args, debouncePeriod);
    // messageId is the idempotency key -- exactly-once delivery. Internal, because the debouncer
    // workflow reads it back as a DebouncerMessage: it must not inherit a portable format from
    // whatever workflow called debounce().
    executor.sendInternal(debouncerWorkflowId, msg, Constants.DEBOUNCER_TOPIC, messageId);
    var ack = dbos.getEvent(debouncerWorkflowId, messageId, Constants.DEBOUNCER_ACK_TIMEOUT);
    if (ack.isEmpty()) {
      return null;
    }
    // The debouncer workflow publishes the child id as its first action, before its receive loop,
    // so once it has acked, the event is there.
    return dbos.<String>getEvent(
            debouncerWorkflowId, Constants.DEBOUNCER_CHILD_ID_KEY, Constants.DEBOUNCER_ACK_TIMEOUT)
        .orElseThrow(
            () ->
                new IllegalStateException(
                    "Debouncer "
                        + debouncerWorkflowId
                        + " acked but did not publish "
                        + Constants.DEBOUNCER_CHILD_ID_KEY));
  }

  /** The ids the first step assigns: the pre-assigned user workflow and the message id. */
  private DebounceIds freshDebounceIds() {
    return new DebounceIds(
        DBOSContextHolder.get().getNextWorkflowId(UUID.randomUUID().toString()),
        UUID.randomUUID().toString(),
        null,
        null);
  }

  private static long delayUntil(Duration debouncePeriod) {
    return Instant.now().plus(debouncePeriod).toEpochMilli();
  }

  /**
   * Adapts a recorded {@code DBOS.assignDebounceIds} step to the shape this version expects.
   *
   * <p>Before this SDK bounced, the step recorded only the two ids; a replay of such a step means
   * nothing was extended. Before 1.2 it recorded no debouncer workflow either, and its replay goes
   * on to the child-enqueue slot, where that release recorded the debouncer workflow it started. A
   * serializer that does not carry Java type information hands back a map rather than the record,
   * so that shape is adapted too.
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
          map.get("bouncedWorkflowId") instanceof String bounced ? bounced : null,
          map.get("debouncerWorkflowId") instanceof String debouncer ? debouncer : null);
    }
    throw new IllegalStateException(
        "DBOS.assignDebounceIds recorded an unexpected %s"
            .formatted(recorded == null ? "null" : recorded.getClass().getName()));
  }

  /**
   * Adapts a recorded {@code DBOS.lookupDebouncer} step to the shape this version expects.
   *
   * <p>Before this SDK bounced, the step recorded the holder's workflow id on its own. A workflow
   * that recorded one under that version and replays under this one still has to resume. That
   * replay only happens when the application version is pinned across the upgrade — patching mode
   * does exactly that — because the SDK version is otherwise hashed into the computed application
   * version, and recovery only claims workflows matching it.
   *
   * <p>That older shape means nothing was bounced, and the holder it names was a debouncer service
   * workflow: nothing else held a debounce key then. {@link #toDeduplicationHolder} reports such a
   * holder with no name, which {@link DeduplicationHolder#isDebouncerWorkflow} reads as exactly
   * that.
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
        // A NotBounced as this version records it, under a serializer that drops Java types: its
        // holder wrapped, or an empty map when the serializer drops nulls too.
        return new DebounceResult.NotBounced(toDeduplicationHolder(map.get("holder")));
      }
    }
    return new DebounceResult.NotBounced(toDeduplicationHolder(recorded));
  }

  /**
   * Adapts a recorded holder to the shape this version expects: a bare workflow id, which is what
   * the previous version recorded, or a holder as a map from a serializer that does not preserve
   * Java types.
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
    // A custom serializer that does not preserve Java types round-trips the record to a map.
    // Everything the record held is still there; only the type was lost.
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
