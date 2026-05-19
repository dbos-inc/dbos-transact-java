package dev.dbos.transact.workflow;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.execution.ThrowingRunnable;
import dev.dbos.transact.execution.ThrowingSupplier;
import dev.dbos.transact.internal.DBOSIntegration;
import dev.dbos.transact.workflow.internal.DebouncerContextOptions;
import dev.dbos.transact.workflow.internal.DebouncerMessage;
import dev.dbos.transact.workflow.internal.DebouncerOptions;
import dev.dbos.transact.workflow.internal.DebouncerService;

import java.time.Duration;
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
   * How long to wait for the debouncer service workflow to acknowledge a forwarded message before
   * retrying.
   */
  private static final Duration ACK_TIMEOUT = Duration.ofSeconds(1);

  private final DBOS dbos;
  private final DebouncerService debouncerProxy;
  private final @Nullable String queueName;
  private final @Nullable Duration debounceTimeout;

  public Debouncer(@NonNull DBOS dbos, @NonNull DebouncerService debouncerProxy) {
    this(dbos, debouncerProxy, null, null);
  }

  private Debouncer(
      DBOS dbos,
      DebouncerService debouncerProxy,
      @Nullable String queueName,
      @Nullable Duration debounceTimeout) {
    this.dbos = Objects.requireNonNull(dbos, "dbos must not be null");
    this.debouncerProxy = Objects.requireNonNull(debouncerProxy, "debouncerProxy must not be null");
    this.queueName = queueName;
    this.debounceTimeout = debounceTimeout;
  }

  /**
   * Set the queue that the user workflow will be enqueued on when the debounce period elapses.
   * {@code null} starts the user workflow directly (not enqueued).
   */
  public @NonNull Debouncer<R> withQueue(@Nullable String queueName) {
    return new Debouncer<>(dbos, debouncerProxy, queueName, debounceTimeout);
  }

  /** See {@link #withQueue(String)}. */
  public @NonNull Debouncer<R> withQueue(@NonNull Queue queue) {
    return withQueue(queue.name());
  }

  /**
   * Set an absolute cap on how long a debouncer for a single key may keep absorbing calls. After
   * this duration elapses from the first call, the user workflow is started even if more calls keep
   * arriving.
   */
  public @NonNull Debouncer<R> withDebounceTimeout(@Nullable Duration debounceTimeout) {
    return new Debouncer<>(dbos, debouncerProxy, queueName, debounceTimeout);
  }

  /**
   * Debounce a workflow with no return value. The supplier's return value is ignored — pass {@code
   * null} when no value is available.
   *
   * @param debounceKey key that groups concurrent calls; calls with the same key are coalesced
   * @param debouncePeriod inactivity window before the user workflow runs; each call resets it
   * @param wfLambda lambda calling exactly one {@code @Workflow} method
   * @return handle to the future user workflow
   */
  public @NonNull <E extends Exception> WorkflowHandle<Void, E> debounceVoid(
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

    DBOSIntegration.CapturedInvocation invocation = dbos.integration().captureInvocation(wfLambda);

    String userWorkflowId = UUID.randomUUID().toString();
    String messageId = UUID.randomUUID().toString();
    String deduplicationId = invocation.workflowName() + "-" + debounceKey;
    long periodMs = debouncePeriod.toMillis();

    DebouncerOptions options =
        new DebouncerOptions(
            invocation.workflowName(),
            invocation.className(),
            invocation.instanceName(),
            queueName,
            debounceTimeout == null ? null : debounceTimeout.toMillis());
    DebouncerContextOptions ctx =
        new DebouncerContextOptions(userWorkflowId, null, null, null, null);
    DebouncerMessage initial = new DebouncerMessage(messageId, invocation.args(), periodMs);

    while (true) {
      try {
        var startOpts =
            new StartWorkflowOptions()
                .withQueue(Constants.DBOS_INTERNAL_QUEUE)
                .withDeduplicationId(deduplicationId);
        dbos.startWorkflow(
            () -> debouncerProxy.debouncerWorkflow(options, ctx, initial), startOpts);
        // Successfully enqueued a fresh debouncer for this key.
        return dbos.retrieveWorkflow(userWorkflowId);
      } catch (DBOSQueueDuplicatedException dup) {
        // A debouncer for this key is already running. Forward the latest args to it.
        String existingDebouncerId = lookupExistingDebouncerId(deduplicationId);
        if (existingDebouncerId == null) {
          // The existing debouncer finished between the enqueue attempt and now. Retry from
          // scratch — the next enqueue should succeed.
          logger.debug(
              "Debouncer for dedupId {} not found after conflict; retrying", deduplicationId);
          continue;
        }
        DebouncerMessage msg = new DebouncerMessage(messageId, invocation.args(), periodMs);
        dbos.send(existingDebouncerId, msg, Constants.DEBOUNCER_TOPIC);

        // Wait for the debouncer to acknowledge receipt. If the debouncer exited before
        // processing this message, no ack arrives — start over.
        var ack = dbos.getEvent(existingDebouncerId, messageId, ACK_TIMEOUT);
        if (ack.isEmpty()) {
          logger.debug(
              "Debouncer {} did not ack message {}; retrying", existingDebouncerId, messageId);
          continue;
        }
        // The existing debouncer absorbed our call. Read the pre-assigned user workflow id
        // from its persisted inputs and return a handle to it.
        var status = dbos.getWorkflowStatus(existingDebouncerId).orElse(null);
        if (status == null || status.input() == null) {
          logger.debug("Debouncer {} status unavailable; retrying", existingDebouncerId);
          continue;
        }
        Object[] dedupInputs = status.input();
        if (dedupInputs.length < 2 || !(dedupInputs[1] instanceof DebouncerContextOptions dco)) {
          throw new IllegalStateException(
              "Unexpected debouncer workflow inputs for " + existingDebouncerId);
        }
        return dbos.retrieveWorkflow(dco.userWorkflowId());
      }
    }
  }

  private @Nullable String lookupExistingDebouncerId(String deduplicationId) {
    var input =
        new ListWorkflowsInput()
            .withQueueName(Constants.DBOS_INTERNAL_QUEUE)
            .withQueuesOnly(true)
            .withWorkflowName(Constants.DEBOUNCER_WORKFLOW_NAME)
            .withLoadInput(false);
    for (var s : dbos.listWorkflows(input)) {
      if (deduplicationId.equals(s.deduplicationId())) {
        return s.workflowId();
      }
    }
    return null;
  }
}
