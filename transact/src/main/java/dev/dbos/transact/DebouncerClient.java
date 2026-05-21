package dev.dbos.transact;

import dev.dbos.transact.exceptions.DBOSQueueDuplicatedException;
import dev.dbos.transact.workflow.Queue;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.internal.DebouncerContextOptions;
import dev.dbos.transact.workflow.internal.DebouncerMessage;
import dev.dbos.transact.workflow.internal.DebouncerOptions;

import java.time.Duration;
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
  private static final Duration ACK_TIMEOUT = Duration.ofSeconds(1);

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

  DebouncerClient(@NonNull DBOSClient client, @NonNull String workflowName) {
    this(client, workflowName, null, null, null, null, null, null, null, null);
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
      @Nullable Duration workflowTimeout) {
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
        workflowTimeout);
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
        workflowTimeout);
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
        workflowTimeout);
  }

  /** See {@link #withQueue(String)}. */
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
        workflowTimeout);
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
        workflowTimeout);
  }

  /** Set the priority for the user workflow (only used when a queue is configured). */
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
        workflowTimeout);
  }

  /** Set a deduplication ID to be forwarded to the user workflow. */
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
        workflowTimeout);
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
        timeout);
  }

  /**
   * Debounce a workflow invocation.
   *
   * @param debounceKey key that groups concurrent calls; calls with the same key are coalesced
   * @param debouncePeriod inactivity window before the user workflow runs; each call resets it
   * @param args positional arguments to pass to the user workflow
   * @return handle pointing to the user workflow that will run with the latest arguments; on the
   *     deduplication path the handle ID is the child ID published by the running debouncer, not
   *     the locally generated UUID
   */
  public @NonNull WorkflowHandle<R, ?> debounce(
      @NonNull String debounceKey, @NonNull Duration debouncePeriod, Object... args) {

    Objects.requireNonNull(debounceKey, "debounceKey must not be null");
    Objects.requireNonNull(debouncePeriod, "debouncePeriod must not be null");
    if (debouncePeriod.isNegative() || debouncePeriod.isZero()) {
      throw new IllegalArgumentException("debouncePeriod must be a positive non-zero duration");
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
    DebouncerContextOptions ctx = new DebouncerContextOptions(userWorkflowId, workflowTimeout);
    DebouncerMessage initial = new DebouncerMessage(messageId, args, debouncePeriod);

    var enqueueOpts =
        new DBOSClient.EnqueueOptions(
                Constants.DEBOUNCER_WORKFLOW_NAME,
                Constants.DEBOUNCER_SERVICE_CLASS_NAME,
                Constants.DBOS_INTERNAL_QUEUE)
            .withDeduplicationId(deduplicationId);

    while (true) {
      try {
        client.enqueueWorkflow(enqueueOpts, new Object[] {debouncerOpts, ctx, initial});
        return client.retrieveWorkflow(userWorkflowId);
      } catch (DBOSQueueDuplicatedException dup) {
        // A debouncer for this key is already running — forward the latest args to it.
        String existingDebouncerId =
            client.findWorkflowIdByDeduplicationId(Constants.DBOS_INTERNAL_QUEUE, deduplicationId);
        if (existingDebouncerId == null) {
          logger.debug(
              "Debouncer for dedupId {} not found after conflict; retrying", deduplicationId);
          continue;
        }

        DebouncerMessage msg = new DebouncerMessage(messageId, args, debouncePeriod);
        client.send(existingDebouncerId, msg, Constants.DEBOUNCER_TOPIC, messageId);

        var ack = client.getEvent(existingDebouncerId, messageId, ACK_TIMEOUT);
        if (ack.isEmpty()) {
          logger.debug(
              "Debouncer {} did not ack message {}; retrying", existingDebouncerId, messageId);
          continue;
        }

        // DEBOUNCER_CHILD_ID_KEY is published as the debouncer's first action, before the
        // recv-loop. If the ack arrived the event should be available; retry if not to guard
        // against transient delays.
        var childIdOpt =
            client.getEvent(existingDebouncerId, Constants.DEBOUNCER_CHILD_ID_KEY, ACK_TIMEOUT);
        if (childIdOpt.isEmpty()) {
          logger.debug(
              "DEBOUNCER_CHILD_ID_KEY not yet available from {}; retrying", existingDebouncerId);
          continue;
        }
        return client.retrieveWorkflow((String) childIdOpt.get());
      }
    }
  }
}
