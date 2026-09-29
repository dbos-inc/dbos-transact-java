package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSWorkflowFunctionNotFoundException;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.internal.DebugTriggers;
import dev.dbos.transact.workflow.WorkflowState;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Built-in workflows registered by DBOS itself. Currently holds the debouncer service workflow.
 *
 * <p>Not part of the public API.
 */
public class InternalWorkflows {

  private static final Logger logger = LoggerFactory.getLogger(InternalWorkflows.class);

  private final DBOS dbos;
  private final Supplier<DBOSExecutor> executorSupplier;

  public InternalWorkflows(DBOS dbos, Supplier<DBOSExecutor> executorSupplier) {
    this.dbos = dbos;
    this.executorSupplier = executorSupplier;
  }

  /**
   * Returns the {@link Method} reference for {@link #debouncerWorkflow}, used by DBOS at startup to
   * register the workflow without relying on reflection over {@code @Workflow} annotations.
   */
  public static Method debouncerWorkflowMethod() {
    try {
      return InternalWorkflows.class.getDeclaredMethod(
          "debouncerWorkflow",
          DebouncerOptions.class,
          DebouncerContextOptions.class,
          DebouncerMessage.class);
    } catch (NoSuchMethodException e) {
      throw new IllegalStateException("debouncerWorkflow method missing", e);
    }
  }

  /**
   * Takes over from a debouncer service workflow that stopped answering: cancels it, which frees
   * its debounce key, and returns the user workflow id it pre-assigned, so the caller can create
   * that workflow itself and the handles earlier callers were given resolve to the workflow that
   * really runs.
   *
   * <p>A service workflow goes silent for good once the last node of the SDK version that enqueued
   * it drains: the computed application version hashes the SDK version, so no remaining node ever
   * dequeues or recovers it, and it holds its key forever.
   *
   * <p>Returns null, cancelling nothing, when the service workflow is no longer waiting or its user
   * workflow already exists -- it was only slow, and has committed to run. Returns null after
   * cancelling when the inputs do not name a user workflow; the caller then creates its own. A live
   * service workflow can still start its user workflow between this cancel and the caller's create;
   * the caller checks for that afterwards.
   */
  public static @Nullable String takeOverStrandedDebouncer(
      SystemDatabase systemDatabase, String serviceWorkflowId) {
    var status = systemDatabase.getWorkflowStatus(serviceWorkflowId);
    if (status == null
        || !(status.status() == WorkflowState.ENQUEUED
            || status.status() == WorkflowState.PENDING)) {
      return null;
    }
    var childId = preassignedChildId(status.input());
    if (childId != null && systemDatabase.getWorkflowStatus(childId) != null) {
      return null;
    }
    logger.warn(
        "Cancelling debouncer service workflow {}, which stopped acknowledging calls; its user"
            + " workflow {} is created by the caller instead",
        serviceWorkflowId,
        childId);
    systemDatabase.cancelWorkflows(List.of(serviceWorkflowId), false);
    try {
      DebugTriggers.debugTriggerPoint(DebugTriggers.DEBUG_TRIGGER_DEBOUNCE_TAKEOVER);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return childId;
  }

  /**
   * The user workflow id a service workflow's inputs pre-assign, from its {@link
   * DebouncerContextOptions}; a serializer that drops Java types hands that back as a map.
   */
  private static @Nullable String preassignedChildId(Object @Nullable [] input) {
    if (input == null || input.length < 2) {
      return null;
    }
    if (input[1] instanceof DebouncerContextOptions ctx) {
      return ctx.userWorkflowId();
    }
    if (input[1] instanceof Map<?, ?> map && map.get("userWorkflowId") instanceof String id) {
      return id;
    }
    return null;
  }

  public void debouncerWorkflow(
      DebouncerOptions options, DebouncerContextOptions ctx, DebouncerMessage initial) {

    // Publish the pre-assigned user workflow id as an event so callers on the deduplication path
    // can retrieve it via getEvent without having to parse workflow inputs.
    dbos.setEvent(Constants.DEBOUNCER_CHILD_ID_KEY, ctx.userWorkflowId());

    // Record the absolute deadline once as a durable step. On recovery this returns the same
    // value so the loop's exit condition is replay-stable across crashes.
    long deadlineEpochMs =
        dbos.runStep(
            () ->
                options.debounceTimeout() == null
                    ? Long.MAX_VALUE
                    : Instant.now().plus(options.debounceTimeout()).toEpochMilli(),
            "DBOS.debouncerComputeDeadline");

    Object[] latestArgs = initial.args();
    Duration debouncePeriod = initial.debouncePeriod();

    DBOSExecutor executor = executorSupplier.get();
    if (executor == null) {
      throw new IllegalStateException("DBOS has not been launched. debounceWorkflow cannot run.");
    }
    while (true) {
      long nowEpochMs = dbos.runStep(() -> Instant.now().toEpochMilli(), "DBOS.debouncerNow");
      Duration remaining = Duration.ofMillis(deadlineEpochMs - nowEpochMs);
      if (remaining.compareTo(Duration.ZERO) <= 0) {
        break;
      }
      Duration waitDuration = remaining.compareTo(debouncePeriod) < 0 ? remaining : debouncePeriod;

      Optional<DebouncerMessage> msg = dbos.recv(Constants.DEBOUNCER_TOPIC, waitDuration);
      if (msg.isEmpty()) {
        break;
      }
      DebouncerMessage next = msg.get();
      latestArgs = next.args();
      debouncePeriod = next.debouncePeriod();
      // Acknowledge receipt so the sender knows the message was consumed by this loop iteration.
      dbos.setEvent(next.messageId(), next.messageId());
    }

    Optional<RegisteredWorkflow> optWorkflow =
        executor.getRegisteredWorkflow(
            options.workflowName(), options.className(), options.instanceName());
    if (optWorkflow.isEmpty()) {
      // The user workflow is not registered in this process (e.g. it was renamed/removed, or we
      // are recovering on a build that no longer declares it). We can never start it, so record
      // a terminal ERROR for the pre-assigned user workflow id. Otherwise any handle returned to
      // the caller would poll getResult() forever, since the status row would never appear.
      var notFound =
          new DBOSWorkflowFunctionNotFoundException(ctx.userWorkflowId(), options.workflowName());
      logger.error(
          "Debouncer cannot find registered user workflow {} (id={}); recording ERROR",
          options.workflowName(),
          ctx.userWorkflowId(),
          notFound);
      executor.recordErrorForUnstartedWorkflow(
          ctx.userWorkflowId(),
          options.workflowName(),
          options.className(),
          options.instanceName(),
          latestArgs,
          notFound);
      return;
    }
    var workflow = optWorkflow.get();

    // priority and deduplicationId are only valid for queued workflows; the executor
    // throws IllegalArgumentException if they are set without a queue name.
    boolean hasQueue = options.queueName() != null;
    // Versions before 1.1 accepted a negative priority, and a debouncer they enqueued may be
    // recovered here. The options now refuse one, so clamp it to the default rather than failing
    // this workflow and leaving the caller's handle polling for a user workflow that never starts.
    Integer priority = hasQueue ? options.priority() : null;
    if (priority != null && priority < 0) {
      logger.warn(
          "Debouncer clamping negative priority {} to 0 for user workflow {} (id={})",
          priority,
          options.workflowName(),
          ctx.userWorkflowId());
      priority = 0;
    }
    var startOpts =
        new StartWorkflowOptions()
            .withWorkflowId(ctx.userWorkflowId())
            .withQueue(options.queueName())
            .withDeduplicationId(hasQueue ? options.deduplicationId() : null)
            .withPriority(priority)
            .withAppVersion(options.appVersion())
            // Replay the attributes captured at debounce time.
            .withAttributes(ctx.workflowAttributes());
    if (ctx.workflowTimeout() != null) {
      startOpts = startOpts.withTimeout(ctx.workflowTimeout());
    }

    logger.debug(
        "Debouncer starting user workflow {} (id={})",
        options.workflowName(),
        ctx.userWorkflowId());
    executor.startRegisteredWorkflow(workflow, latestArgs, startOpts);
  }
}
