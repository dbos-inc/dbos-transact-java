package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.exceptions.DBOSWorkflowFunctionNotFoundException;
import dev.dbos.transact.execution.RegisteredWorkflow;

import java.lang.reflect.Method;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

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

  public InternalWorkflows(DBOS dbos) {
    this.dbos = dbos;
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
        dbos.integration()
            .getRegisteredWorkflow(
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
      dbos.integration()
          .recordErrorForUnstartedWorkflow(
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
    var startOpts =
        new StartWorkflowOptions()
            .withWorkflowId(ctx.userWorkflowId())
            .withQueue(options.queueName())
            .withDeduplicationId(hasQueue ? options.deduplicationId() : null)
            .withPriority(hasQueue ? options.priority() : null)
            .withAppVersion(options.appVersion());
    if (ctx.workflowTimeout() != null) {
      startOpts = startOpts.withTimeout(ctx.workflowTimeout());
    }

    logger.debug(
        "Debouncer starting user workflow {} (id={})",
        options.workflowName(),
        ctx.userWorkflowId());
    dbos.integration().startRegisteredWorkflow(workflow, latestArgs, startOpts);
  }
}
