package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.workflow.Workflow;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the debouncer service workflow. Holds a reference to the {@link DBOS} instance
 * to call durable primitives (recv, setEvent, runStep) and to start the user workflow when the
 * debounce period elapses.
 *
 * <p>Auto-registered by DBOS during construction.
 */
public class DebouncerServiceImpl implements DebouncerService {

  private static final Logger logger = LoggerFactory.getLogger(DebouncerServiceImpl.class);

  private final DBOS dbos;

  public DebouncerServiceImpl(DBOS dbos) {
    this.dbos = dbos;
  }

  @Override
  @Workflow(name = Constants.DEBOUNCER_WORKFLOW_NAME)
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
      Duration waitDuration =
          remaining.compareTo(debouncePeriod) < 0 ? remaining : debouncePeriod;

      Optional<DebouncerMessage> msg = dbos.recv(Constants.DEBOUNCER_TOPIC, waitDuration);
      if (msg.isEmpty()) {
        // Period elapsed with no new message — fire.
        break;
      }
      DebouncerMessage next = msg.get();
      latestArgs = next.args();
      debouncePeriod = next.debouncePeriod();
      // Acknowledge receipt so the sender knows the message was consumed by this loop iteration.
      dbos.setEvent(next.messageId(), next.messageId());
    }

    var workflow =
        dbos.integration()
            .getRegisteredWorkflow(
                options.workflowName(), options.className(), options.instanceName())
            .orElseThrow(
                () ->
                    new IllegalStateException(
                        "Debouncer cannot find registered user workflow: "
                            + options.workflowName()
                            + " / "
                            + options.className()
                            + (options.instanceName() == null
                                ? ""
                                : " / " + options.instanceName())));

    // priority and deduplicationId are only valid for queued workflows; the executor
    // throws IllegalArgumentException if they are set without a queue name.
    boolean hasQueue = options.queueName() != null;
    var startOpts =
        new StartWorkflowOptions()
            .withWorkflowId(ctx.userWorkflowId())
            .withQueue(options.queueName())
            .withDeduplicationId(hasQueue ? ctx.deduplicationId() : null)
            .withPriority(hasQueue ? ctx.priority() : null)
            .withAppVersion(ctx.appVersion());
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
