package dev.dbos.transact.workflow.internal;

import dev.dbos.transact.Constants;
import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.json.JsonUtility;
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

    // Publish the pre-assigned user workflow id as an event so callers waiting on the
    // deduplication path can retrieve it via getEvent without relying on Jackson deserializing
    // workflow inputs (records are final classes so @class type info is not emitted).
    dbos.setEvent(Constants.DEBOUNCER_CHILD_ID_KEY, ctx.userWorkflowId());

    // Record the absolute deadline once as a durable step. On recovery this returns the same
    // value so the loop's exit condition is replay-stable across crashes.
    long deadlineEpochMs =
        dbos.runStep(
            () -> {
              if (options.debounceTimeoutMs() == null) {
                return Long.MAX_VALUE;
              }
              return Instant.now().toEpochMilli() + options.debounceTimeoutMs();
            },
            "DBOS.debouncerComputeDeadline");

    Object[] latestArgs = initial.args();
    long debouncePeriodMs = initial.debouncePeriodMs();

    while (true) {
      long now = dbos.runStep(() -> Instant.now().toEpochMilli(), "DBOS.debouncerNow");
      if (now >= deadlineEpochMs) {
        break;
      }
      long timeUntilDeadlineMs = Math.max(deadlineEpochMs - now, 0);
      long waitMs = Math.min(debouncePeriodMs, timeUntilDeadlineMs);

      Optional<DebouncerMessage> msg =
          dbos.recv(Constants.DEBOUNCER_TOPIC, Duration.ofMillis(waitMs));
      if (msg.isEmpty()) {
        // Period elapsed with no new message — fire.
        break;
      }
      DebouncerMessage next = msg.get();
      latestArgs = next.args();
      debouncePeriodMs = next.debouncePeriodMs();
      // Acknowledge receipt so the sender knows the message was consumed by this loop iteration.
      dbos.setEvent(next.messageId(), next.messageId());
    }

    var optWorkflow =
        dbos.integration()
            .getRegisteredWorkflow(
                options.workflowName(),
                options.className(),
                options.instanceName() == null ? "" : options.instanceName());
    if (optWorkflow.isEmpty()) {
      throw new IllegalStateException(
          "Debouncer cannot find registered user workflow: "
              + options.workflowName()
              + " / "
              + options.className()
              + (options.instanceName() == null ? "" : " / " + options.instanceName()));
    }

    var startOpts =
        new StartWorkflowOptions()
            .withWorkflowId(ctx.userWorkflowId())
            .withQueue(options.queueName())
            .withDeduplicationId(ctx.deduplicationId())
            .withPriority(ctx.priority())
            .withAppVersion(ctx.appVersion());
    if (ctx.workflowTimeoutMs() != null) {
      startOpts = startOpts.withTimeout(Duration.ofMillis(ctx.workflowTimeoutMs()));
    }

    // Coerce args to the method's declared parameter types before invocation.
    // Jackson's type-info round-trip through send/recv (Object[]) can produce numeric
    // mismatches (e.g. long → Integer) that cause IllegalArgumentException at reflection
    // call-site. This mirrors the coercion already applied in executeWorkflowById.
    var workflow = optWorkflow.orElseThrow();
    try {
      latestArgs = JsonUtility.coerceArguments(latestArgs, workflow.workflowMethod());
    } catch (IllegalArgumentException e) {
      throw new IllegalStateException(
          "Debouncer argument coercion failed for workflow " + options.workflowName(), e);
    }

    logger.debug(
        "Debouncer starting user workflow {} (id={})",
        options.workflowName(),
        ctx.userWorkflowId());
    dbos.integration().startRegisteredWorkflow(workflow, latestArgs, startOpts);
  }
}
