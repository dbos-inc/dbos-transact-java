package dev.dbos.transact.context;

import dev.dbos.transact.workflow.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * The WorkflowOptions class is used to specify options for DBOS workflow functions that are invoked
 * synchronously. For example, the following construct will run a workflow under id `wfId`, and
 * restore the context when complete: try (var _i = new WorkflowOptions(wfId).setContext()) { ...
 * function called here will get id `wfId` ... }
 *
 * @param workflowId The ID to be assigned to the next workflow in the DBOS context
 * @param timeout The timeout to be assigned to the next workflow in the DBOS context
 * @param deadline The deadline to be assigned to the next workflow in the DBOS context
 */
public record WorkflowOptions(String workflowId, Timeout timeout, Instant deadline) {

  public WorkflowOptions {
    if (timeout != null && timeout instanceof Timeout.Explicit explicit) {
      if (explicit.value().isNegative() || explicit.value().isZero()) {
        throw new IllegalArgumentException("timeout must be a positive non-zero duration");
      }
      if (deadline != null && timeout instanceof Timeout.Explicit) {
        throw new IllegalArgumentException(
            "WorkflowOptions may not specify both `timeout` and `deadline`");
      }
    }
  }

  /** Create a WorkflowOptions with no ID and no timout */
  public WorkflowOptions() {
    this(null, null, null);
  }

  /** Create a WorkflowOptions with a specified workflow ID and no timout */
  public WorkflowOptions(String workflowId) {
    this(workflowId, null, null);
  }

  /** Create a WorkflowOptions like this one, but with the workflowId set */
  public WorkflowOptions withWorkflowId(String workflowId) {
    return new WorkflowOptions(workflowId, this.timeout, this.deadline);
  }

  /**
   * Create a WorkflowOptions like this one, but with the timeout set
   *
   * @param timeout timeout to use, expressed as a `dev.dbos.transact.workflow.Timeout`
   */
  public WorkflowOptions withTimeout(Timeout timeout) {
    return new WorkflowOptions(this.workflowId, timeout, this.deadline);
  }

  /**
   * Create a WorkflowOptions like this one, but with the timeout set
   *
   * @param timeout timeout to use, expressed as a `java.util.Duration`
   */
  public WorkflowOptions withTimeout(Duration timeout) {
    return new WorkflowOptions(this.workflowId, Timeout.of(timeout), this.deadline);
  }

  /**
   * Create a WorkflowOptions like this one, but with the timeout set
   *
   * @param value timeout value to use, expressed as a value (see `unit`)
   * @param unit units to use for interpreting timeout `value`
   */
  public WorkflowOptions withTimeout(long value, TimeUnit unit) {
    return withTimeout(Duration.ofNanos(unit.toNanos(value)));
  }

  /**
   * Create a WorkflowOptions like this one, but with the deadline set
   *
   * @param deadline deadline to use, expressed as a `java.util.Instant`
   */
  public WorkflowOptions withDeadline(Instant deadline) {
    return new WorkflowOptions(this.workflowId, this.timeout, deadline);
  }

  /** Create a workflow options like this one, but without a timeout */
  public WorkflowOptions withNoTimeout() {
    return new WorkflowOptions(this.workflowId, Timeout.none(), this.deadline);
  }

  /**
   * @return The workflow ID that will be used
   */
  @Override
  public String workflowId() {
    return workflowId != null && workflowId.isEmpty() ? null : workflowId;
  }

  /**
   * Set the workflow options contained in this `WorkflowOptions` into the current DBOS context.
   * Should be called as an AutoCloseable so that the context is restored at the end of the block.
   * try (var _i = new WorkflowOptions(...).setContext()) { ... }
   */
  public Guard setContext() {
    var ctx = DBOSContextHolder.get();
    var guard = new Guard(ctx);

    if (workflowId != null) {
      ctx.nextWorkflowId = workflowId;
    }
    if (timeout != null) {
      ctx.nextTimeout = timeout;
    }
    if (deadline != null) {
      ctx.nextDeadline = deadline;
    }

    return guard;
  }

  public static class Guard implements AutoCloseable {

    private final DBOSContext ctx;
    private final String nextWorkflowId;
    private final Timeout timeout;
    private final Instant deadline;

    private Guard(DBOSContext ctx) {
      this.ctx = ctx;
      this.nextWorkflowId = ctx.nextWorkflowId;
      this.timeout = ctx.nextTimeout;
      this.deadline = ctx.nextDeadline;
    }

    @Override
    public void close() {
      ctx.nextWorkflowId = nextWorkflowId;
      ctx.nextTimeout = timeout;
      ctx.nextDeadline = deadline;
    }
  }
}
