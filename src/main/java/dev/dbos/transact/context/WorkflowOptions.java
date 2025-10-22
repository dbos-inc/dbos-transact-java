package dev.dbos.transact.context;

import dev.dbos.transact.workflow.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public record WorkflowOptions(String workflowId, Timeout timeout) {

  public WorkflowOptions {
    if (timeout != null && timeout instanceof Timeout.Explicit explicit) {
      if (explicit.value().isNegative() || explicit.value().isZero()) {
        throw new IllegalArgumentException("timeout must be positive");
      }
    }
  }

  public WorkflowOptions() {
    this(null, null);
  }

  public WorkflowOptions(String workflowId) {
    this(workflowId, null);
  }

  public WorkflowOptions withWorkflowId(String workflowId) {
    return new WorkflowOptions(workflowId, this.timeout);
  }

  public WorkflowOptions withTimeout(Timeout timeout) {
    return new WorkflowOptions(this.workflowId, timeout);
  }

  public WorkflowOptions withTimeout(Duration timeout) {
    return new WorkflowOptions(this.workflowId, Timeout.of(timeout));
  }

  public WorkflowOptions withTimeout(long value, TimeUnit unit) {
    return withTimeout(Duration.ofNanos(unit.toNanos(value)));
  }

  public WorkflowOptions withNoTimeout() {
    return new WorkflowOptions(this.workflowId, Timeout.none());
  }

  @Override
  public String workflowId() {
    return workflowId != null && workflowId.isEmpty() ? null : workflowId;
  }

  public Guard setContext() {
    var ctx = DBOSContextHolder.get();
    var guard = new Guard(ctx);

    if (workflowId != null) {
      ctx.nextWorkflowId = workflowId;
    }
    if (timeout != null) {
      ctx.nextTimeout = timeout;
    }

    return guard;
  }

  public static class Guard implements AutoCloseable {

    private final DBOSContext ctx;
    private final String nextWorkflowId;
    private final Timeout timeout;

    public Guard(DBOSContext ctx) {
      this.ctx = ctx;
      this.nextWorkflowId = ctx.nextWorkflowId;
      this.timeout = ctx.nextTimeout;
    }

    @Override
    public void close() {
      ctx.nextWorkflowId = nextWorkflowId;
      ctx.nextTimeout = timeout;
    }
  }
}
