package dev.dbos.transact.context;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public record WorkflowOptions(String workflowId, Duration timeout) {

  public WorkflowOptions() {
    this(null, null);
  }

  public WorkflowOptions(String workflowId) {
    this(workflowId, null);
  }

  public WorkflowOptions(Duration timeout) {
    this(null, timeout);
  }

  public WorkflowOptions(long value, TimeUnit unit) {
    this(Duration.ofNanos(unit.toNanos(value)));
  }

  public WorkflowOptions withWorkflowId(String workflowId) {
    return new WorkflowOptions(workflowId, this.timeout);
  }

  public WorkflowOptions withTimeout(Duration timeout) {
    return new WorkflowOptions(this.workflowId, timeout);
  }

  public WorkflowOptions withTimeout(long value, TimeUnit unit) {
    var timeout = Duration.ofNanos(unit.toNanos(value));
    return new WorkflowOptions(this.workflowId, timeout);
  }

  public Guard setContext() {
    var ctx = DBOSContextHolder.get();
    var guard = new Guard(ctx);
    ctx.nextWorkflowId = Objects.requireNonNullElse(workflowId, ctx.nextWorkflowId);
    ctx.timeout = Objects.requireNonNullElse(timeout, ctx.timeout);
    return guard;
  }

  public static class Guard implements AutoCloseable {

    private final DBOSContext ctx;
    private final String nextWorkflowId;
    private final Duration timeout;

    public Guard(DBOSContext ctx) {
      this.ctx = ctx;
      this.nextWorkflowId = ctx.nextWorkflowId;
      this.timeout = ctx.timeout;
    }

    @Override
    public void close() {
      ctx.nextWorkflowId = nextWorkflowId;
      ctx.timeout = timeout;
    }
  }
}
