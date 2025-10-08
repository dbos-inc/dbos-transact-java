package dev.dbos.transact.context;

import dev.dbos.transact.StartWorkflowOptions;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;

public class DBOSContext {

  // assigned context options
  String nextWorkflowId;
  Duration nextTimeout;
  StartWorkflowOptions startOptions;
  String startedWorkflowId;

  // TODO: auth support
  // String authenticatedUser;
  // List<String> authenticatedRoles;
  // String assumedRole;

  // current workflow fields
  private final String workflowId;
  private int functionId;
  private Integer stepFunctionId;
  private final WorkflowInfo parent;
  private final Duration timeout;
  private final Instant deadline;
  private CompletableFuture<String> startWorkflowFuture;

  // private StepStatus stepStatus;

  public DBOSContext() {
    workflowId = null;
    functionId = -1;
    parent = null;
    timeout = null;
    deadline = null;
  }

  public DBOSContext(String workflowId, WorkflowInfo parent, Duration timeout, Instant deadline) {
    this.workflowId = workflowId;
    this.functionId = 0;
    this.parent = parent;
    this.timeout = timeout;
    this.deadline = deadline;
  }

  public DBOSContext(
      DBOSContext other,
      StartWorkflowOptions options,
      Integer functionId,
      CompletableFuture<String> future) {
    this.nextWorkflowId = other.nextWorkflowId;
    this.nextTimeout = other.nextTimeout;
    this.workflowId = other.workflowId;
    this.functionId = functionId == null ? other.functionId : functionId;
    this.stepFunctionId = other.stepFunctionId;
    this.parent = other.parent;
    this.timeout = other.timeout;
    this.deadline = other.deadline;

    if (other.startedWorkflowId != null) {
      throw new IllegalStateException("startedWorkflowId not null");
    }

    this.startOptions = options;
    this.startWorkflowFuture = future;
    this.startedWorkflowId = null;
  }

  public boolean isInWorkflow() {
    return workflowId != null;
  }

  public boolean isInStep() {
    return stepFunctionId != null;
  }

  public String getWorkflowId() {
    return workflowId;
  }

  public Integer getStepId() {
    return stepFunctionId;
  }

  public int getAndIncrementFunctionId() {
    return functionId++;
  }

  public void setStepFunctionId(int functionId) {
    stepFunctionId = functionId;
  }

  public void resetStepFunctionId() {
    stepFunctionId = null;
  }

  public WorkflowInfo getParent() {
    return parent;
  }

  public String getNextWorkflowId() {
    return getNextWorkflowId(null);
  }

  public String getNextWorkflowId(String workflowId) {
    if (startOptions != null && startOptions.workflowId() != null) {
      return startOptions.workflowId();
    }
    if (nextWorkflowId != null) {
      var value = nextWorkflowId;
      this.nextWorkflowId = null;
      return value;
    }

    return workflowId;
  }

  public Duration getNextTimeout() {
    if (startOptions != null && startOptions.timeout() != null) {
      return startOptions.timeout();
    }

    return nextTimeout;
  }

  public Duration getTimeout() {
    return timeout;
  }

  public Instant getDeadline() {
    return deadline;
  }

  public String getQueueName() {
    if (startOptions != null) {
      return startOptions.queueName();
    }
    return null;
  }

  public String getDeduplicationId() {
    if (startOptions != null) {
      return startOptions.deduplicationId();
    }
    return null;
  }

  public OptionalInt getPriority() {
    if (startOptions != null) {
      return startOptions.priority();
    }
    return OptionalInt.empty();
  }

  public void setStartOptions(StartWorkflowOptions options, CompletableFuture<String> future) {
    if (startedWorkflowId != null) {
      throw new IllegalStateException();
    }
    startOptions = options;
    startWorkflowFuture = future;
  }

  public boolean validateStartedWorkflow() {
    return startOptions == null || startedWorkflowId == null;
  }

  public void setStartedWorkflowId(String workflowId) {
    if (startOptions != null) {
      if (startedWorkflowId != null) {
        throw new IllegalStateException(
            String.format(
                "more than one workflow called from start workflow lambda: %s %s",
                workflowId, startedWorkflowId));
      }
      startedWorkflowId = Objects.requireNonNull(workflowId);
    }
  }

  public CompletableFuture<String> getStartWorkflowFuture() {
    return this.startWorkflowFuture;
  }

  public static String workflowId() {
    var ctx = DBOSContextHolder.get();
    return ctx == null ? null : ctx.workflowId;
  }

  public static Integer stepId() {
    var ctx = DBOSContextHolder.get();
    return ctx == null ? null : ctx.stepFunctionId;
  }

  public static boolean inWorkflow() {
    var ctx = DBOSContextHolder.get();
    return ctx == null ? false : ctx.isInWorkflow();
  }

  public static boolean inStep() {
    var ctx = DBOSContextHolder.get();
    return ctx == null ? false : ctx.isInStep();
  }
}
