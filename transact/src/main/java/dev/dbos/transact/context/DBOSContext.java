package dev.dbos.transact.context;

import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.workflow.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;

public class DBOSContext {

  // assigned context options
  String nextWorkflowId;
  Timeout nextTimeout;
  Instant nextDeadline;

  // current workflow fields
  private final String workflowId;
  private int functionId;
  private Integer stepFunctionId;
  private final WorkflowInfo parent;
  private final Duration timeout;
  private final Instant deadline;

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
    this.nextDeadline = other.nextDeadline;
    this.workflowId = other.workflowId;
    this.functionId = functionId == null ? other.functionId : functionId;
    this.stepFunctionId = other.stepFunctionId;
    this.parent = other.parent;
    this.timeout = other.timeout;
    this.deadline = other.deadline;
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

  public int getCurrentFunctionId() {
    return functionId;
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
    if (nextWorkflowId != null) {
      var value = nextWorkflowId;
      this.nextWorkflowId = null;
      return value;
    }

    return workflowId;
  }

  public Timeout getNextTimeout() {
    return nextTimeout;
  }

  public Duration getTimeout() {
    return timeout;
  }

  public Instant getNextDeadline() {
    return nextDeadline;
  }

  public Instant getDeadline() {
    return deadline;
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
