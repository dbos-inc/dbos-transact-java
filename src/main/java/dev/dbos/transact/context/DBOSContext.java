package dev.dbos.transact.context;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.StartWorkflowOptions;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOSContext {

  private static final Logger logger = LoggerFactory.getLogger(DBOSContext.class);

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
  private final DBOS dbos;
  private final String workflowId;
  private int functionId;
  private Integer stepFunctionId;
  private final WorkflowInfo parent;
  private final Duration timeout;
  private final Instant deadline;

  // private StepStatus stepStatus;

  public DBOSContext() {
    dbos = null;
    workflowId = null;
    functionId = -1;
    parent = null;
    timeout = null;
    deadline = null;
  }

  public DBOSContext(
      DBOS dbos, String workflowId, WorkflowInfo parent, Duration timeout, Instant deadline) {
    this.dbos = dbos;
    this.workflowId = workflowId;
    this.functionId = 0;
    this.parent = parent;
    this.timeout = timeout;
    this.deadline = deadline;
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

  public record StartOptionsResult(StartWorkflowOptions options, String workflowId) {}

  public StartOptionsResult setStartOptions(StartWorkflowOptions options) {
    var result = new StartOptionsResult(startOptions, startedWorkflowId);
    startOptions = options;
    startedWorkflowId = null;
    return result;
  }

  public void validateStartedWorkflow() {
    if (startOptions != null && startedWorkflowId != null) {
      throw new IllegalCallerException(
          "attempting to call multiple workflows from a start workflow lambda");
    }
  }

  public void setStartedWorkflowId(String workflowId) {
    if (startedWorkflowId != null) {
      throw new IllegalStateException("more than one workflow called from start workflow lambda");
    }
    startedWorkflowId = Objects.requireNonNull(workflowId);
  }

  public String getStartedWorkflowId() {
    return startedWorkflowId;
  }

  public void resetStartOptions(StartOptionsResult result) {
    startOptions = result.options;
    startedWorkflowId = result.workflowId;
  }

  public static Optional<String> workflowId() {
    var ctx = DBOSContextHolder.get();
    return ctx == null ? Optional.empty() : Optional.ofNullable(ctx.workflowId);
  }

  public static Optional<DBOS> dbosInstance() {
    var ctx = DBOSContextHolder.get();
    return ctx == null ? Optional.empty() : Optional.ofNullable(ctx.dbos);
  }

  public static boolean inWorkflow() {
    var ctx = DBOSContextHolder.get();
    return ctx == null ? false : ctx.isInWorkflow();
  }
}
