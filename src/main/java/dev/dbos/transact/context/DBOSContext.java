package dev.dbos.transact.context;

import dev.dbos.transact.DBOS;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOSContext {

  private static final Logger logger = LoggerFactory.getLogger(DBOSContext.class);

  public record StepStatus(int stepId, int currentAttempt, int maxAttempts) {}

  // assigned context options
  String nextWorkflowId;
  Duration timeout;

  // TODO: auth support
  // String authenticatedUser;
  // List<String> authenticatedRoles;
  // String assumedRole;

  // current workflow fields
  private final DBOS dbos;
  private final String workflowId;
  private int functionId;
  private final WorkflowInfo parent;
  private final Instant deadline;

  // private StepStatus stepStatus;

  public DBOSContext() {
    dbos = null;
    workflowId = null;
    functionId = -1;
    parent = null;
    deadline = null;
    // stepStatus = null;
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

  public String getWorkflowId() {
    return workflowId;
  }

  public int getAndIncrementFunctionId() {
    return functionId++;
  }

  public WorkflowInfo getParent() {
    return parent;
  }

  public Instant getDeadline(Duration timeout) {
    return timeout != null
        ? Instant.ofEpochMilli(System.currentTimeMillis() + timeout.toMillis())
        : deadline;
  }

  // public boolean hasParent() {
  //   return parent != null;
  // }

  // public String getParentWorkflowId() {
  //   return parent != null ? parent.workflowId() : null;
  // }

  // public int getParentFunctionId() {
  //   return parent != null ? parent.functionId() : -1;
  // }

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

  public Duration getTimeout() {
    return this.timeout;
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
