package dev.dbos.transact.context;

import dev.dbos.transact.DBOS;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOSContext {

  private static final Logger logger = LoggerFactory.getLogger(DBOSContext.class);

  public record StepStatus(int stepId, int currentAttempt, int maxAttempts) {}

  // assigned context options
  String nextWorkflowId;
  Duration timeout;
  String authenticatedUser;
  List<String> authenticatedRoles;
  String assumedRole;

  // current workflow fields
  private final DBOS dbos;
  private final String workflowId;
  private int functionId;
  private final String parentWorkflowId;
  private final int parentFunctionId;
  private StepStatus stepStatus;

  public DBOSContext() {
    dbos = null;
    workflowId = null;
    functionId = -1;
    parentWorkflowId = null;
    parentFunctionId = -1;
    stepStatus = null;
  }

  public DBOSContext(DBOS dbos, String workflowId) {
    this.dbos = dbos;
    this.workflowId = workflowId;
    this.functionId = 0;
    this.parentWorkflowId = null;
    this.parentFunctionId = -1;
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

  public long getWorkflowTimeoutMs() {
    return timeout == null ? 0L : timeout.toMillis();
  }

  public boolean hasParent() {
    return parentWorkflowId != null;
  }

  public String getParentWorkflowId() {
    return parentWorkflowId;
  }

  public int getParentFunctionId() {
    return parentFunctionId;
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
