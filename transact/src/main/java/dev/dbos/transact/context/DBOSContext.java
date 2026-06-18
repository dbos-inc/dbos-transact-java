package dev.dbos.transact.context;

import dev.dbos.transact.StartWorkflowOptions;
import dev.dbos.transact.workflow.Field;
import dev.dbos.transact.workflow.SerializationStrategy;
import dev.dbos.transact.workflow.Timeout;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class DBOSContext {

  // assigned context options
  String nextWorkflowId;
  Timeout nextTimeout;
  Instant nextDeadline;
  Field<String> nextAuthenticatedUser = Field.absent();
  Field<String> nextAssumedRole = Field.absent();
  Field<List<String>> nextAuthenticatedRoles = Field.absent();
  // Custom attributes to attach to the next workflow started or enqueued in this context.
  // Not inherited by child workflows: workflows run in a fresh context that does not carry it.
  Map<String, Object> nextAttributes;

  // current workflow fields
  private final String workflowId;
  private int functionId;
  private Integer stepFunctionId;
  private final WorkflowInfo parent;
  private final Duration timeout;
  private final Instant deadline;
  private final String authenticatedUser;
  private final String assumedRole;
  private final List<String> authenticatedRoles;
  private SerializationStrategy serialization;

  // private StepStatus stepStatus;

  public DBOSContext() {
    workflowId = null;
    functionId = -1;
    parent = null;
    timeout = null;
    deadline = null;
    authenticatedUser = null;
    assumedRole = null;
    authenticatedRoles = null;
    serialization = SerializationStrategy.DEFAULT;
  }

  public DBOSContext(
      String workflowId,
      WorkflowInfo parent,
      Duration timeout,
      Instant deadline,
      String authenticatedUser,
      String assumedRole,
      List<String> authenticatedRoles,
      SerializationStrategy serialization) {
    this.workflowId = workflowId;
    this.functionId = 0;
    this.parent = parent;
    this.timeout = timeout;
    this.deadline = deadline;
    this.authenticatedUser = authenticatedUser;
    this.assumedRole = assumedRole;
    this.authenticatedRoles = authenticatedRoles;
    this.serialization = serialization;
  }

  public DBOSContext(
      DBOSContext other,
      StartWorkflowOptions options,
      Integer functionId,
      CompletableFuture<String> future) {
    this.nextWorkflowId = other.nextWorkflowId;
    this.nextTimeout = other.nextTimeout;
    this.nextDeadline = other.nextDeadline;
    this.nextAuthenticatedUser = other.nextAuthenticatedUser;
    this.nextAssumedRole = other.nextAssumedRole;
    this.nextAuthenticatedRoles = other.nextAuthenticatedRoles;
    this.nextAttributes = other.nextAttributes;
    this.workflowId = other.workflowId;
    this.functionId = functionId == null ? other.functionId : functionId;
    this.stepFunctionId = other.stepFunctionId;
    this.parent = other.parent;
    this.timeout = other.timeout;
    this.deadline = other.deadline;
    this.authenticatedUser = other.authenticatedUser;
    this.assumedRole = other.assumedRole;
    this.authenticatedRoles = other.authenticatedRoles;
    this.serialization = other.serialization;
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

  public SerializationStrategy getSerialization() {
    return serialization;
  }

  public static String workflowId() {
    return DBOSContextHolder.get().workflowId;
  }

  public static Integer stepId() {
    return DBOSContextHolder.get().stepFunctionId;
  }

  public static boolean inWorkflow() {
    return DBOSContextHolder.get().isInWorkflow();
  }

  public static boolean inStep() {
    return DBOSContextHolder.get().isInStep();
  }

  public static SerializationStrategy serializationStrategy() {
    return DBOSContextHolder.get().getSerialization();
  }

  public static String authenticatedUser() {
    return DBOSContextHolder.get().getAuthenticatedUser();
  }

  public static String assumedRole() {
    return DBOSContextHolder.get().getAssumedRole();
  }

  public static List<String> authenticatedRoles() {
    return DBOSContextHolder.get().getAuthenticatedRoles();
  }

  public record TimeoutAndDeadline(Duration timeout, Instant deadline) {}

  public TimeoutAndDeadline resolveTimeoutAndDeadline() {
    return resolveTimeoutAndDeadline(null, null);
  }

  public TimeoutAndDeadline resolveTimeoutAndDeadline(Timeout nextTimeout, Instant nextDeadline) {
    if (nextTimeout == null) nextTimeout = this.nextTimeout;
    if (nextDeadline == null) nextDeadline = this.nextDeadline;
    Duration resolvedTimeout = this.timeout;
    Instant resolvedDeadline = this.deadline;
    if (nextDeadline != null) {
      resolvedDeadline = nextDeadline;
    } else if (nextTimeout instanceof Timeout.None) {
      resolvedTimeout = null;
      resolvedDeadline = null;
    } else if (nextTimeout instanceof Timeout.Explicit e) {
      resolvedTimeout = e.value();
      resolvedDeadline = Instant.ofEpochMilli(System.currentTimeMillis() + e.value().toMillis());
    }
    return new TimeoutAndDeadline(resolvedTimeout, resolvedDeadline);
  }

  public String getAuthenticatedUser() {
    return authenticatedUser;
  }

  public String getAssumedRole() {
    return assumedRole;
  }

  public List<String> getAuthenticatedRoles() {
    return authenticatedRoles;
  }

  public String resolveNextAuthenticatedUser() {
    return nextAuthenticatedUser instanceof Field.Present<String> p ? p.value() : authenticatedUser;
  }

  public String resolveNextAssumedRole() {
    return nextAssumedRole instanceof Field.Present<String> p ? p.value() : assumedRole;
  }

  public List<String> resolveNextAuthenticatedRoles() {
    return nextAuthenticatedRoles instanceof Field.Present<List<String>> p
        ? p.value()
        : authenticatedRoles;
  }

  public Map<String, Object> resolveNextAttributes() {
    return nextAttributes;
  }
}
