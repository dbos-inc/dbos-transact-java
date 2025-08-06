package dev.dbos.transact.context;

import dev.dbos.transact.Constants;
import dev.dbos.transact.queue.Queue;

public class DBOSContext {

  private String executorId = Constants.DEFAULT_EXECUTORID;

  private String parentWorkflowId;
  private int parentFunctionId;
  private volatile String workflowId;
  private volatile int functionId;

  private int currStepFunctionId;

  private String authenticatedUser;
  private String authenticateRole;
  private String assumedRole;

  private String appVersion;

  // workflow timeouts
  private Queue queue;
  private long workflowTimeoutMs;
  private long workflowDeadlineEpochMs;

  // Queues
  private String deduplicationId;
  private int priority;

  //
  private volatile boolean inWorkflow = false;

  private String stepId;

  private boolean async;

  public DBOSContext() {}

  public DBOSContext(String workflowId, int functionId) {
    this.workflowId = workflowId;
    this.functionId = functionId;
    this.inWorkflow = false;
  }

  public DBOSContext(
      String workflowId,
      int functionId,
      String parentWorkflowId,
      int parentFunctionId,
      boolean inWorkflow,
      boolean async,
      Queue q,
      long timeout) {
    this.workflowId = workflowId;
    this.functionId = functionId;
    this.inWorkflow = inWorkflow;
    this.parentWorkflowId = parentWorkflowId;
    this.parentFunctionId = parentFunctionId;
    this.async = async;
    this.queue = q;
    this.workflowTimeoutMs = timeout;
  }

  public DBOSContext(DBOSOptions options, int functionId) {
    this.workflowId = options.getWorkflowId();
    this.functionId = functionId;
    this.inWorkflow = false;
    this.async = options.isAsync();
    this.queue = options.getQueue();
    this.workflowTimeoutMs = options.getTimeout() * 1000;
  }

  private DBOSContext(
      String childWorkflowId,
      String parentWorkflowId,
      int parentFunctionId,
      boolean async,
      Queue queue,
      long workflowTimeout) {
    this.workflowId = childWorkflowId;
    this.parentWorkflowId = parentWorkflowId;
    this.parentFunctionId = parentFunctionId;
    this.inWorkflow = true;
    this.async = async;
    this.queue = queue;
    this.workflowTimeoutMs = workflowTimeout;
  }

  private DBOSContext(
      DBOSOptions options, String parentWorkflowId, int parentFunctionId, long parentTimeout) {
    this.workflowId = options.getWorkflowId();
    this.parentWorkflowId = parentWorkflowId;
    this.parentFunctionId = parentFunctionId;
    this.inWorkflow = true;
    this.async = options.isAsync();
    this.queue = options.getQueue();
    if (options.getTimeout() > 0) {
      this.workflowTimeoutMs = options.getTimeout() * 1000;
    } else {
      this.workflowTimeoutMs = parentTimeout;
    }
  }

  public String getWorkflowId() {
    return workflowId;
  }

  public String getParentWorkflowId() {
    return parentWorkflowId;
  }

  public void setWorkflowId(String workflowId) {
    this.workflowId = workflowId;
  }

  public String getAuthenticatedUser() {
    return authenticatedUser;
  }

  public void setAuthenticatedUser(String user) {
    this.authenticatedUser = user;
  }

  public int getFunctionId() {
    return functionId;
  }

  public int getParentFunctionId() {
    return parentFunctionId;
  }

  public int getAndIncrementFunctionId() {
    return functionId++;
  }

  public String getStepId() {
    return stepId;
  }

  public void setStepId(String stepId) {
    this.stepId = stepId;
  }

  public DBOSContext copy() {
    return new DBOSContext(
        workflowId,
        functionId,
        parentWorkflowId,
        parentFunctionId,
        inWorkflow,
        async,
        queue,
        workflowTimeoutMs);
  }

  public DBOSContext createChild(String childWorkflowId) {
    return new DBOSContext(
        childWorkflowId,
        workflowId,
        this.getAndIncrementFunctionId(),
        this.async,
        this.getQueue(),
        this.workflowTimeoutMs);
  }

  public DBOSContext createChild(DBOSOptions options) {
    return new DBOSContext(
        options, workflowId, this.getAndIncrementFunctionId(), this.workflowTimeoutMs);
  }

  public boolean hasParent() {
    return this.parentWorkflowId != null;
  }

  public boolean isInWorkflow() {
    return inWorkflow;
  }

  public void setInWorkflow(boolean in) {
    this.inWorkflow = true;
  }

  public boolean isAsync() {
    return async;
  }

  public Queue getQueue() {
    return queue;
  }

  public long getWorkflowTimeoutMs() {
    return workflowTimeoutMs;
  }

  public DBOSContext copyWithAsync() {
    return new DBOSContext(
        workflowId,
        functionId,
        parentWorkflowId,
        parentFunctionId,
        inWorkflow,
        true,
        queue,
        workflowTimeoutMs);
  }

  public DBOSContext copyWithQueue(Queue q) {
    return new DBOSContext(
        workflowId,
        functionId,
        parentWorkflowId,
        parentFunctionId,
        inWorkflow,
        async,
        q,
        workflowTimeoutMs);
  }

  public DBOSContext copyWithWorkflowId(String id) {
    return new DBOSContext(
        id,
        functionId,
        parentWorkflowId,
        parentFunctionId,
        inWorkflow,
        async,
        queue,
        workflowTimeoutMs);
  }
}
