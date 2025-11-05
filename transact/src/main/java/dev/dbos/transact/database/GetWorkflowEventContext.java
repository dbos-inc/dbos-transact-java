package dev.dbos.transact.database;

public class GetWorkflowEventContext {

  private String workflowId;
  private int functionId;
  private int timeoutFunctionId;

  public GetWorkflowEventContext(String workflowId, int functionId, int timeoutFunctionId) {
    this.workflowId = workflowId;
    this.functionId = functionId;
    this.timeoutFunctionId = timeoutFunctionId;
  }

  public String getWorkflowId() {
    return workflowId;
  }

  public int getFunctionId() {
    return functionId;
  }

  public int getTimeoutFunctionId() {
    return timeoutFunctionId;
  }
}
