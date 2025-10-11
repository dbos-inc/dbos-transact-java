package dev.dbos.transact.workflow.internal;

public class StepResult {
  private String workflowId;
  private int functionId;
  private String functionName;
  private String output;
  private String error;

  public StepResult() {}

  public StepResult(
      String workflowId, int functionId, String functionName, String output, String error) {
    this.workflowId = workflowId;
    this.functionId = functionId;
    this.functionName = functionName;
    this.output = output;
    this.error = error;
  }

  public String getWorkflowId() {
    return workflowId;
  }

  public int getFunctionId() {
    return functionId;
  }

  public String getFunctionName() {
    return functionName;
  }

  public String getOutput() {
    return output;
  }

  public String getError() {
    return error;
  }

  public void setWorkflowId(String workflowId) {
    this.workflowId = workflowId;
  }

  public void setFunctionId(int functionId) {
    this.functionId = functionId;
  }

  public void setFunctionName(String functionName) {
    this.functionName = functionName;
  }

  public void setOutput(String output) {
    this.output = output;
  }

  public void setError(String error) {
    this.error = error;
  }
}
