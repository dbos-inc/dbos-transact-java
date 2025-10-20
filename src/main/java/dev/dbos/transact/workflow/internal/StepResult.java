package dev.dbos.transact.workflow.internal;

public class StepResult {
  private String workflowId;
  private String childWorkflowId;
  private int stepId;
  private String functionName;
  private String output;
  private String error;

  public StepResult() {}

  public StepResult(
      String workflowId,
      int stepId,
      String functionName,
      String output,
      String error,
      String childWorkflowId) {
    this.workflowId = workflowId;
    this.stepId = stepId;
    this.functionName = functionName;
    this.output = output;
    this.error = error;
    this.childWorkflowId = childWorkflowId;
  }

  public String getWorkflowId() {
    return workflowId;
  }

  public String getChildWorkflowId() {
    return childWorkflowId;
  }

  public int getStepId() {
    return stepId;
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

  public void setChildWorkflowId(String childWorkflowId) {
    this.childWorkflowId = childWorkflowId;
  }

  public void setStepId(int stepId) {
    this.stepId = stepId;
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
