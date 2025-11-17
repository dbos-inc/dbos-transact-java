package dev.dbos.transact.workflow.internal;

public record StepResult(
    String workflowId,
    int stepId,
    String functionName,
    String output,
    String error,
    String childWorkflowId) {

  public StepResult(String workflowId, int stepId, String functionName) {
    this(workflowId, stepId, functionName, null, null, null);
  }

  public StepResult withOutput(String v) {
    return new StepResult(workflowId, stepId, functionName, v, error, childWorkflowId);
  }

  public StepResult withError(String v) {
    return new StepResult(workflowId, stepId, functionName, output, v, childWorkflowId);
  }

  public StepResult withChildWorkflowId(String v) {
    return new StepResult(workflowId, stepId, functionName, output, error, v);
  }
}
