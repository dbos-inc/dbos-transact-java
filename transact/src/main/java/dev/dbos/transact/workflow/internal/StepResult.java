package dev.dbos.transact.workflow.internal;

public record StepResult(
    String workflowId,
    int stepId,
    String functionName,
    String output,
    String error,
    String childWorkflowId,
    String serialization) {

  public StepResult(String workflowId, int stepId, String functionName) {
    this(workflowId, stepId, functionName, null, null, null, null);
  }

  public StepResult withOutput(String v) {
    return new StepResult(
        workflowId, stepId, functionName, v, error, childWorkflowId, serialization);
  }

  public StepResult withError(String v) {
    return new StepResult(
        workflowId, stepId, functionName, output, v, childWorkflowId, serialization);
  }

  public StepResult withChildWorkflowId(String v) {
    return new StepResult(workflowId, stepId, functionName, output, error, v, serialization);
  }

  public StepResult withSerialization(String v) {
    return new StepResult(workflowId, stepId, functionName, output, error, childWorkflowId, v);
  }
}
