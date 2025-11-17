package dev.dbos.transact.workflow.internal;

public record StepResult(
    String workflowId,
    int stepId,
    String functionName,
    String output,
    String error,
    String childWorkflowId,
    long startTimeEpochMs) {

  public StepResult(String workflowId, int stepId, String functionName, long startTime) {
    this(workflowId, stepId, functionName, null, null, null, startTime);
  }

  public StepResult withOutput(String v) {
    return new StepResult(
        workflowId, stepId, functionName, v, error, childWorkflowId, startTimeEpochMs);
  }

  public StepResult withError(String v) {
    return new StepResult(
        workflowId, stepId, functionName, output, v, childWorkflowId, startTimeEpochMs);
  }

  public StepResult withChildWorkflowId(String v) {
    return new StepResult(workflowId, stepId, functionName, output, error, v, startTimeEpochMs);
  }
}
