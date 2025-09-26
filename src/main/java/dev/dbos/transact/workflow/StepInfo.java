package dev.dbos.transact.workflow;

public record StepInfo(
    int functionId,
    String functionName,
    Object output,
    StepInfoError error,
    String childWorkflowId) {
  public static record StepInfoError(
      String className, String message, String serializedError, Throwable throwable) {}

  @Override
  public String toString() {
    return String.format(
        "StepInfo{functionId=%d, functionName='%s', output=%s, error=%s, childWorkflowId='%s'}",
        functionId,
        functionName,
        output,
        error != null ? String.format("%s: %S", error.className(), error.message()) : null,
        childWorkflowId);
  }
}
