package dev.dbos.transact.exceptions;

public class DBOSUnexpectedStepException extends RuntimeException {
  private final String workflowId;
  private final int stepId;
  private final String expectedName;
  private final String recordedName;

  public DBOSUnexpectedStepException(
      String workflowId, int stepId, String expectedName, String recordedName) {
    super(
        String.format(
            "During execution of workflow %s step %s, function %s was recorded when %s was expected. Check that your workflow is deterministic.",
            workflowId, stepId, recordedName, expectedName));
    this.workflowId = workflowId;
    this.stepId = stepId;
    this.expectedName = expectedName;
    this.recordedName = recordedName;
  }

  public String workflowId() {
    return this.workflowId;
  }

  public int stepId() {
    return this.stepId;
  }

  public String expectedName() {
    return this.expectedName;
  }

  public String recordedName() {
    return this.recordedName;
  }
}
