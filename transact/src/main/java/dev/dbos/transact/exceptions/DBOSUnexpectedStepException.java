package dev.dbos.transact.exceptions;

/**
 * {@code DBOSUnexpectedStepException} indicates that a restarted/replayed workflow function
 * attempted to execute a different step than it did during its original run. This indicates that
 * the workflow function is not deterministic, which is a programmer error.
 */
public class DBOSUnexpectedStepException extends RuntimeException {
  private final String workflowId;
  private final int stepId;
  private final String attemptedName;
  private final String recordedName;

  public DBOSUnexpectedStepException(
      String workflowId, int stepId, String attemptedName, String recordedName) {
    super(
        String.format(
            "During reexecution of workflow %s step %s, function %s was attempted but %s was executed in the original execution. Check that your workflow is deterministic.",
            workflowId, stepId, attemptedName, recordedName));
    this.workflowId = workflowId;
    this.stepId = stepId;
    this.attemptedName = attemptedName;
    this.recordedName = recordedName;
  }

  /** The ID of the nondeterministic workflow */
  public String workflowId() {
    return this.workflowId;
  }

  /** The step number within the workflow at which nondeterminism was detected */
  public int stepId() {
    return this.stepId;
  }

  /** The step function name called during the current reexecution */
  public String attemptedName() {
    return this.attemptedName;
  }

  /** The step function name called during the original execution */
  public String recordedName() {
    return this.recordedName;
  }
}
