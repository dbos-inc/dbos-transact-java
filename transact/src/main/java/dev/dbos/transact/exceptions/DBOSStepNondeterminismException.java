package dev.dbos.transact.exceptions;

/**
 * {@code DBOSStepNondeterminismException} indicates that one execution of a workflow recorded the
 * same step twice with different results. Only the execution that owns a workflow records its
 * steps, so the earlier result is this execution's own: the workflow function is not deterministic,
 * which is a programmer error.
 */
public class DBOSStepNondeterminismException extends RuntimeException {
  private final String workflowId;
  private final int stepId;

  public DBOSStepNondeterminismException(String workflowId, int stepId) {
    super(
        "Step %d of workflow %s was recorded twice by the same execution with different results. Check that your workflow is deterministic."
            .formatted(stepId, workflowId));
    this.workflowId = workflowId;
    this.stepId = stepId;
  }

  /** The ID of the nondeterministic workflow */
  public String workflowId() {
    return this.workflowId;
  }

  /** The step number that was recorded twice */
  public int stepId() {
    return this.stepId;
  }
}
