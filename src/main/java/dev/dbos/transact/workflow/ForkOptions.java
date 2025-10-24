package dev.dbos.transact.workflow;

import java.time.Duration;

/**
 * Options for forking a workflow.  This includes:
 *   Specified ID for the new workflow
 *   Application version to use for executing the new workflow
 *   Timeout to apply for the new workflow execution
 */
public record ForkOptions(String forkedWorkflowId, String applicationVersion, Duration timeout) {

  public ForkOptions {
    if (timeout != null && timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must not be negative");
    }
  }

  public ForkOptions() {
    this(null, null, null);
  }

  /** Assign the workflow ID for the new workflow */
  public ForkOptions(String forkedWorkflowId) {
    this(forkedWorkflowId, null, null);
  }

  /**
   * Returns a copy of this object with the given forkedWorkflowId.
   * @param forkedWorkflowId ID to assign to the forked workflow.
   */
  public ForkOptions withForkedWorkflowId(String forkedWorkflowId) {
    return new ForkOptions(forkedWorkflowId, this.applicationVersion, this.timeout);
  }

  /**
   * Returns a copy of this object with the given applicationVersion.
   * @param applicationVersion Application version to use for the new fork of the workflow
   */
  public ForkOptions withApplicationVersion(String applicationVersion) {
    return new ForkOptions(this.forkedWorkflowId, applicationVersion, this.timeout);
  }

  /**
   * Returns a copy of this object with the given timeout.
   * @param timeout Duration to allow for the workflow to run, before canceling the workflow
   */
  public ForkOptions withTimeout(Duration timeout) {
    return new ForkOptions(this.forkedWorkflowId, this.applicationVersion, timeout);
  }
}
