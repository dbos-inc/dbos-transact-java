package dev.dbos.transact.exceptions;

/**
 * {@code DBOSMaxRecoveryAttemptsExceededException} is thrown when workflow recovery is attempted,
 * but the maximum number of recovery attempts have already been made.
 */
public class DBOSMaxRecoveryAttemptsExceededException extends RuntimeException {

  private String workflowId;
  private int maxRetries;

  public DBOSMaxRecoveryAttemptsExceededException(String workflowId, int maxRetries) {
    super(
        String.format(
            "Workflow %s has been moved to the dead-letter queue after exceeding the maximum of %d retries",
            workflowId, maxRetries));
  }

  /**
   * The ID of the workflow for which a maximum number of recovery attempts have already been made
   */
  public String workflowId() {
    return workflowId;
  }

  /** The number of retries allowed */
  public int maxRetries() {
    return maxRetries;
  }
}
