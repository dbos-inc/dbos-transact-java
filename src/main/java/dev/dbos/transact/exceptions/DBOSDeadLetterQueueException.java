package dev.dbos.transact.exceptions;

public class DBOSDeadLetterQueueException extends RuntimeException {

  private String workflowId;
  private int maxRetries;

  public DBOSDeadLetterQueueException(String workflowId, int maxRetries) {
    super(
        String.format(
            "Workflow %s has been moved to the dead-letter queue after exceeding the maximum of %d retries",
            workflowId, maxRetries));
  }

  public String getWorkflowId() {
    return workflowId;
  }

  public int getMaxRetries() {
    return maxRetries;
  }
}
