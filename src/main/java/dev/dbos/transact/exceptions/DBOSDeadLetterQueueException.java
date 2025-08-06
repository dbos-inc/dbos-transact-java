package dev.dbos.transact.exceptions;

import static dev.dbos.transact.exceptions.ErrorCode.DEAD_LETTER_QUEUE;

public class DBOSDeadLetterQueueException extends DBOSException {

  private String workflowId;
  private int maxRetries;

  public DBOSDeadLetterQueueException(String workflowId, int maxRetries) {
    super(
        DEAD_LETTER_QUEUE.getCode(),
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
