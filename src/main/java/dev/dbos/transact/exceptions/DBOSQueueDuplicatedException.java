package dev.dbos.transact.exceptions;

public class DBOSQueueDuplicatedException extends RuntimeException {
  private final String workflowId;
  private final String queueName;
  private final String deduplicationId;

  public DBOSQueueDuplicatedException(String workflowId, String queueName, String deduplicationId) {
    super(
        String.format(
            "Workflow %s (Queue: %s, Deduplication ID: %s) is already enqueued.",
            workflowId, queueName, deduplicationId));
    this.workflowId = workflowId;
    this.queueName = queueName;
    this.deduplicationId = deduplicationId;
  }

  public String workflowId() {
    return workflowId;
  }

  public String queueName() {
    return queueName;
  }

  public String deduplicationId() {
    return deduplicationId;
  }
}
