package dev.dbos.transact.exceptions;

/**
 * {@code DBOSQueueDuplicatedException} is thrown when an attempt is made to enqueue a workflow into
 * a queue with deduplication. DBOSQueueDuplicatedException is thrown to indicate that a queued
 * workflow with this `deduplicationId` already exists.
 */
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

  /** ID of the workflow for which enqueue was attempted (This is not the existing workflow ID) */
  public String workflowId() {
    return workflowId;
  }

  /** The name of the queue into which enqueueing was attempted */
  public String queueName() {
    return queueName;
  }

  /**
   * The deduplication ID within the queue; a workflow with this deduplication ID was already
   * enqueued.
   */
  public String deduplicationId() {
    return deduplicationId;
  }
}
