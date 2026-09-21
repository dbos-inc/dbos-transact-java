package dev.dbos.transact.exceptions;

/**
 * Thrown when a debounce finds its key held by a debouncer service workflow that never acknowledges
 * the forwarded call.
 *
 * <p>A service workflow acknowledges within milliseconds while it runs. One that holds the key and
 * stays silent is stranded: it was enqueued by an application version no running executor serves,
 * or it was mid-flight on an executor that is gone. The debounce cannot coalesce into it and cannot
 * start beside it, so it gives up after a bounded wait rather than retrying forever.
 */
public class DBOSDebouncerUnreachableException extends RuntimeException {
  private final String holderWorkflowId;
  private final String queueName;
  private final String deduplicationId;

  public DBOSDebouncerUnreachableException(
      String holderWorkflowId, String queueName, String deduplicationId) {
    super(
        String.format(
            "Debouncer %s (Queue: %s, Deduplication ID: %s) holds the debounce key but did not"
                + " acknowledge the call; it is likely stranded and should be cancelled.",
            holderWorkflowId, queueName, deduplicationId));
    this.holderWorkflowId = holderWorkflowId;
    this.queueName = queueName;
    this.deduplicationId = deduplicationId;
  }

  /** The debouncer service workflow holding the key. */
  public String holderWorkflowId() {
    return holderWorkflowId;
  }

  /** The queue the key is held on. */
  public String queueName() {
    return queueName;
  }

  /** The debounce key, as held: the workflow name and the debounce key joined by a hyphen. */
  public String deduplicationId() {
    return deduplicationId;
  }
}
