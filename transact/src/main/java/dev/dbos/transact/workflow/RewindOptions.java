package dev.dbos.transact.workflow;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

/**
 * Options for rewinding a workflow. A rewound workflow keeps its ID and is re-enqueued, so these
 * options say where it is enqueued and which application version runs it.
 *
 * @param applicationVersion Application version to run the rewound workflow on; {@code null} keeps
 *     the version the workflow already has
 * @param queueName Queue to re-enqueue the workflow on; {@code null} uses the internal queue
 * @param queuePartitionKey Partition key on that queue; {@code null} clears any key the workflow
 *     had
 */
public record RewindOptions(
    @Nullable String applicationVersion,
    @Nullable String queueName,
    @Nullable String queuePartitionKey) {

  public RewindOptions() {
    this(null, null, null);
  }

  /**
   * Returns a copy of this object with the given applicationVersion.
   *
   * @param applicationVersion Application version to run the rewound workflow on
   */
  public RewindOptions withApplicationVersion(@Nullable String applicationVersion) {
    return new RewindOptions(applicationVersion, this.queueName, this.queuePartitionKey);
  }

  /**
   * Returns a copy of this object with the given queue.
   *
   * @param queue Queue to re-enqueue the rewound workflow on
   * @return a copy with the queue set
   */
  public RewindOptions withQueue(@NonNull QueueName queue) {
    return withQueue(queue.value());
  }

  /**
   * Returns a copy of this object with the given queueName.
   *
   * @param queueName Queue name to re-enqueue the rewound workflow on
   */
  public RewindOptions withQueue(@Nullable String queueName) {
    return new RewindOptions(this.applicationVersion, queueName, this.queuePartitionKey);
  }

  /**
   * Returns a copy of this object with the given queuePartitionKey.
   *
   * @param queuePartitionKey Queue partition key to re-enqueue the rewound workflow with
   */
  public RewindOptions withQueuePartitionKey(@Nullable String queuePartitionKey) {
    return new RewindOptions(this.applicationVersion, this.queueName, queuePartitionKey);
  }
}
