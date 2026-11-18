package dev.dbos.transact.exceptions;

import dev.dbos.transact.Constants;

import org.jspecify.annotations.Nullable;

/**
 * Thrown by a {@code recv} or {@code sleep} called with {@code suspend} once its workflow is
 * suspended in the database. The execution must exit: a send on the topic, the recv's timeout, or
 * the end of the sleep re-enqueues the workflow, and the replayed call returns.
 *
 * <p>An {@link Error}, not an {@link Exception}, so that workflow code which catches {@code
 * Exception} cannot swallow it and carry on after suspending. Workflow code should not catch it;
 * one that does must rethrow it. It is not a failure, and nothing is recorded as the workflow's
 * outcome.
 */
public class DBOSWorkflowSuspendedException extends Error {
  private static final long serialVersionUID = 1L;

  private final String workflowId;
  private final @Nullable String topic;

  /**
   * @param workflowId the workflow that was suspended
   * @param topic the topic it waits on, {@link Constants#DBOS_NULL_TOPIC} for a recv with none, or
   *     {@code null} for a sleep
   */
  public DBOSWorkflowSuspendedException(String workflowId, @Nullable String topic) {
    super(message(workflowId, topic), null, false, false);
    this.workflowId = workflowId;
    this.topic = topic;
  }

  private static String message(String workflowId, @Nullable String topic) {
    var waiting =
        topic == null
            ? "sleeping"
            : topic.equals(Constants.DBOS_NULL_TOPIC)
                ? "waiting on a message with no topic"
                : "waiting on topic " + topic;
    return "Workflow %s suspended %s".formatted(workflowId, waiting);
  }

  /** The workflow that was suspended. */
  public String workflowId() {
    return workflowId;
  }

  /** The topic the workflow waits on, or {@code null} if it sleeps. */
  public @Nullable String topic() {
    return topic;
  }
}
