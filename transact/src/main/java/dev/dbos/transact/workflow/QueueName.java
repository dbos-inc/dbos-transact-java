package dev.dbos.transact.workflow;

import java.util.Objects;

import org.jspecify.annotations.NonNull;

/**
 * A workflow queue's name, which is its identity.
 *
 * <p>Tells a queue name apart from the other {@code String}s it travels with — most sharply a
 * workflow id, which {@link dev.dbos.transact.StartWorkflowOptions}'s single-argument constructor
 * takes instead. The {@code String} overloads remain; this is an alternative, not a replacement.
 *
 * <p>Validates only that the name is present and not blank. No character-set or length rule:
 * Transact reserves {@code _dbos_internal_queue} and nothing else, and a stricter rule here could
 * not address queues a peer SDK created on a shared system database.
 *
 * @param value the queue's name
 */
public record QueueName(@NonNull String value) {

  public QueueName {
    Objects.requireNonNull(value, "Queue name must not be null");
    if (value.isBlank()) {
      throw new IllegalArgumentException("Queue name must not be blank");
    }
  }

  /**
   * The name {@code value} refers to.
   *
   * @param value the queue's name
   * @return the wrapped name
   */
  public static @NonNull QueueName of(@NonNull String value) {
    return new QueueName(value);
  }

  /** The bare name, for log lines and exception messages. */
  @Override
  public String toString() {
    return value;
  }
}
