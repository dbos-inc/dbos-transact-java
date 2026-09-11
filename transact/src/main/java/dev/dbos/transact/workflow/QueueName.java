package dev.dbos.transact.workflow;

import java.util.Objects;

import org.jspecify.annotations.NonNull;

/**
 * A workflow queue's name, which is its identity.
 *
 * <p>A queue is addressed by name everywhere: at enqueue time, when forking onto one, when
 * debouncing through one, and when telling a process which queues to poll. Every one of those
 * surfaces takes a {@code String}, and so does {@link dev.dbos.transact.StartWorkflowOptions}'s
 * single-argument constructor — where a {@code String} means a <em>workflow id</em> instead. The
 * two are unrelated identifiers spelled the same way, so
 *
 * <pre>{@code
 * new StartWorkflowOptions("my-queue")   // a workflow id, not a queue
 * }</pre>
 *
 * compiles cleanly and starts an unqueued workflow under a nonsense id. Wrapping the name gives the
 * compiler something to tell them apart by:
 *
 * <pre>{@code
 * new StartWorkflowOptions(QueueName.of("my-queue"))
 * }</pre>
 *
 * <p>The {@code String} overloads are not going anywhere — this is an alternative for callers who
 * want the distinction, not a replacement.
 *
 * <h2>What this does not validate</h2>
 *
 * <p>Only that the name is present and not blank. It deliberately imposes <strong>no character set
 * or length rule</strong>, because queue names are not application names: Transact's only
 * constraint is that {@code _dbos_internal_queue} is reserved, and that is enforced at registration
 * rather than here — the SDK itself legitimately names the internal queue. Python, Go and
 * TypeScript accept the same wide set, and a stricter rule here would refuse to address queues that
 * a peer SDK created on a shared system database.
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

  /**
   * The bare name, not the record's generated form.
   *
   * <p>These end up in log lines and exception messages, where {@code QueueName[value=orders]}
   * reads worse than {@code orders} and tells the reader nothing they did not already know from the
   * surrounding message.
   */
  @Override
  public String toString() {
    return value;
  }
}
