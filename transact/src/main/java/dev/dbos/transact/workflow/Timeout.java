package dev.dbos.transact.workflow;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public sealed interface Timeout permits Timeout.Inherit, Timeout.None, Timeout.Explicit {
  record Inherit() implements Timeout {}

  record None() implements Timeout {}

  record Explicit(Duration value) implements Timeout {
    public Explicit {
      Objects.requireNonNull(value, "timeout duration must not be null");
    }
  }

  /**
   * Bound the workflow by the running workflow's deadline, if any. Outside a workflow, no timeout.
   */
  static Timeout inherit() {
    return new Inherit();
  }

  /**
   * Run the workflow with no timeout, and don't inherit a deadline. A deadline set on the same
   * options still applies.
   */
  static Timeout none() {
    return new None();
  }

  /**
   * Cancel the workflow once it has run for {@code d}, timed from when it starts, or from when it
   * is dequeued if it is queued. A null duration means {@link #none()}.
   */
  static Timeout of(Duration d) {
    if (d == null) return none();
    return new Explicit(d);
  }

  /** As {@link #of(Duration)}, with the duration given as a value and unit. */
  static Timeout of(long value, TimeUnit unit) {
    return new Explicit(Duration.ofNanos(unit.toNanos(value)));
  }
}
