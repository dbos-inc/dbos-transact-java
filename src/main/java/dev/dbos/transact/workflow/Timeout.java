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

  static Timeout inherit() {
    return new Inherit();
  }

  static Timeout none() {
    return new None();
  }

  static Timeout of(Duration d) {
    return new Explicit(Objects.requireNonNull(d, "Explicit timeout duration must not be null"));
  }

  static Timeout of(long value, TimeUnit unit) {
    return new Explicit(Duration.ofNanos(unit.toNanos(value)));
  }
}
