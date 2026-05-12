package dev.dbos.transact.workflow;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.NonNull;

public sealed interface WorkflowDelay permits WorkflowDelay.Delay, WorkflowDelay.DelayUntil {

  record Delay(@NonNull Duration delay) implements WorkflowDelay {
    public Delay {
      Objects.requireNonNull(delay);
    }
  }

  record DelayUntil(@NonNull Instant delayUntil) implements WorkflowDelay {
    public DelayUntil {
      if (Objects.requireNonNull(delayUntil).toEpochMilli() < 0) {
        throw new IllegalArgumentException("delayUntil must be positive");
      }
    }
  }
}
