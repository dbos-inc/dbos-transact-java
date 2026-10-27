package dev.dbos.transact.workflow;

import static dev.dbos.transact.internal.Validation.exceedsMillis;
import static dev.dbos.transact.internal.Validation.nullableExceedsMillis;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

import org.jspecify.annotations.NonNull;

public sealed interface WorkflowDelay permits WorkflowDelay.Delay, WorkflowDelay.DelayUntil {

  record Delay(@NonNull Duration delay) implements WorkflowDelay {
    public Delay {
      // Refused here, before setWorkflowDelay takes a step, so a refused call records nothing.
      if (nullableExceedsMillis(Objects.requireNonNull(delay))) {
        throw new IllegalArgumentException(exceedsMillis("delay"));
      }
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
