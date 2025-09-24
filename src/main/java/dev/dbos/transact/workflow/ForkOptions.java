package dev.dbos.transact.workflow;

import java.time.Duration;

public record ForkOptions(String forkedWorkflowId, String applicationVersion, Duration timeout) {

  public ForkOptions {
    if (timeout != null && timeout.isNegative()) {
      throw new IllegalArgumentException("timeout must not be negative");
    }
  }

  public ForkOptions() {
    this(null, null, null);
  }

  public ForkOptions(String forkedWorkflowId) {
    this(forkedWorkflowId, null, null);
  }
}
