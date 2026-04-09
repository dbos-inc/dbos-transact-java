package dev.dbos.transact.workflow;

import java.time.Instant;

public record StepInfo(
    int functionId,
    String functionName,
    Object output,
    ErrorResult error,
    String childWorkflowId,
    Instant startedAt,
    Instant completedAt,
    String serialization) {

  public Long startedAtEpochMs() {
    return startedAt == null ? null : startedAt.toEpochMilli();
  }

  public Long completedAtEpochMs() {
    return completedAt == null ? null : completedAt.toEpochMilli();
  }
}
