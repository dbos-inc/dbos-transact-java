package dev.dbos.transact.workflow;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonIgnore;

public record StepInfo(
    int functionId,
    String functionName,
    Object output,
    ErrorResult error,
    String childWorkflowId,
    Instant startedAt,
    Instant completedAt,
    String serialization) {

  @JsonIgnore
  public Long startedAtEpochMs() {
    return startedAt == null ? null : startedAt.toEpochMilli();
  }

  @JsonIgnore
  public Long completedAtEpochMs() {
    return completedAt == null ? null : completedAt.toEpochMilli();
  }
}
