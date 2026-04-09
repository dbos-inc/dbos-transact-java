package dev.dbos.transact.workflow;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonProperty;

public record StepInfo(
    int functionId,
    String functionName,
    Object output,
    ErrorResult error,
    String childWorkflowId,
    Instant startedAt,
    Instant completedAt,
    String serialization) {

  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public Long startedAtEpochMs() {
    return startedAt == null ? null : startedAt.toEpochMilli();
  }

  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public Long completedAtEpochMs() {
    return completedAt == null ? null : completedAt.toEpochMilli();
  }
}
