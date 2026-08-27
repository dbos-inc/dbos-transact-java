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
    String serialization,
    /**
     * The application that recorded this step, or null if the row is unclaimed and so belongs to
     * every application sharing the system database. Normally the owner of the step's workflow, but
     * a workflow one application owns can be resumed or restarted by another addressing it by ID,
     * and the steps that run then belong to the application that ran them.
     */
    String applicationName) {

  @JsonIgnore
  public Long startedAtEpochMs() {
    return startedAt == null ? null : startedAt.toEpochMilli();
  }

  @JsonIgnore
  public Long completedAtEpochMs() {
    return completedAt == null ? null : completedAt.toEpochMilli();
  }
}
