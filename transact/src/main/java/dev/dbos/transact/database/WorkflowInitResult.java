package dev.dbos.transact.database;

import java.util.Objects;

public record WorkflowInitResult(
    String workflowId,
    String status,
    Long deadlineEpochMS,
    boolean shouldExecuteOnThisExecutor,
    String serialization) {
  public Long deadlineEpochMS() {
    return Objects.requireNonNullElse(deadlineEpochMS, 0L);
  }
}
