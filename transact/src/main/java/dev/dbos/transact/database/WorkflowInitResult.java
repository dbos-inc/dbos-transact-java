package dev.dbos.transact.database;

import java.util.Objects;

public class WorkflowInitResult {
  private String workflowId;
  private String status;
  private Long deadlineEpochMS; // Use Long for nullable number

  public WorkflowInitResult(String id, String status, Long deadlineEpochMS) {
    this.workflowId = id;
    this.status = status;
    this.deadlineEpochMS = deadlineEpochMS;
  }

  public String getStatus() {
    return status;
  }

  public Long getDeadlineEpochMS() {
    return Objects.requireNonNullElse(deadlineEpochMS, 0L);
  }

  public String getWorkflowId() {
    return workflowId;
  }
}
