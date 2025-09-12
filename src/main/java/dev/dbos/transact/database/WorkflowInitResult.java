package dev.dbos.transact.database;

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
    return deadlineEpochMS;
  }

  public String getWorkflowId() {
    return workflowId;
  }
}
