package dev.dbos.transact.workflow;

public enum WorkflowState {
  PENDING,
  SUCCESS,
  ERROR,
  MAX_RECOVERY_ATTEMPTS_EXCEEDED,
  CANCELLED,
  ENQUEUED,
  DELAYED;

  public boolean isActive() {
    return this == PENDING || this == ENQUEUED || this == DELAYED;
  }
}
