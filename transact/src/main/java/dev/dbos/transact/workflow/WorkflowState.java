package dev.dbos.transact.workflow;

import com.fasterxml.jackson.annotation.JsonIgnore;

public enum WorkflowState {
  PENDING,
  SUCCESS,
  ERROR,
  MAX_RECOVERY_ATTEMPTS_EXCEEDED,
  CANCELLED,
  ENQUEUED,
  DELAYED;

  @JsonIgnore
  public boolean isActive() {
    return this == PENDING || this == ENQUEUED || this == DELAYED;
  }
}
