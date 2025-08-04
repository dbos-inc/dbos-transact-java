package dev.dbos.transact.workflow;

public enum WorkflowState {
  PENDING,
  SUCCESS,
  ERROR,
  RETRIES_EXCEEDED,
  CANCELLED,
  ENQUEUED
}
