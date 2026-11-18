package dev.dbos.transact.workflow;

import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

public enum WorkflowState {
  PENDING(true),
  SUCCESS(false),
  ERROR(false),
  MAX_RECOVERY_ATTEMPTS_EXCEEDED(false),
  CANCELLED(false),
  ENQUEUED(true),
  DELAYED(true),
  SUSPENDED(true);

  private static final List<WorkflowState> ACTIVE =
      Arrays.stream(values()).filter(WorkflowState::isActive).toList();
  private static final List<String> ACTIVE_NAMES = ACTIVE.stream().map(Enum::name).toList();

  private final boolean active;

  WorkflowState(boolean active) {
    this.active = active;
  }

  /** Whether a workflow in this state is in flight: it has not finished and may still run. */
  @JsonIgnore
  public boolean isActive() {
    return active;
  }

  /** The states for which {@link #isActive} is true, in declaration order. */
  public static List<WorkflowState> activeStates() {
    return ACTIVE;
  }

  /** The names of {@link #activeStates}, as stored in {@code workflow_status.status}. */
  public static List<String> activeNames() {
    return ACTIVE_NAMES;
  }
}
