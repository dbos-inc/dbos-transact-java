package dev.dbos.transact.workflow;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;

public enum WorkflowState {
  PENDING,
  SUCCESS,
  ERROR,
  MAX_RECOVERY_ATTEMPTS_EXCEEDED,
  CANCELLED,
  ENQUEUED,
  DELAYED,
  SUSPENDED,
  /**
   * A status this version of DBOS Transact does not know, such as one a newer version wrote. It is
   * never written: {@link WorkflowStatus#statusValue} holds the status as stored. This version
   * can't tell whether such a workflow is in flight or finished, so it is neither {@linkplain
   * #isActive active} nor {@linkplain #isTerminal terminal}: anything waiting for the workflow to
   * finish keeps waiting, and only an explicit cancel or resume of that workflow writes over its
   * status.
   */
  UNKNOWN;

  private static final List<WorkflowState> ACTIVE = List.of(PENDING, ENQUEUED, DELAYED, SUSPENDED);
  private static final List<WorkflowState> TERMINAL =
      List.of(SUCCESS, ERROR, MAX_RECOVERY_ATTEMPTS_EXCEEDED, CANCELLED);
  private static final List<String> ACTIVE_NAMES = ACTIVE.stream().map(Enum::name).toList();
  private static final List<String> KNOWN_NAMES =
      Arrays.stream(values()).filter(s -> s.isActive() || s.isTerminal()).map(Enum::name).toList();

  /**
   * Whether a workflow in this state is in flight: it has not finished and may still run. False for
   * {@link #UNKNOWN}, which is not known to be in flight, though it is not known to be finished
   * either.
   */
  @JsonIgnore
  public boolean isActive() {
    return ACTIVE.contains(this);
  }

  /**
   * Whether a workflow in this state has finished: it will not run again unless it is resumed,
   * forked or rewound. False for {@link #UNKNOWN}, which is not known to be finished, though it is
   * not known to be in flight either.
   */
  @JsonIgnore
  public boolean isTerminal() {
    return TERMINAL.contains(this);
  }

  /** The states for which {@link #isActive} is true, in declaration order. */
  public static List<WorkflowState> activeStates() {
    return ACTIVE;
  }

  /** The names of {@link #activeStates}, as stored in {@code workflow_status.status}. */
  public static List<String> activeNames() {
    return ACTIVE_NAMES;
  }

  /**
   * The names of every state this version knows, as stored in {@code workflow_status.status}, in
   * declaration order: the {@linkplain #isActive active} and {@linkplain #isTerminal terminal}
   * states, which is all but {@link #UNKNOWN}.
   */
  public static List<String> knownNames() {
    return KNOWN_NAMES;
  }

  /**
   * The state a stored status value names: its constant, or {@link #UNKNOWN} for a status this
   * version does not know. Never throws on an unexpected spelling, so a row or an export a newer
   * version wrote still reads; JSON is read through it too.
   *
   * @param statusValue the status as stored in {@code workflow_status.status}
   * @return the matching state, or {@link #UNKNOWN}
   */
  @JsonCreator
  public static WorkflowState fromStatusValue(String statusValue) {
    Objects.requireNonNull(statusValue, "statusValue must not be null");
    return KNOWN_NAMES.contains(statusValue) ? valueOf(statusValue) : UNKNOWN;
  }
}
