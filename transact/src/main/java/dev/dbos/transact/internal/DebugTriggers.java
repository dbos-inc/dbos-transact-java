package dev.dbos.transact.internal;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class DebugTriggers {

  private DebugTriggers() {}

  public static final class DebugAction {
    /** Synchronous callback (optional). */
    private Runnable callback;

    /** SQLException to throw (optional). */
    private SQLException sqlExceptionToThrow;

    public Runnable getCallback() {
      return callback;
    }

    public DebugAction setCallback(Runnable callback) {
      this.callback = callback;
      return this;
    }

    public SQLException getSqlExceptionToThrow() {
      return this.sqlExceptionToThrow;
    }

    public DebugAction setSqlExceptionToThrow(SQLException sqle) {
      this.sqlExceptionToThrow = sqle;
      return this;
    }
  }

  private static final Map<String, DebugAction> pointTriggers = new ConcurrentHashMap<>();

  /**
   * Proceed according to the configured DebugAction: run its callback, then throw its exception.
   */
  public static void debugTriggerPoint(String name) throws SQLException {
    DebugAction action = pointTriggers.get(name);
    if (action == null) {
      return; // nothing to do
    }

    if (action.getCallback() != null) {
      action.getCallback().run();
    }

    SQLException sqle = action.getSqlExceptionToThrow();
    if (sqle != null) {
      action.setSqlExceptionToThrow(null); // Only do once
      throw sqle;
    }
  }

  public static void setDebugTrigger(String name, DebugAction action) {
    pointTriggers.put(name, action);
  }

  public static void clearDebugTriggers() {
    pointTriggers.clear();
  }

  // ----- Constants (Should use in just one place in the code) -----

  // public static final String DEBUG_TRIGGER_WORKFLOW_QUEUE_START =
  // "DEBUG_TRIGGER_WORKFLOW_QUEUE_START";
  // public static final String DEBUG_TRIGGER_WORKFLOW_ENQUEUE =
  // "DEBUG_TRIGGER_WORKFLOW_ENQUEUE";
  public static final String DEBUG_TRIGGER_STEP_COMMIT = "DEBUG_TRIGGER_STEP_COMMIT";
  public static final String DEBUG_TRIGGER_INITWF_COMMIT = "DEBUG_TRIGGER_INITWF_COMMIT";
  // Inside a debouncer's takeover transaction, after it cancels a stranded debouncer workflow and
  // before it creates the promised workflow.
  public static final String DEBUG_TRIGGER_DEBOUNCE_TAKEOVER = "DEBUG_TRIGGER_DEBOUNCE_TAKEOVER";
}
