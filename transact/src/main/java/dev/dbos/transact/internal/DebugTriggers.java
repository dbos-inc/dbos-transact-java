package dev.dbos.transact.internal;

import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public final class DebugTriggers {

  private DebugTriggers() {}

  public static final class CallSiteInfo {
    private final String fileName;
    private final int lineNumber;

    public CallSiteInfo(String fileName, int lineNumber) {
      this.fileName = fileName;
      this.lineNumber = lineNumber;
    }

    public String getFileName() {
      return fileName;
    }

    public int getLineNumber() {
      return lineNumber;
    }
  }

  public static CallSiteInfo getCallSiteInfo() {
    Throwable t = new Throwable();
    StackTraceElement[] stack = t.getStackTrace();

    if (stack == null || stack.length == 0) {
      return new CallSiteInfo("unknown", -1);
    }

    String thisClassName = DebugTriggers.class.getName();

    for (StackTraceElement ste : stack) {
      if (!thisClassName.equals(ste.getClassName())) {
        String fileName = ste.getFileName() != null ? ste.getFileName() : "unknown";
        int lineNumber = ste.getLineNumber();
        return new CallSiteInfo(fileName, lineNumber);
      }
    }

    StackTraceElement ste = stack[0];
    String fileName = ste.getFileName() != null ? ste.getFileName() : "unknown";
    return new CallSiteInfo(fileName, ste.getLineNumber());
  }

  public static final class DebugPoint {
    private final String name;
    private final String fileName;
    private final int lineNumber;
    private final AtomicLong hitCount = new AtomicLong(0);

    public DebugPoint(String name, String fileName, int lineNumber) {
      this.name = name;
      this.fileName = fileName;
      this.lineNumber = lineNumber;
    }

    public String getName() {
      return name;
    }

    public String getFileName() {
      return fileName;
    }

    public int getLineNumber() {
      return lineNumber;
    }

    public long getHitCount() {
      return hitCount.get();
    }

    public long incrementHitCount() {
      return hitCount.incrementAndGet();
    }
  }

  public static final class DebugAction {
    /** Sleep in milliseconds at the point (optional). */
    private Long sleepMs;

    /** Synchronous callback (optional). */
    private Runnable callback;

    /** Semaphore to await (optional). */
    private Semaphore semaphore;

    /** SQLException to throw (optional). */
    private SQLException sqlExceptionToThrow;

    public Long getSleepMs() {
      return sleepMs;
    }

    public DebugAction setSleepMs(Long sleepMs) {
      this.sleepMs = sleepMs;
      return this;
    }

    public Runnable getCallback() {
      return callback;
    }

    public DebugAction setCallback(Runnable callback) {
      this.callback = callback;
      return this;
    }

    public Semaphore getSemaphore() {
      return semaphore;
    }

    public DebugAction setSemaphore(Semaphore semaphore) {
      this.semaphore = semaphore;
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
  private static final Map<String, DebugPoint> pointLocations = new ConcurrentHashMap<>();

  /**
   * Blocks the current thread according to the configured DebugAction.
   *
   * <p>Order: 1. Record point + increment hit count 2. Sleep (if configured) 3. Run callback (if
   * configured) 4. Acquire semaphore (if configured) and block until someone releases
   */
  public static void debugTriggerPoint(String name) throws SQLException {
    CallSiteInfo csi = getCallSiteInfo();

    DebugPoint point =
        pointLocations.computeIfAbsent(
            name, n -> new DebugPoint(n, csi.getFileName(), csi.getLineNumber()));
    point.incrementHitCount();

    DebugAction action = pointTriggers.get(name);
    if (action == null) {
      return; // nothing to do
    }

    try {
      if (action.getSleepMs() != null && action.getSleepMs() > 0) {
        Thread.sleep(action.getSleepMs());
      }

      if (action.getCallback() != null) {
        action.getCallback().run();
      }

      SQLException sqle = action.getSqlExceptionToThrow();
      if (sqle != null) {
        throw sqle;
      }

      Semaphore sem = action.getSemaphore();
      if (sem != null) {
        sem.acquire();
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }

  public static void setDebugTrigger(String name, DebugAction action) {
    pointTriggers.put(name, action);
  }

  public static void clearDebugTriggers() {
    pointTriggers.clear();
  }

  public static Map<String, DebugPoint> getPointLocationsSnapshot() {
    return new ConcurrentHashMap<>(pointLocations);
  }

  /** Convenience: release a permit on the semaphore for a given trigger (if present) */
  public static void releaseDebugTrigger(String name) {
    DebugAction action = pointTriggers.get(name);
    if (action != null && action.getSemaphore() != null) {
      action.getSemaphore().release();
    }
  }

  // ----- Constants (Should use in just one place in the code) -----

  // public static final String DEBUG_TRIGGER_WORKFLOW_QUEUE_START =
  // "DEBUG_TRIGGER_WORKFLOW_QUEUE_START";
  // public static final String DEBUG_TRIGGER_WORKFLOW_ENQUEUE =
  // "DEBUG_TRIGGER_WORKFLOW_ENQUEUE";
  public static final String DEBUG_TRIGGER_STEP_COMMIT = "DEBUG_TRIGGER_STEP_COMMIT";
  public static final String DEBUG_TRIGGER_INITWF_COMMIT = "DEBUG_TRIGGER_INITWF_COMMIT";
}
