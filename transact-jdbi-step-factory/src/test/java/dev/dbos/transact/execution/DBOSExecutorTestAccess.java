package dev.dbos.transact.execution;

// In this package to reach the executor's package-private methods.
public class DBOSExecutorTestAccess {

  /** Waits until the executor holds the background-jobs lock, and the connection it keeps. */
  public static void awaitBackgroundLease(DBOSExecutor exec) throws InterruptedException {
    for (int i = 0; i < 200 && !exec.backgroundLease().held(); i++) {
      Thread.sleep(50);
    }
    if (!exec.backgroundLease().held()) {
      throw new IllegalStateException("the executor never took the background-jobs lock");
    }
  }
}
