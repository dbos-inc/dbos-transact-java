package dev.dbos.transact.execution;

import dev.dbos.transact.database.BackgroundJobLock;

// In this package to reach the executor's package-private methods.
public class DBOSExecutorTestAccess {

  /** Waits until the executor holds the background-jobs lock, and the connection it keeps. */
  public static void awaitBackgroundJobLock(DBOSExecutor exec) throws Exception {
    var lockName = exec.getBackgroundJobs().lockName();
    for (int i = 0; i < 200; i++) {
      // A try of its own fails only while another holds the lock, here the executor.
      try (var probe =
          exec.getSystemDatabase()
              .backgroundJobLock(lockName, BackgroundJobLock.DEFAULT_IDLE_TIMEOUT)) {
        if (!probe.acquired()) {
          return;
        }
      }
      Thread.sleep(50);
    }
    throw new IllegalStateException("the executor never took the background-jobs lock");
  }
}
