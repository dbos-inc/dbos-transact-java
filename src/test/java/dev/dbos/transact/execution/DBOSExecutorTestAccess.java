package dev.dbos.transact.execution;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.queue.QueueService;
import dev.dbos.transact.scheduled.SchedulerService;

/**
 * Note that this class is in the `dev.dbos.transact.execution` package and allows access to
 * package-scoped executor methods as a result.
 */
public class DBOSExecutorTestAccess {
  public static SystemDatabase getSystemDatabase(DBOSExecutor exec) {
    return exec.getSystemDatabase();
  }

  public static QueueService getQueueService(DBOSExecutor exec) {
    return exec.getQueueService();
  }

  public static SchedulerService getSchedulerService(DBOSExecutor exec) {
    return exec.getSchedulerService();
  }
}
