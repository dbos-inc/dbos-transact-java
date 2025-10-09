package dev.dbos.transact;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.DBOSExecutorTestAccess;
import dev.dbos.transact.queue.QueueService;
import dev.dbos.transact.scheduled.SchedulerService;

// Helper class to retrieve DBOS internals via package private methods
public class DBOSTestAccess {
  public static DBOSExecutor getDbosExecutor() {
    return DBOS.instance().getDbosExecutor();
  }

  public static void clearRegistry() {
    DBOS.instance().clearRegistry();
  }

  public static SystemDatabase getSystemDatabase() {
    var exec = getDbosExecutor();
    return DBOSExecutorTestAccess.getSystemDatabase(exec);
  }

  public static QueueService getQueueService() {
    var exec = getDbosExecutor();
    return DBOSExecutorTestAccess.getQueueService(exec);
  }

  public static SchedulerService getSchedulerService() {
    var exec = getDbosExecutor();
    return DBOSExecutorTestAccess.getSchedulerService(exec);
  }
}
