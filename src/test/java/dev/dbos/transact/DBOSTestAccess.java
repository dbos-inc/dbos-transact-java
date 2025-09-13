package dev.dbos.transact;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.DBOSExecutorTestAccess;
import dev.dbos.transact.queue.QueueService;
import dev.dbos.transact.scheduled.SchedulerService;

// Helper class to retrieve DBOS internals via package private methods
public class DBOSTestAccess {
  public static DBOSExecutor getDbosExecutor(DBOS dbos) {
    return dbos.getDbosExecutor();
  }

  public static void clearRegistry(DBOS dbos) {
    dbos.clearRegistry();
  }

  public static SystemDatabase getSystemDatabase(DBOS dbos) {
    var exec = getDbosExecutor(dbos);
    return DBOSExecutorTestAccess.getSystemDatabase(exec);
  }

  public static QueueService getQueueService(DBOS dbos) {
    var exec = getDbosExecutor(dbos);
    return DBOSExecutorTestAccess.getQueueService(exec);
  }

  public static SchedulerService getSchedulerService(DBOS dbos) {
    var exec = getDbosExecutor(dbos);
    return DBOSExecutorTestAccess.getSchedulerService(exec);
  }
}
