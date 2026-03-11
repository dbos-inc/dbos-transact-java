package dev.dbos.transact;

import dev.dbos.transact.config.DBOSConfig;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.DBOSExecutorTestAccess;
import dev.dbos.transact.execution.QueueService;
import dev.dbos.transact.execution.SchedulerService;

// Helper class to retrieve DBOS internals via package private methods
public class DBOSTestAccess {

  public static DBOSExecutor getDbosExecutor(DBOS.Instance dbos) {
    return dbos.getDbosExecutor();
  }

  public static QueueService getQueueService(DBOS.Instance dbos) {
    var exec = getDbosExecutor(dbos);
    return DBOSExecutorTestAccess.getQueueService(exec);
  }

  public static SystemDatabase getSystemDatabase(DBOS.Instance dbos) {
    var exec = getDbosExecutor(dbos);
    return DBOSExecutorTestAccess.getSystemDatabase(exec);
  }

  public static DBOSExecutor getDbosExecutor() {
    return DBOS.getDbosExecutor();
  }

  public static void reinitialize(DBOSConfig config) {
    DBOS.reinitialize(config);
  }

  // public static void clearRegistry() {
  //   DBOS.instance().clearRegistry();
  // }

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
