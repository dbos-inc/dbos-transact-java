package dev.dbos.transact;

import dev.dbos.transact.execution.DBOSExecutor;

// Helper class to retrieve DBOS internals via package private methods
public class DBOSTestAccess {

  public static DBOSExecutor getDbosExecutor(DBOS dbos) {
    return dbos.getDbosExecutor();
  }
}
