package dev.dbos.transact.execution;

import dev.dbos.transact.conductor.ConductorOptions;
import dev.dbos.transact.conductor.ConductorProvider;
import dev.dbos.transact.database.DBTestAccess;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.WorkflowHandle;

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

  public static ConductorOptions getConductorOptions(DBOSExecutor exec) {
    return exec.conductorOptions();
  }

  public static ConductorProvider getConductorProvider(DBOSExecutor exec) {
    return exec.conductorProvider();
  }

  /**
   * Runs a workflow from its row as a queue claim would: under a fresh owner token, installed on
   * the row first. An execution of it already running no longer owns it.
   */
  public static <T, E extends Exception> WorkflowHandle<T, E> executeWorkflowById(
      DBOSExecutor exec, String workflowId) {
    var ownerXid = DBTestAccess.takeOwnership(exec.getSystemDatabase(), workflowId);
    return exec.executeWorkflowById(workflowId, ownerXid);
  }
}
