package dev.dbos.transact.execution;

import dev.dbos.transact.conductor.ConductorOptions;
import dev.dbos.transact.database.DBTestAccess;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.spi.EnterpriseProvider;
import dev.dbos.transact.workflow.WorkflowHandle;

import java.util.concurrent.ScheduledThreadPoolExecutor;

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

  /** Whether the executor holds the background-jobs lock. */
  public static boolean holdsBackgroundLease(DBOSExecutor exec) {
    return exec.backgroundLease().held();
  }

  /** Waits until the executor holds the background-jobs lock, and the connection it keeps. */
  public static void awaitBackgroundLease(DBOSExecutor exec) throws InterruptedException {
    for (int i = 0; i < 200 && !exec.backgroundLease().held(); i++) {
      Thread.sleep(50);
    }
    if (!exec.backgroundLease().held()) {
      throw new IllegalStateException("the executor never took the background-jobs lock");
    }
  }

  /** Tasks queued on the executor's timeout scheduler: workflow timers and periodic upkeep. */
  public static int queuedTimeoutTasks(DBOSExecutor exec) {
    return ((ScheduledThreadPoolExecutor) exec.timeoutScheduler()).getQueue().size();
  }

  public static ConductorOptions getConductorOptions(DBOSExecutor exec) {
    return exec.conductorOptions();
  }

  public static EnterpriseProvider getEnterpriseProvider(DBOSExecutor exec) {
    return exec.enterpriseProvider();
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
