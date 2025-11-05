package dev.dbos.transact.execution;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSWorkflowFunctionNotFoundException;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveryService {

  private final SystemDatabase systemDatabase;
  private final DBOSExecutor dbosExecutor;
  private static final Logger logger = LoggerFactory.getLogger(RecoveryService.class);

  private volatile boolean stopRequested = false;
  private Thread recoveryThread;

  public RecoveryService(DBOSExecutor dbosExecutor, SystemDatabase systemDatabase) {
    this.systemDatabase = systemDatabase;
    this.dbosExecutor = dbosExecutor;
  }

  /**
   * Starts the background recovery thread for startup workflow recovery. This method will attempt
   * to recover pending workflows in a separate thread.
   */
  public void start() {
    if (recoveryThread != null && recoveryThread.isAlive()) {
      logger.warn("Recovery thread is already running");
      return;
    }

    List<GetPendingWorkflowsOutput> workflows =
        systemDatabase.getPendingWorkflows(dbosExecutor.executorId(), dbosExecutor.appVersion());

    if (workflows.size() > 0) {
      logger.info(
          "Recovering {} workflows for application version {}",
          workflows.size(),
          dbosExecutor.appVersion());
    } else {
      logger.info("No workflows to recover for application version {}", dbosExecutor.appVersion());
    }

    final List<GetPendingWorkflowsOutput> toRecover = workflows;
    stopRequested = false;
    recoveryThread = new Thread(() -> startupRecoveryThread(toRecover), "RecoveryService-Thread");
    recoveryThread.setDaemon(true);
    recoveryThread.start();
    logger.debug("Recovery service started");
  }

  /**
   * Stops the background recovery thread. This method will signal the thread to stop and wait for
   * it to complete.
   */
  public void stop() {
    logger.debug("stop() called");

    stopRequested = true;
    if (recoveryThread != null) {
      try {
        recoveryThread.join(5000); // Wait up to 5 seconds for thread to stop
        if (recoveryThread.isAlive()) {
          logger.warn("Recovery thread did not stop within timeout");
          recoveryThread.interrupt();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn("Interrupted while stopping recovery thread", e);
      }
    }
    logger.debug("Recovery service stopped");
  }

  /**
   * Background thread method that attempts to recover local pending workflows on startup. This
   * method runs continuously until stop is requested or all workflows are recovered.
   */
  private void startupRecoveryThread(List<GetPendingWorkflowsOutput> wToRecover) {
    try {
      List<GetPendingWorkflowsOutput> pendingWorkflows = new CopyOnWriteArrayList<>(wToRecover);

      logger.debug("Starting recovery thread {} ", pendingWorkflows.size());

      while (!stopRequested && !pendingWorkflows.isEmpty()) {
        try {
          // Create a copy to iterate over to avoid concurrent modification
          List<GetPendingWorkflowsOutput> currentPending = new ArrayList<>(pendingWorkflows);

          for (GetPendingWorkflowsOutput pendingWorkflow : currentPending) {
            if (stopRequested) {
              break;
            }
            dbosExecutor.recoverWorkflow(pendingWorkflow);
            pendingWorkflows.remove(pendingWorkflow);
          }
        } catch (DBOSWorkflowFunctionNotFoundException e) {
          logger.debug("Workflow function not found during recovery, retrying in 1 second", e);
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            logger.warn("Recovery thread interrupted during sleep");
            break;
          }
        } catch (Exception e) {
          logger.error("Exception encountered when recovering workflows", e);
          throw e;
        }
      }

      if (!stopRequested && pendingWorkflows.isEmpty()) {
        logger.debug("All pending workflows recovered successfully");
      }

    } catch (Exception e) {
      logger.error("Unexpected error during workflow recovery", e);
    } finally {
      logger.debug("Exiting recovery thread ");
    }
  }
}
