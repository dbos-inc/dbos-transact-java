package dev.dbos.transact.execution;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.WorkflowFunctionNotFoundException;
import dev.dbos.transact.workflow.WorkflowHandle;
import dev.dbos.transact.workflow.internal.GetPendingWorkflowsOutput;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecoveryService {

    private final SystemDatabase systemDatabase;
    private final DBOSExecutor dbosExecutor;
    public Logger logger = LoggerFactory.getLogger(RecoveryService.class);

    private volatile boolean stopRequested = false;
    private Thread recoveryThread;

    public RecoveryService(DBOSExecutor dbosExecutor, SystemDatabase systemDatabase) {
        this.systemDatabase = systemDatabase;
        this.dbosExecutor = dbosExecutor;
    }

    public void recoverWorkflows() {

        try {
            List<GetPendingWorkflowsOutput> pendingWorkflowsOutputs = getPendingWorkflows();
            recoverWorkflows(pendingWorkflowsOutputs);
        } catch (SQLException e) {
            logger.error("Recovery could not complete due to SQL error", e);
        }
    }

    public List<WorkflowHandle> recoverWorkflows(
            List<GetPendingWorkflowsOutput> pendingWorkflowsOutputs) {

        List<WorkflowHandle> handles = new ArrayList<>();

        for (GetPendingWorkflowsOutput pendingW : pendingWorkflowsOutputs) {
            logger.info("Recovery executing workflow " + pendingW.getWorkflowUuid());
            handles.add(dbosExecutor.executeWorkflowById(pendingW.getWorkflowUuid()));
        }

        return handles;
    }

    public List<GetPendingWorkflowsOutput> getPendingWorkflows() throws SQLException {
        return systemDatabase.getPendingWorkflows(dbosExecutor.getExecutorId(),
                dbosExecutor.getAppVersion());
    }

    /**
     * Starts the background recovery thread for startup workflow recovery. This
     * method will attempt to recover pending workflows in a separate thread.
     */
    public void start() {
        if (recoveryThread != null && recoveryThread.isAlive()) {
            logger.warn("Recovery thread is already running");
            return;
        }

        List<GetPendingWorkflowsOutput> workflows = new ArrayList<>();

        try {
            workflows = getPendingWorkflows();
        } catch (SQLException e) {
            logger.error("Error getting pending workflows", e.getMessage());
        }

        final List<GetPendingWorkflowsOutput> toRecover = workflows;
        stopRequested = false;
        recoveryThread = new Thread(() -> startupRecoveryThread(toRecover),
                "RecoveryService-Thread");
        recoveryThread.setDaemon(true);
        recoveryThread.start();
        logger.info("Recovery service started");
    }

    /**
     * Stops the background recovery thread. This method will signal the thread to
     * stop and wait for it to complete.
     */
    public void stop() {
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
        logger.info("Recovery service stopped");
    }

    /**
     * Background thread method that attempts to recover local pending workflows on
     * startup. This method runs continuously until stop is requested or all
     * workflows are recovered.
     */
    private void startupRecoveryThread(List<GetPendingWorkflowsOutput> wToRecover) {
        try {
            List<GetPendingWorkflowsOutput> pendingWorkflows = new CopyOnWriteArrayList<>(
                    wToRecover);

            logger.info("Starting recovery thread " + pendingWorkflows.size());

            while (!stopRequested && !pendingWorkflows.isEmpty()) {
                try {
                    // Create a copy to iterate over to avoid concurrent modification
                    List<GetPendingWorkflowsOutput> currentPending = new ArrayList<>(
                            pendingWorkflows);

                    for (GetPendingWorkflowsOutput pendingWorkflow : currentPending) {
                        if (stopRequested) {
                            break;
                        }
                        recoverWorkflow(pendingWorkflow);
                        pendingWorkflows.remove(pendingWorkflow);
                    }
                } catch (WorkflowFunctionNotFoundException e) {
                    logger.debug(
                            "Workflow function not found during recovery, retrying in 1 second",
                            e);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        logger.info("Recovery thread interrupted during sleep");
                        break;
                    }
                } catch (Exception e) {
                    logger.error("Exception encountered when recovering workflows", e);
                    throw e;
                }
            }

            if (!stopRequested && pendingWorkflows.isEmpty()) {
                logger.info("All pending workflows recovered successfully");
            }

        } catch (Exception e) {
            logger.error("Unexpected error during workflow recovery", e);
        } finally {
            logger.info("Exiting recovery thread ");
        }
    }

    /**
     * Recovers a single workflow.
     *
     * @param pendingWorkflow
     *            the workflow to recover
     */
    private void recoverWorkflow(GetPendingWorkflowsOutput pendingWorkflow) {
        logger.info("Recovery executing workflow " + pendingWorkflow.getWorkflowUuid());
        dbosExecutor.executeWorkflowById(pendingWorkflow.getWorkflowUuid());
    }
}
