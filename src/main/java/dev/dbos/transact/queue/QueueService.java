package dev.dbos.transact.queue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import static java.lang.Math.max;

public class QueueService {

    Logger logger = LoggerFactory.getLogger(QueueService.class);

    private SystemDatabase systemDatabase ;
    private DBOSExecutor dbosExecutor;
    private volatile boolean running = false ;
    private Thread workerThread;
    private QueueRegistry queueRegistry ;
    private CountDownLatch shutdownLatch;

    public QueueService(SystemDatabase systemDatabase) {
        this.systemDatabase = systemDatabase ;
        queueRegistry = new QueueRegistry();
    }

    public void setDbosExecutor(DBOSExecutor dbosExecutor) {
        this.dbosExecutor = dbosExecutor;
        dbosExecutor.setQueueService(this);
    }

    public void register(Queue queue) {
        queueRegistry.register(queue.getName(), queue);
    }

    private void pollForWorkflows() {
        logger.info("PollQueuesThread started ....") ;

        double pollingInterval = 1.0 ;
        double minPollingInterval = 1.0 ;
        double maxPollingInterval = 120.0 ;
        int randomSleep = 0;

        try {

            while (running) {

                /* randomSleep = (int) (pollingInterval * (0.95 + 0.1 * Math.random())); */

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.error("QueuesPollThread interrupted while sleeping");
                    running = false;
                }

                if (!running) {
                    // check again after sleep
                    break;
                }

                List<Queue> queuesList = queueRegistry.getAllQueuesSnapshot();

                logger.info("Got queues " + queuesList.size()) ;

                for (Queue queue : queuesList) {

                    try {

                        List<String> workflowIds = systemDatabase.getAndStartQueuedWorkflows(queue, Constants.DEFAULT_EXECUTORID, Constants.DEFAULT_APP_VERSION);

                        logger.info("Got workflows " + workflowIds.size());

                        for (String id : workflowIds) {
                            logger.debug("Submitting queued workflow for execution " + id);
                            dbosExecutor.executeWorkflowById(id);
                        }


                    } catch (Exception e) {

                        logger.error("Error executing queued workflow", e);
                    }


                }

                pollingInterval = max(minPollingInterval, pollingInterval * 0.9);

            }

        } finally {
            shutdownLatch.countDown();
            logger.info("QueuesPolThread has ended. Exiting");
        }

    }

    public synchronized void start() {

        if (running) {
            logger.info("QueuesPollThread is already running.");
            return;
        }
        running = true;
        shutdownLatch = new CountDownLatch(1);
        workerThread = new Thread(this::pollForWorkflows, "QueuesPollThread");
        workerThread.setDaemon(true);
        workerThread.start();
        logger.info("QueuesPollThread started.");


    }

    public synchronized void stop() {

        if (!running) {
            logger.info("QueuesPollThread is not running.");
            return;
        }
        running = false;
        if (workerThread != null) {
            try {
                workerThread.join(100);
                // Adding a latch so stop is absolute and there is no race condition for tests
                shutdownLatch.await(); // timeout ?
                if (workerThread.isAlive()) {
                    logger.warn("QueuePollThread did not stop gracefully. It might be stuck. Interrupting...");
                    workerThread.interrupt(); // Interrupt if it's still alive after join
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt(); // Restore interrupt status
                logger.warn( "Interrupted QueuesPollThread", e);
            } finally {
                workerThread = null;
            }
        }
        logger.info("QueuePollThread stopped.");
    }

    public synchronized  boolean isStopped() {
        // If the workerThread reference is null, it implies it hasn't started or has been fully cleaned up.
        if (workerThread == null) {
            return true;
        }
        // The most definitive check: if the latch has counted down to zero, the worker's run() method has completed.
        // We also check !workerThread.isAlive() as a final confirmation.
        return shutdownLatch != null && shutdownLatch.getCount() == 0 && !workerThread.isAlive();
    }
}
