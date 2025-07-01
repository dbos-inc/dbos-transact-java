package dev.dbos.transact.queue;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static java.lang.Math.max;

public class QueueService {

    Logger logger = LoggerFactory.getLogger(QueueService.class);

    private SystemDatabase systemDatabase ;
    private DBOSExecutor dbosExecutor;
    private volatile boolean running = false ;
    private Thread workerThread;
    private QueueRegistry queueRegistry ;

    public QueueService(SystemDatabase systemDatabase) {
        systemDatabase = systemDatabase ;
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
        while(running) {

            List<Queue> queuesList = queueRegistry.getAllQueuesSnapshot();

            for( Queue queue : queuesList) {

                try {

                    List<String> workflowIds = systemDatabase.getAndStartQueuedWorkflows(queue, Constants.DEFAULT_EXECUTORID, Constants.DEFAULT_APP_VERSION) ;

                    for (String id : workflowIds) {
                        dbosExecutor.executeWorkflowById(id);
                    }


                } catch(Exception e) {

                    logger.error("Error executing queued workflow", e) ;
                }


            }

            pollingInterval = max(minPollingInterval, pollingInterval * 0.9) ;
            randomSleep = (int)(pollingInterval * (0.95 + 0.1 * Math.random()));

            try {
                Thread.sleep(randomSleep);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("QueuesPollThread interrupted while sleeping");
                running = false ;
            }

        }

        logger.info("QueuesPolThread has ended. Exiting") ;

    }

    public synchronized void start() {

        if (running) {
            logger.info("QueuesPollThread is already running.");
            return;
        }
        running = true;
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
                workerThread.join(5000); // Wait up to 5 seconds for the thread to finish gracefully
                if (workerThread.isAlive()) {
                    logger.warn("QueuePollThread did not stop gracefully. Interrupting...");
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
}
