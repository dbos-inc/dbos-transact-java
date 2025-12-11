package dev.dbos.transact.execution;

import static java.lang.Math.max;
import static java.lang.Math.min;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.Queue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueService {

  private static final Logger logger = LoggerFactory.getLogger(QueueService.class);

  private List<Queue> queues;

  private SystemDatabase systemDatabase;
  private DBOSExecutor dbosExecutor;
  private volatile boolean running = false;
  private volatile boolean paused = false;
  private Thread workerThread;
  private CountDownLatch shutdownLatch;
  private double speedup = 1.0;

  public QueueService(DBOSExecutor dbosExecutor, SystemDatabase systemDatabase) {
    this.systemDatabase = systemDatabase;
    this.dbosExecutor = dbosExecutor;
  }

  public void setSpeedupForTest() {
    speedup = 0.01;
  }

  @SuppressWarnings("deprecation") // Thread.currentThread().getId()
  private void pollForWorkflows() {
    logger.debug("PollQueuesThread started {}", Thread.currentThread().getId());

    double pollingIntervalSec = 1.0;
    double minPollingIntervalSec = 1.0;
    double maxPollingIntervalSec = 120.0;
    double randomSleepFactor = 0;

    try {

      while (running) {

        randomSleepFactor = (0.95 + ThreadLocalRandom.current().nextDouble(0.1));

        try {
          Thread.sleep((long) (randomSleepFactor * pollingIntervalSec * 1000 * speedup));
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          logger.error("QueuesPollThread interrupted while sleeping");
          running = false;
        }

        if (!running) {
          // check again after sleep
          break;
        }

        if (paused) {
          continue;
        }

        for (Queue queue : queues) {
          try {
            if (queue.partitionedEnabled()) {
              var partitions = systemDatabase.getQueuePartitions(queue.name());
              for (var partition : partitions) {
                var workflowIds =
                    systemDatabase.getAndStartQueuedWorkflows(
                        queue, dbosExecutor.executorId(), dbosExecutor.appVersion(), partition);
                for (var workflowId : workflowIds) {
                  logger.debug(
                      "Starting workflow {} from partition {} of queue {}",
                      workflowId,
                      partition,
                      queue.name());
                  dbosExecutor.executeWorkflowById(workflowId, false, true);
                }
              }
            } else {
              var workflowIds =
                  systemDatabase.getAndStartQueuedWorkflows(
                      queue, dbosExecutor.executorId(), dbosExecutor.appVersion(), null);
              for (var workflowId : workflowIds) {
                logger.debug("Starting workflow {} from queue {}", workflowId, queue.name());
                dbosExecutor.executeWorkflowById(workflowId, false, true);
              }
            }
          } catch (Exception e) {
            pollingIntervalSec = min(maxPollingIntervalSec, pollingIntervalSec * 2);
            logger.error("Error executing queued workflow", e);
          }
        }

        pollingIntervalSec = max(minPollingIntervalSec, pollingIntervalSec * 0.9);
      }

    } finally {
      shutdownLatch.countDown();
      logger.debug("QueuesPollThread {} has ended. Exiting", Thread.currentThread().getId());
    }
  }

  public synchronized void pause() {
    this.paused = true;
  }

  public synchronized void unpause() {
    this.paused = false;
  }

  public synchronized void start(List<Queue> queues) {

    this.queues = queues;

    if (running) {
      logger.warn("QueuesPollThread is already running.");
      return;
    }

    running = true;
    shutdownLatch = new CountDownLatch(1);
    workerThread = new Thread(this::pollForWorkflows, "QueuesPollThread");
    workerThread.setDaemon(true);
    workerThread.start();
    logger.debug("QueuesPollThread started.");
  }

  public synchronized void stop() {
    logger.debug("stop() called");

    if (!running) {
      logger.warn("QueuesPollThread is not running.");
      return;
    }
    running = false;
    if (workerThread != null) {
      try {
        workerThread.join(100);
        // Adding a latch so stop is absolute and there is no race condition for
        // tests
        shutdownLatch.await(); // timeout ?
        if (workerThread.isAlive()) {
          logger.warn(
              "QueuePollThread did not stop gracefully. It might be stuck. Interrupting...");
          workerThread.interrupt(); // Interrupt if it's still alive after join
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt(); // Restore interrupt status
        logger.warn("Interrupted QueuesPollThread", e);
      } finally {
        workerThread = null;
      }
    }

    this.queues = null;
    logger.debug("QueuePollThread stopped.");
  }

  public synchronized boolean isStopped() {
    // If the workerThread reference is null, it implies it hasn't started or has
    // been fully cleaned
    // up.
    if (workerThread == null) {
      return true;
    }
    // The most definitive check: if the latch has counted down to zero, the
    // worker's run() method
    // has completed.
    // We also check !workerThread.isAlive() as a final confirmation.
    return shutdownLatch != null && shutdownLatch.getCount() == 0 && !workerThread.isAlive();
  }
}
