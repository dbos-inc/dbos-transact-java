package dev.dbos.transact.execution;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.Queue;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueService {

  private static final Logger logger = LoggerFactory.getLogger(QueueService.class);

  private final AtomicReference<ScheduledExecutorService> scheduler = new AtomicReference<>();
  private final AtomicBoolean paused = new AtomicBoolean(false);

  private SystemDatabase systemDatabase;
  private DBOSExecutor dbosExecutor;
  private double speedup = 1.0;

  public QueueService(DBOSExecutor dbosExecutor, SystemDatabase systemDatabase) {
    this.systemDatabase = systemDatabase;
    this.dbosExecutor = dbosExecutor;
  }

  public void setSpeedupForTest() {
    speedup = 0.01;
  }

  public synchronized void pause() {
    paused.set(true);
  }

  public synchronized void unpause() {
    paused.set(false);
  }

  public void start(List<Queue> queues, Set<String> listenQueues) {
    if (this.scheduler.get() == null) {
      var scheduler = Executors.newScheduledThreadPool(4);
      if (this.scheduler.compareAndSet(null, scheduler)) {
        startQueueListeners(queues, listenQueues);
      }
    }
  }

  public void stop() {
    var scheduler = this.scheduler.getAndSet(null);
    if (scheduler != null) {
      var notRun = scheduler.shutdownNow();
      logger.debug("Shutting down queue service. Tasks not run {}", notRun.size());
    }
  }

  public boolean isStopped() {
    return this.scheduler.get() == null;
  }

  private void startQueueListeners(List<Queue> queues, Set<String> listenQueues) {
    logger.debug("startQueueListeners");

    final var executorId = dbosExecutor.executorId();
    final var appVersion = dbosExecutor.appVersion();
    final Duration minPollingInterval = Duration.ofSeconds(1);
    final Duration maxPollingInterval = Duration.ofSeconds(120);

    for (var _queue : queues) {

      var listening =
          _queue.name().equals(Constants.DBOS_INTERNAL_QUEUE)
              || listenQueues.isEmpty()
              || listenQueues.contains(_queue.name());
      if (!listening) {
        continue;
      }

      var task =
          new Runnable() {
            final Queue queue = _queue;
            Duration pollingInterval = Duration.ofSeconds(1);

            public void schedule() {
              var randomSleepFactor = 0.95 + ThreadLocalRandom.current().nextDouble(0.1);
              var delayMs = (long) (randomSleepFactor * pollingInterval.toMillis() * speedup);
              var localScheduler = scheduler.get();
              if (localScheduler != null) {
                localScheduler.schedule(this, delayMs, TimeUnit.MILLISECONDS);
              }
            }

            private void processPartition(String partition) {
              var partitionLog = Objects.requireNonNullElse(partition, "<null>");
              var workflowIds =
                  systemDatabase.getAndStartQueuedWorkflows(
                      queue, executorId, appVersion, partition);
              if (workflowIds.size() > 0) {
                logger.debug(
                    "Retrieved {} workflows from {} partition of queue {}",
                    workflowIds.size(),
                    partitionLog,
                    queue.name());
              }
              for (var workflowId : workflowIds) {
                logger.debug(
                    "Starting workflow {} from {} partition of queue {}",
                    workflowId,
                    partitionLog,
                    queue.name());
                dbosExecutor.executeWorkflowById(workflowId, false, true);
              }
            }

            @Override
            public void run() {
              // if scheduler service isn't running, the queue service was stopped so don't start
              // the workflow or schedule the next execution
              if (scheduler.get() == null) {
                return;
              }

              try {
                if (paused.get()) {
                  pollingInterval = minPollingInterval;
                  return;
                }

                if (queue.partitionedEnabled()) {
                  var partitions = systemDatabase.getQueuePartitions(queue.name());
                  for (var partition : partitions) {
                    processPartition(partition);
                  }
                } else {
                  processPartition(null);
                }

                pollingInterval = Duration.ofMillis((long) (pollingInterval.toMillis() * 0.9));
                pollingInterval =
                    pollingInterval.compareTo(minPollingInterval) >= 0
                        ? pollingInterval
                        : minPollingInterval;
              } catch (Exception e) {
                logger.error("Error executing queued workflow(s) for queue {}", queue.name(), e);
                pollingInterval = pollingInterval.multipliedBy(2);
                pollingInterval =
                    pollingInterval.compareTo(maxPollingInterval) <= 0
                        ? pollingInterval
                        : maxPollingInterval;
              } finally {
                this.schedule();
              }
            }
          };

      task.schedule();
    }
  }
}
