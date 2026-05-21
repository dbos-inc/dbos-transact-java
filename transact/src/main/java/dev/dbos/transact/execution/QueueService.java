package dev.dbos.transact.execution;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.Queue;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueService implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(QueueService.class);

  private final AtomicReference<ScheduledExecutorService> execServiceRef = new AtomicReference<>();
  private final AtomicBoolean paused = new AtomicBoolean(false);

  private final SystemDatabase systemDatabase;
  private final DBOSExecutor dbosExecutor;
  private Set<String> listenQueues;
  private double speedup = 1.0;

  public QueueService(DBOSExecutor dbosExecutor, SystemDatabase systemDatabase) {
    this.systemDatabase = systemDatabase;
    this.dbosExecutor = dbosExecutor;
  }

  public void setSpeedupForTest() {
    speedup = 0.01;
  }

  public void pause() {
    paused.set(true);
  }

  public void unpause() {
    paused.set(false);
  }

  public void start(Collection<Queue> queues, Set<String> listenQueues) {
    if (this.execServiceRef.get() == null) {
      var procCount = Runtime.getRuntime().availableProcessors();
      var scheduler = Executors.newScheduledThreadPool(procCount);
      if (this.execServiceRef.compareAndSet(null, scheduler)) {
        this.listenQueues = listenQueues;
        startQueueListeners(queues);
      }
    }
  }

  @Override
  public void close() {
    var scheduler = this.execServiceRef.getAndSet(null);
    if (scheduler != null) {
      var notRun = scheduler.shutdownNow();
      logger.debug("Shutting down queue service. {} task(s) not run.", notRun.size());
    }
  }

  public boolean isStopped() {
    return this.execServiceRef.get() == null;
  }

  private void startQueueListeners(Collection<Queue> queues) {
    logger.debug("startQueueListeners");

    final var executorId = dbosExecutor.executorId();
    final var appVersion = dbosExecutor.appVersion();
    final Duration minPollingInterval = Duration.ofSeconds(1);
    final Duration maxPollingInterval = Duration.ofSeconds(120);

    var execService = execServiceRef.get();
    if (execService != null) {
      execService.scheduleAtFixedRate(this::transitionDelayedWorkflows, 1, 1, TimeUnit.SECONDS);
      execService.scheduleAtFixedRate(
          this::pollDynamicQueues, 0, DB_QUEUE_SUPERVISOR_INTERVAL_SEC, TimeUnit.SECONDS);
    }

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
            Duration pollingInterval = queue.pollingInterval();

            public void schedule() {
              var randomSleepFactor = 0.95 + ThreadLocalRandom.current().nextDouble(0.1);
              var delayMs = (long) (randomSleepFactor * pollingInterval.toMillis() * speedup);
              var execService = execServiceRef.get();
              if (execService != null) {
                execService.schedule(this, delayMs, TimeUnit.MILLISECONDS);
              }
            }

            private void processPartition(String partition) {
              var partitionLog = Objects.requireNonNullElse(partition, "<null>");
              if (!paused.get()) {
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
            }

            @Override
            public void run() {
              // if scheduler service isn't running, the queue service was stopped so don't start
              // the workflow or schedule the next execution
              if (execServiceRef.get() == null) {
                return;
              }

              try {
                if (queue.partitioningEnabled()) {
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

  // ── DB-backed (dynamic) queue support ────────────────────────────────────

  private static final long DB_QUEUE_SUPERVISOR_INTERVAL_SEC = 5;
  private static final Duration DB_MIN_POLLING_INTERVAL = Duration.ofSeconds(1);
  private static final Duration DB_MAX_POLLING_INTERVAL = Duration.ofSeconds(120);

  private final Set<String> dbListeningQueues = ConcurrentHashMap.newKeySet();

  private void pollDynamicQueues() {
    try {
      if (execServiceRef.get() == null) return;

      var dbQueues = systemDatabase.listQueues();
      var dbQueueNames = dbQueues.stream().map(Queue::name).collect(Collectors.toSet());

      // Stop listeners for queues that have been deleted from the DB.
      dbListeningQueues.removeIf(name -> !dbQueueNames.contains(name));

      for (var queue : dbQueues) {
        startDynamicQueueListenerIfNeeded(queue);
      }
    } catch (Exception e) {
      logger.error("pollDynamicQueues failed", e);
    }
  }

  private void startDynamicQueueListenerIfNeeded(Queue queue) {
    var listening =
        queue.name().equals(Constants.DBOS_INTERNAL_QUEUE)
            || listenQueues.isEmpty()
            || listenQueues.contains(queue.name());
    if (!listening) return;

    if (!dbListeningQueues.add(queue.name())) return;

    var execService = execServiceRef.get();
    if (execService == null) return;

    final var executorId = dbosExecutor.executorId();
    final var appVersion = dbosExecutor.appVersion();

    var task =
        new Runnable() {
          Queue queue;
          Duration pollingInterval;

          public void schedule() {
            var randomSleepFactor = 0.95 + ThreadLocalRandom.current().nextDouble(0.1);
            var delayMs = (long) (randomSleepFactor * pollingInterval.toMillis() * speedup);
            var svc = execServiceRef.get();
            if (svc != null) {
              svc.schedule(this, delayMs, TimeUnit.MILLISECONDS);
            }
          }

          private void processPartition(String partition) {
            var partitionLog = Objects.requireNonNullElse(partition, "<null>");
            if (!paused.get()) {
              var workflowIds =
                  systemDatabase.getAndStartQueuedWorkflows(
                      queue, executorId, appVersion, partition);
              if (!workflowIds.isEmpty()) {
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
          }

          @Override
          public void run() {
            if (execServiceRef.get() == null) return;
            if (!dbListeningQueues.contains(queue.name())) return;

            // Reload config from DB so live updates (concurrency, rate limits, etc.) take effect.
            queue = systemDatabase.findQueue(queue.name()).orElse(queue);

            try {
              if (queue.partitioningEnabled()) {
                var partitions = systemDatabase.getQueuePartitions(queue.name());
                for (var partition : partitions) {
                  processPartition(partition);
                }
              } else {
                processPartition(null);
              }

              pollingInterval = Duration.ofMillis((long) (pollingInterval.toMillis() * 0.9));
              pollingInterval =
                  pollingInterval.compareTo(DB_MIN_POLLING_INTERVAL) >= 0
                      ? pollingInterval
                      : DB_MIN_POLLING_INTERVAL;
            } catch (Exception e) {
              logger.error("Error executing queued workflow(s) for queue {}", queue.name(), e);
              pollingInterval = pollingInterval.multipliedBy(2);
              pollingInterval =
                  pollingInterval.compareTo(DB_MAX_POLLING_INTERVAL) <= 0
                      ? pollingInterval
                      : DB_MAX_POLLING_INTERVAL;
            } finally {
              this.schedule();
            }
          }
        };

    task.queue = queue;
    task.pollingInterval = queue.pollingInterval();
    task.schedule();
  }

  // ── Shared helpers ────────────────────────────────────────────────────────

  private void transitionDelayedWorkflows() {
    if (!paused.get()) {
      try {
        systemDatabase.transitionDelayedWorkflows();
      } catch (Throwable e) {
        logger.error("Exception transitioning delayed workflows", e);
      }
    }
  }
}
