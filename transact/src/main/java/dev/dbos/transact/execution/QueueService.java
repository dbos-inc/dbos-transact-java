package dev.dbos.transact.execution;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.Queue;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
  private static final Duration MAX_POLLING_INTERVAL = Duration.ofSeconds(120);
  private static final long DB_QUEUE_SUPERVISOR_INTERVAL_SEC = 1;

  private final AtomicReference<ScheduledExecutorService> execServiceRef = new AtomicReference<>();
  private final AtomicBoolean paused = new AtomicBoolean(false);
  private final Set<String> dbListeningQueues = ConcurrentHashMap.newKeySet();
  private volatile Map<String, Queue> dynamicQueueMap = Map.of();

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

  public void start(Collection<Queue> staticQueues, Set<String> listenQueues) {
    if (this.execServiceRef.get() == null) {
      var procCount = Runtime.getRuntime().availableProcessors();
      var scheduler = Executors.newScheduledThreadPool(procCount);
      if (this.execServiceRef.compareAndSet(null, scheduler)) {
        this.listenQueues = listenQueues;
        scheduler.scheduleAtFixedRate(this::transitionDelayedWorkflows, 1, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(
            this::pollDynamicQueues, 0, DB_QUEUE_SUPERVISOR_INTERVAL_SEC, TimeUnit.SECONDS);
        for (var queue : staticQueues) {
          startQueueListenerIfNeeded(queue, false);
        }
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

  public Optional<Queue> findDynamicQueue(String queueName) {
    return Optional.ofNullable(dynamicQueueMap.get(queueName));
  }

  private boolean isListening(String queueName) {
    return queueName.equals(Constants.DBOS_INTERNAL_QUEUE)
        || listenQueues.isEmpty()
        || listenQueues.contains(queueName);
  }

  private void startQueueListenerIfNeeded(Queue queue, boolean dynamic) {
    if (!isListening(queue.name())) return;
    if (dynamic && !dbListeningQueues.add(queue.name())) return;
    if (execServiceRef.get() == null) return;

    new QueueListenerTask(queue, dynamic)
        .schedule(); // executor holds the reference via the scheduled future
  }

  // ── Dynamic queue supervisor ──────────────────────────────────────────────

  private void pollDynamicQueues() {
    try {
      if (execServiceRef.get() == null) return;

      var dbQueues = systemDatabase.listQueues();
      dynamicQueueMap =
          dbQueues.stream().collect(Collectors.toUnmodifiableMap(Queue::name, q -> q));
      if (logger.isDebugEnabled()) {
        logger.debug("pollDynamicQueues found {} queues", dbQueues.size());
        for (var q : dbQueues) {
          logger.debug(
              "  queue: {} concurrency: {} pollingInterval: {}",
              q.name(),
              q.concurrency(),
              q.pollingInterval());
        }
      }

      for (var queue : dbQueues) {
        if (dbosExecutor.findStaticQueue(queue.name()).isPresent()) {
          logger.warn(
              "Database-backed queue {} has the same name as a static queue; "
                  + "the static queue's configuration is being used and the database-backed queue is ignored.",
              queue.name());
          continue;
        }
        startQueueListenerIfNeeded(queue, true);
      }
    } catch (Exception e) {
      logger.error("pollDynamicQueues failed", e);
    }
  }

  // ── Queue listener task ───────────────────────────────────────────────────

  private class QueueListenerTask implements Runnable {

    Queue queue;
    double backoffFactor = 1.0;
    final boolean dynamic;
    final String executorId = dbosExecutor.executorId();
    final String appVersion = dbosExecutor.appVersion();

    QueueListenerTask(Queue queue, boolean dynamic) {
      this.queue = queue;
      this.dynamic = dynamic;
    }

    void schedule() {
      var randomSleepFactor = 0.95 + ThreadLocalRandom.current().nextDouble(0.1);
      var delayMs =
          (long) (randomSleepFactor * queue.pollingInterval().toMillis() * backoffFactor * speedup);
      var svc = execServiceRef.get();
      if (svc != null) {
        svc.schedule(this, delayMs, TimeUnit.MILLISECONDS);
      }
    }

    private void processPartition(String partition) {
      var partitionLog = Objects.requireNonNullElse(partition, "<null>");
      if (!paused.get()) {
        var workflowIds =
            systemDatabase.getAndStartQueuedWorkflows(queue, executorId, appVersion, partition);
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
      if (dynamic) {
        var refreshed = systemDatabase.findQueue(queue.name());
        if (refreshed.isEmpty()) {
          dbListeningQueues.remove(queue.name());
          return;
        }
        queue = refreshed.get();
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

        backoffFactor = Math.max(backoffFactor * 0.9, 1.0);
      } catch (Exception e) {
        logger.error("Error executing queued workflow(s) for queue {}", queue.name(), e);
        double maxFactor =
            (double) MAX_POLLING_INTERVAL.toMillis() / queue.pollingInterval().toMillis();
        backoffFactor = Math.min(backoffFactor * 2.0, maxFactor);
      } finally {
        this.schedule();
      }
    }
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
