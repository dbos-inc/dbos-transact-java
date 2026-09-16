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

  // Matches the backoff and scaleback factors every other SDK's queue runner uses.
  private static final double BACKOFF_GROWTH_FACTOR = 2.0;
  private static final double BACKOFF_SCALEBACK_FACTOR = 0.9;
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

  // Package-private, with its sweep and dispatch, so a test can drive one poll directly.
  class QueueListenerTask implements Runnable {

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

    /**
     * Claims from each partition in turn, skipping any a peer is already dequeuing.
     *
     * <p>Contention listing the partitions is left to propagate: it is not scoped to any one
     * partition, so it backs the whole queue off.
     */
    void sweepPartitions() {
      for (var partition : systemDatabase.getQueuePartitions(queue.name())) {
        try {
          processPartition(partition);
        } catch (Exception e) {
          // Skip just this partition, no queue-wide backoff -- deliberately including 40001,
          // which would back off from a non-partitioned dequeue. The other partitions are
          // unrelated rows this poll can still claim.
          if (!SystemDatabase.isContentionError(e)) {
            throw e;
          }
          logger.debug(
              "Partition {} of queue {} is contended; skipping it", partition, queue.name());
        }
      }
    }

    void processPartition(String partition) {
      var partitionLog = Objects.requireNonNullElse(partition, "<null>");
      if (!paused.get()) {
        long localRunningCount = dbosExecutor.queueActiveCount(queue.name(), partition);
        var workflowIds =
            systemDatabase.startQueuedWorkflows(
                queue, executorId, appVersion, partition, localRunningCount);
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
          try {
            dbosExecutor.executeWorkflowById(workflowId);
          } catch (Exception e) {
            // A failed dispatch must not strand the rest of the batch, and its failure is not
            // the dequeue contention the poll loop would read it as. A workflow out of attempts is
            // dead-lettered here, and says so by throwing.
            logger.error(
                "Error starting workflow {} from {} partition of queue {}",
                workflowId,
                partitionLog,
                queue.name(),
                e);
          }
        }
      }
    }

    /**
     * Reloads a database-backed queue's configuration so changes take effect without a restart.
     *
     * @return false if the queue no longer exists, in which case this listener stops
     */
    boolean refreshQueue() {
      Optional<Queue> refreshed;
      try {
        refreshed = systemDatabase.findQueue(queue.name());
      } catch (Exception e) {
        // Keep polling on the configuration already in hand. A row that fails to load is a
        // reason to try again next poll, not to stop dequeuing this queue for good.
        logger.warn(
            "Could not reload queue {}; keeping its current configuration", queue.name(), e);
        return true;
      }
      if (refreshed.isEmpty()) {
        dbListeningQueues.remove(queue.name());
        return false;
      }
      queue = refreshed.get();
      return true;
    }

    @Override
    public void run() {
      if (execServiceRef.get() == null) return;

      // Rescheduling is the only thing keeping this queue polling, so nothing between here and
      // the finally may escape it -- including reloading the queue's own configuration, which
      // reaches dbRetry and so can throw for a conflict or any non-transient failure.
      boolean reschedule = true;
      boolean backoffRequested = false;
      try {
        if (dynamic && !refreshQueue()) {
          reschedule = false;
          return;
        }

        if (queue.partitioningEnabled()) {
          sweepPartitions();
        } else {
          processPartition(null);
        }
      } catch (Exception e) {
        backoffRequested = shouldBackOff(e);
        if (backoffRequested) {
          logger.debug("Lost a dequeue race on queue {}; backing off", queue.name());
        } else if (SystemDatabase.isLockNotAvailable(e)) {
          logger.debug("A peer is mid-dequeue on queue {}; retrying next poll", queue.name());
        } else {
          logger.error("Error executing queued workflow(s) for queue {}", queue.name(), e);
        }
      } finally {
        if (reschedule) {
          backoffFactor =
              nextBackoffFactor(backoffFactor, backoffRequested, queue.pollingInterval());
          this.schedule();
        }
      }
    }
  }

  /**
   * Whether a failed poll should lengthen the polling interval.
   *
   * <p>A lost row lock (55P03) should not: the peer holding those rows commits in milliseconds, so
   * the obstruction is gone by the next tick and a failed poll costs one read that NOWAIT made fail
   * immediately. There is no load to shed, and escalating abandons a queue that has work waiting --
   * which is what #512 measured, 25.5 s of idle time from a 60 s lock hold against a 40 ms
   * uncontended control.
   *
   * <p>A serialization failure (40001) should: a peer already committed, and under a shared budget
   * that can keep happening, so damping spreads the contenders out. Python draws the line in the
   * same place; TypeScript and Go back off on both codes.
   *
   * <p>Everything else is a genuine error, which backing off would not help either -- see the catch
   * in {@code run()}.
   */
  static boolean shouldBackOff(Throwable failure) {
    return !SystemDatabase.isLockNotAvailable(failure) && SystemDatabase.isContentionError(failure);
  }

  /**
   * The polling multiplier for the next poll: grown when a poll asks to back off, decayed
   * otherwise, and clamped into range either way.
   *
   * <p>The clamp is not only for growth. A queue's polling interval can be raised while it is
   * backed off, which leaves a multiplier earned against the old interval far too large for the new
   * one, and decay alone would take hours of polls to work it off. Every other SDK reclamps for the
   * same reason after reloading the queue's configuration.
   *
   * @param current the multiplier in force
   * @param backoffRequested whether this poll asked to lengthen the interval
   * @param pollingInterval the queue's base interval, which the multiplier scales
   * @return the multiplier for the next poll, within [1.0, {@link #MAX_POLLING_INTERVAL}]
   */
  static double nextBackoffFactor(
      double current, boolean backoffRequested, Duration pollingInterval) {
    double cap =
        Math.max(1.0, (double) MAX_POLLING_INTERVAL.toMillis() / pollingInterval.toMillis());
    double next = current * (backoffRequested ? BACKOFF_GROWTH_FACTOR : BACKOFF_SCALEBACK_FACTOR);
    return Math.min(Math.max(next, 1.0), cap);
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
