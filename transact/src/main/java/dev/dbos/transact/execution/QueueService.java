package dev.dbos.transact.execution;

import dev.dbos.transact.Constants;
import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.workflow.Queue;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueService implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(QueueService.class);
  private static final Duration MAX_POLLING_INTERVAL = Duration.ofSeconds(120);

  // Matches the backoff and scaleback factors every other SDK's queue runner uses.
  private static final double BACKOFF_GROWTH_FACTOR = 2.0;
  private static final double BACKOFF_SCALEBACK_FACTOR = 0.9;
  private static final long DB_QUEUE_SUPERVISOR_INTERVAL_SEC = 1;
  // One claim round trip plus dispatching what it claimed, with room for a slow database.
  private static final Duration PAUSE_DRAIN_TIMEOUT = Duration.ofSeconds(30);

  private final AtomicReference<ScheduledExecutorService> execServiceRef = new AtomicReference<>();

  // `paused` and `passesInFlight` are guarded together, so a pass cannot see the flag clear and
  // then slip in behind a pause() that has already found nothing in flight.
  private final ReentrantLock passLock = new ReentrantLock();
  private final Condition passesDrained = passLock.newCondition();
  private boolean paused = false;
  private int passesInFlight = 0;
  // In-flight passes whose own thread is blocked in pause(). They cannot finish until it returns,
  // so pause() does not wait for them: its own, or another pass pausing at the same time.
  private int passesPausing = 0;
  // Set while this thread runs a pass.
  private final ThreadLocal<Boolean> inPass = new ThreadLocal<>();

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

  /**
   * Stops claiming queued workflows and transitioning delayed ones, and returns once no pass that
   * could still do either is in flight. A row enqueued after this returns stays put until {@link
   * #unpause()}; workflows already dispatched keep running.
   *
   * <p>Waits at most 30 seconds, and gives up early if interrupted (restoring the interrupt) or if
   * {@link #unpause()} supersedes it. Called from inside a pass, it does not wait for that pass,
   * which checks the flag again before its next claim.
   */
  public void pause() {
    boolean fromPass = Boolean.TRUE.equals(inPass.get());
    passLock.lock();
    try {
      paused = true;
      if (fromPass) {
        passesPausing++;
        // Another pause() may be waiting on this pass, which will not finish while it waits here.
        passesDrained.signalAll();
      }
      long remaining = PAUSE_DRAIN_TIMEOUT.toNanos();
      while (paused && passesInFlight > passesPausing) {
        if (remaining <= 0) {
          logger.warn(
              "Queue service paused, but {} poll pass(es) still in flight after {}",
              passesInFlight - passesPausing,
              PAUSE_DRAIN_TIMEOUT);
          return;
        }
        remaining = passesDrained.awaitNanos(remaining);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      if (fromPass) {
        passesPausing--;
      }
      passLock.unlock();
    }
  }

  public void unpause() {
    passLock.lock();
    try {
      paused = false;
      // A pause() still draining has been superseded; release it.
      passesDrained.signalAll();
    } finally {
      passLock.unlock();
    }
  }

  /**
   * Registers a pass that may claim or transition rows, unless the service is paused.
   *
   * @return false if paused, in which case the caller must do nothing and not call {@link
   *     #endPass()}
   */
  private boolean beginPass() {
    passLock.lock();
    try {
      if (paused) return false;
      passesInFlight++;
      inPass.set(Boolean.TRUE);
      return true;
    } finally {
      passLock.unlock();
    }
  }

  private void endPass() {
    passLock.lock();
    try {
      inPass.remove();
      if (--passesInFlight == 0) {
        passesDrained.signalAll();
      }
    } finally {
      passLock.unlock();
    }
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
     * Sweeps every partition once, in a random order and within the queue-wide worker budget,
     * skipping any partition a peer is already dequeuing.
     *
     * <p>A fixed order would let whichever partitions the database returned first spend a shared
     * budget before the rest were reached, starving them for as long as they stayed behind in the
     * ordering.
     *
     * <p>Contention listing the partitions is left to propagate: it is not scoped to any one
     * partition, so it backs the whole queue off.
     */
    void sweepPartitions() {
      var partitions = new ArrayList<>(systemDatabase.getQueuePartitions(queue.name()));
      Collections.shuffle(partitions, ThreadLocalRandom.current());
      // Snapshot the running count once and carry this sweep's own claims forward in `claimed`:
      // dispatch is asynchronous, so re-reading per partition would not yet see what the
      // partitions before it just claimed, and every partition would spend the same budget.
      long running = dbosExecutor.queueActiveCount(queue.name());
      long claimed = 0;
      for (var partition : partitions) {
        if (workerBudget(running + claimed) <= 0) break;
        try {
          claimed += processPartition(partition, running + claimed);
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

    /**
     * Room left under this worker's queue-wide concurrency limit, given how many of its workflows
     * are already running or claimed. Unbounded when only a per-partition worker limit is set,
     * which the dequeue enforces within each partition instead.
     */
    private long workerBudget(long running) {
      var workerConcurrency = queue.resolveLimits().workerConcurrency();
      if (workerConcurrency == null) return Long.MAX_VALUE;
      return Math.max(0, workerConcurrency - running);
    }

    /**
     * Dequeues and dispatches one partition of the queue, or the whole queue when {@code partition}
     * is null.
     *
     * @param running how many of this queue's workflows this worker is already running or has
     *     claimed earlier in this sweep
     * @return how many workflows this call claimed
     */
    int processPartition(@Nullable String partition, long running) {
      var partitionLog = Objects.requireNonNullElse(partition, "<null>");
      // The claim and the dispatch of what it claimed are one pass: pause() waits for both.
      if (!beginPass()) return 0;
      try {
        // The two worker-scoped limits count different things: workerConcurrency bounds the queue
        // across every partition, partitionWorkerConcurrency bounds this partition alone.
        long partitionLocalRunningCount =
            partition == null ? running : dbosExecutor.queueActiveCount(queue.name(), partition);
        var workflowIds =
            systemDatabase.startQueuedWorkflows(
                queue, executorId, appVersion, partition, running, partitionLocalRunningCount);
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
        return workflowIds.size();
      } finally {
        endPass();
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

        if (queue.isPartitioned()) {
          sweepPartitions();
        } else {
          processPartition(null, dbosExecutor.queueActiveCount(queue.name()));
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

  // Package-private so a test can drive one transition directly.
  void transitionDelayedWorkflows() {
    if (!beginPass()) return;
    try {
      systemDatabase.transitionDelayedWorkflows();
    } catch (Throwable e) {
      logger.error("Exception transitioning delayed workflows", e);
    } finally {
      endPass();
    }
  }
}
