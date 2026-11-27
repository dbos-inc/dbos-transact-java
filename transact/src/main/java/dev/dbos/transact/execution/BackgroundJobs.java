package dev.dbos.transact.execution;

import dev.dbos.transact.database.BackgroundJobLock;
import dev.dbos.transact.database.SystemDatabase;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Runs jobs once per interval across all of an application version's processes: whichever takes
// their lock runs them, holding it up to the hold duration before retaking it.
final class BackgroundJobs {

  private static final Logger logger = LoggerFactory.getLogger(BackgroundJobs.class);

  static final String THREAD_NAME = "dbos-background-jobs";
  static final Duration INTERVAL = Duration.ofSeconds(1);
  // How long a holder keeps one lock transaction before retaking the lock: a peer can win it then.
  static final Duration HOLD = Duration.ofSeconds(60);

  // One holder per application version, so the latest code always runs them; they tolerate
  // overlap.
  static String lockName(String appVersion) {
    return "background_jobs." + appVersion;
  }

  private final SystemDatabase systemDatabase;
  private final String lockName;
  private final Duration interval;
  private final Map<String, Runnable> jobs;
  private final BackgroundLease lease;
  private final CountDownLatch stop;
  private final Duration hold;
  private final Duration idleTimeout;
  private final Thread thread;

  BackgroundJobs(
      SystemDatabase systemDatabase,
      String lockName,
      Duration interval,
      Map<String, Runnable> jobs,
      BackgroundLease lease,
      CountDownLatch stop,
      Duration hold,
      Duration idleTimeout) {
    this.systemDatabase = systemDatabase;
    this.lockName = lockName;
    this.interval = interval;
    this.jobs = new LinkedHashMap<>(jobs);
    this.lease = lease;
    this.stop = stop;
    this.hold = hold;
    this.idleTimeout = idleTimeout;
    this.thread = new Thread(this::run, THREAD_NAME);
    this.thread.setDaemon(true);
  }

  String lockName() {
    return lockName;
  }

  Thread thread() {
    return thread;
  }

  void start() {
    thread.start();
  }

  /** Interrupts the loop, once its stop signal is set, instead of waiting for it. */
  void stopNow() {
    thread.interrupt();
  }

  private void run() {
    try {
      hold();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      lease.lose();
    }
  }

  private void hold() throws InterruptedException {
    while (stop.getCount() > 0) {
      boolean held = false;
      try (var lock = systemDatabase.backgroundJobLock(lockName, idleTimeout)) {
        if (lock.acquired()) {
          lease.renew();
          runWhileHeld(lock);
          held = true;
        } else {
          lease.lose();
        }
      } catch (InterruptedException e) {
        throw e;
      } catch (Throwable e) {
        lease.lose();
        if (stop.getCount() > 0) {
          logger.warn("Exception taking the {} lock: {}", lockName, e.toString());
        } else {
          logger.debug("The {} lock ended with the shutdown", lockName, e);
        }
      }
      // The holder tries again at once and so keeps the jobs; the rest poll for them.
      long waitNanos =
          held ? 0 : (long) (interval.toNanos() * ThreadLocalRandom.current().nextDouble(0.9, 1.1));
      if (stop.await(waitNanos, TimeUnit.NANOSECONDS)) {
        return;
      }
    }
  }

  private void runWhileHeld(BackgroundJobLock lock) throws Exception {
    long holdUntil = System.nanoTime() + hold.toNanos();
    while (true) {
      // Timed from the pass's start, so slow jobs can't shorten the interval.
      long started = System.nanoTime();
      for (var job : jobs.entrySet()) {
        try {
          job.getValue().run();
        } catch (Throwable e) {
          // An Error too: the loop must outlive any one job's failure.
          if (stop.getCount() > 0) {
            logger.warn("Exception in background job {}", job.getKey(), e);
          } else {
            logger.debug("Background job {} ended with the shutdown", job.getKey(), e);
          }
        }
      }
      long remaining = interval.toNanos() - (System.nanoTime() - started);
      if (stop.await(Math.max(0, remaining), TimeUnit.NANOSECONDS)) {
        return;
      }
      if (System.nanoTime() - holdUntil >= 0) {
        return;
      }
      // In place, so the lock is never free mid-hold.
      lock.renew();
    }
  }
}
