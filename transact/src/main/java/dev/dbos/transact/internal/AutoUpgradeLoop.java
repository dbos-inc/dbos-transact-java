package dev.dbos.transact.internal;

import dev.dbos.transact.spi.AutoUpgradePass;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Runs the auto-upgrade pass on a timer, from start to stop. */
public final class AutoUpgradeLoop {
  private static final Logger logger = LoggerFactory.getLogger(AutoUpgradeLoop.class);

  /** How long stop waits for a pass in flight. */
  static final Duration STOP_TIMEOUT = Duration.ofSeconds(5);

  private final AutoUpgradePass upgradePass;
  private final Duration interval;
  private final Duration stopTimeout;
  private final CountDownLatch stopRequested = new CountDownLatch(1);
  // The one stop signal every pass is given.
  private final BooleanSupplier stopSignal = () -> stopRequested.getCount() == 0;
  private final Thread thread;

  public AutoUpgradeLoop(AutoUpgradePass upgradePass, Duration interval) {
    this(upgradePass, interval, STOP_TIMEOUT);
  }

  AutoUpgradeLoop(AutoUpgradePass upgradePass, Duration interval, Duration stopTimeout) {
    this.upgradePass = upgradePass;
    this.interval = interval;
    this.stopTimeout = stopTimeout;
    this.thread = new Thread(this::run, "dbos-auto-upgrade");
    this.thread.setDaemon(true);
  }

  public AutoUpgradePass upgradePass() {
    return upgradePass;
  }

  public Duration interval() {
    return interval;
  }

  public boolean isAlive() {
    return thread.isAlive();
  }

  public void start() {
    logger.debug("Starting auto-upgrade thread, one pass every {}", interval);
    thread.start();
  }

  /** Stops the loop, waits a bounded time for a pass in flight, then closes the pass. */
  public void stop() {
    stopRequested.countDown();
    try {
      thread.join(stopTimeout.toMillis());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    if (thread.isAlive()) {
      logger.warn("Auto-upgrade thread did not exit within timeout");
    }
    try {
      upgradePass.close();
    } catch (Exception e) {
      logger.warn("Auto-upgrade pass did not close cleanly", e);
    }
  }

  private void run() {
    while (!stopSignal.getAsBoolean()) {
      try {
        upgradePass.run(stopSignal);
      } catch (Exception e) {
        logger.warn("Auto-upgrade pass failed", e);
      }
      try {
        if (stopRequested.await(waitNanos(), TimeUnit.NANOSECONDS)) {
          break;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      }
    }
  }

  // An interval too long for a long count of nanoseconds waits as long as one can.
  private long waitNanos() {
    try {
      return interval.toNanos();
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }
}
