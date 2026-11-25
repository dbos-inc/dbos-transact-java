package dev.dbos.transact.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.spi.AutoUpgradePass;
import dev.dbos.transact.spi.TestEnterpriseProvider.RecordingAutoUpgradePass;
import dev.dbos.transact.utils.LogCapture;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import ch.qos.logback.classic.Level;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

/** The auto-upgrade loop on its own, against a pass that records every call. */
// These tests count the loop's warnings, in a log every loop in this JVM writes to.
@ResourceLock(AutoUpgradeLoopTest.LOOP_LOG)
public class AutoUpgradeLoopTest {
  static final String LOOP_LOG = "AutoUpgradeLoop log";

  private static RecordingAutoUpgradePass recordingPass() {
    return new RecordingAutoUpgradePass(null, Duration.ZERO);
  }

  static void awaitRuns(RecordingAutoUpgradePass upgradePass, int count)
      throws InterruptedException {
    var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (upgradePass.runs.size() < count) {
      assertTrue(System.nanoTime() < deadline, "the pass ran " + upgradePass.runs.size() + "x");
      Thread.sleep(10);
    }
  }

  @Test
  public void aFailingPassDoesNotStopTheLoop() throws Exception {
    var upgradePass = recordingPass();
    upgradePass.runError = new RuntimeException("boom");
    var loop = new AutoUpgradeLoop(upgradePass, Duration.ofMillis(10));
    try (var logs = new LogCapture(AutoUpgradeLoop.class)) {
      loop.start();
      try {
        awaitRuns(upgradePass, 3);
      } finally {
        loop.stop();
      }
      assertFalse(logs.messages(Level.WARN, "Auto-upgrade pass failed").isEmpty());
    }
    assertFalse(loop.isAlive());
  }

  @Test
  public void stopLetsAPassInFlightFinishBeforeClosing() throws Exception {
    var upgradePass = recordingPass();
    upgradePass.blockUntilStopped = true;
    var loop = new AutoUpgradeLoop(upgradePass, Duration.ofMillis(10));
    loop.start();
    awaitRuns(upgradePass, 1);

    loop.stop();

    assertFalse(loop.isAlive());
    // The pass saw the stop signal, returned, and only then was closed.
    assertEquals(List.of("run-start", "run-end", "close"), upgradePass.events);
  }

  @Test
  public void stopClosesAPassThatOutlastsTheWait() throws Exception {
    var events = new CopyOnWriteArrayList<String>();
    var release = new CountDownLatch(1);
    var running = new CountDownLatch(1);
    var upgradePass =
        new AutoUpgradePass() {
          @Override
          public int run(BooleanSupplier stopRequested) {
            events.add("run-start");
            running.countDown();
            try {
              release.await();
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            events.add("run-end");
            return 0;
          }

          @Override
          public void close() {
            events.add("close");
          }
        };
    var loop = new AutoUpgradeLoop(upgradePass, Duration.ofMillis(10), Duration.ofMillis(100));
    try (var logs = new LogCapture(AutoUpgradeLoop.class)) {
      loop.start();
      assertTrue(running.await(10, TimeUnit.SECONDS));

      loop.stop();

      assertEquals(List.of("run-start", "close"), events);
      assertEquals(
          1, logs.messages(Level.WARN, "Auto-upgrade thread did not exit within timeout").size());
    } finally {
      release.countDown();
    }
  }

  @Test
  public void aCloseThatFailsIsOnlyAWarning() throws Exception {
    var upgradePass = recordingPass();
    upgradePass.closeError = new IllegalStateException("close failed");
    var loop = new AutoUpgradeLoop(upgradePass, Duration.ofMillis(10));
    try (var logs = new LogCapture(AutoUpgradeLoop.class)) {
      loop.start();
      awaitRuns(upgradePass, 1);

      loop.stop();

      assertEquals(1, logs.messages(Level.WARN, "Auto-upgrade pass did not close cleanly").size());
    }
    assertFalse(loop.isAlive());
    assertEquals(1, upgradePass.closes.get());
  }

  @Test
  public void thePassRunsAtOnceThenOncePerInterval() throws Exception {
    var upgradePass = recordingPass();
    var loop = new AutoUpgradeLoop(upgradePass, Duration.ofHours(1));
    loop.start();
    try {
      // The first pass does not wait out the interval.
      awaitRuns(upgradePass, 1);
      Thread.sleep(100);
      assertEquals(1, upgradePass.runs.size());
    } finally {
      loop.stop();
    }
    assertFalse(loop.isAlive());
    assertEquals(1, upgradePass.closes.get());
  }
}
