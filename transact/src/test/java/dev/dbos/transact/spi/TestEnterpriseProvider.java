package dev.dbos.transact.spi;

import dev.dbos.transact.conductor.ConductorConnection;
import dev.dbos.transact.conductor.ConductorOptions;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.jspecify.annotations.Nullable;

/**
 * Stands in for dev.dbos:transact-enterprise in this module's tests, so a launch with a Conductor
 * key, on DBOS Cloud, with suspend mode on or with a workflow enrolled in auto-upgrade finds a
 * provider. Its connections connect to nothing, and it records the options each was created with.
 * Its suspend mode counts how often it is built and suspends nothing. Its auto-upgrade passes move
 * nothing and record every call.
 */
public final class TestEnterpriseProvider implements EnterpriseProvider {
  private static final List<ConductorOptions> created = new CopyOnWriteArrayList<>();
  private static final AtomicInteger suspendBackendsCreated = new AtomicInteger();
  private static final List<RecordingAutoUpgradePass> autoUpgradePasses =
      new CopyOnWriteArrayList<>();

  /** An auto-upgrade pass that moves nothing and records every call. */
  public static final class RecordingAutoUpgradePass implements AutoUpgradePass {
    public final EnterpriseEngine engine;
    public final Duration grace;
    public final List<BooleanSupplier> runs = new CopyOnWriteArrayList<>();
    public final List<String> events = new CopyOnWriteArrayList<>();
    public final AtomicInteger closes = new AtomicInteger();
    // Run as the pass is closed, before the close is counted.
    public volatile Runnable onClose = () -> {};
    public volatile RuntimeException runError;
    public volatile RuntimeException closeError;
    public volatile boolean blockUntilStopped;

    public RecordingAutoUpgradePass(EnterpriseEngine engine, Duration grace) {
      this.engine = engine;
      this.grace = grace;
    }

    @Override
    public int run(BooleanSupplier stopRequested) {
      events.add("run-start");
      runs.add(stopRequested);
      if (runError != null) {
        throw runError;
      }
      while (blockUntilStopped && !stopRequested.getAsBoolean()) {
        try {
          Thread.sleep(5);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new IllegalStateException(e);
        }
      }
      events.add("run-end");
      return 0;
    }

    @Override
    public void close() {
      events.add("close");
      onClose.run();
      closes.incrementAndGet();
      if (closeError != null) {
        throw closeError;
      }
    }
  }

  /** Every auto-upgrade pass built so far, oldest first. */
  public static List<RecordingAutoUpgradePass> autoUpgradePasses() {
    return List.copyOf(autoUpgradePasses);
  }


  /**
   * Runs inside every connection's stop(), whichever test's shutdown it is; a test that sets it to
   * hold its own shutdown there checks the thread it runs on, and resets only the hook it set.
   */
  public static volatile Runnable onStop = () -> {};

  /** The options of every connection created so far, oldest first. */
  public static List<ConductorOptions> created() {
    return List.copyOf(created);
  }

  /** How many times suspend mode has been built so far. */
  public static int suspendBackendsCreated() {
    return suspendBackendsCreated.get();
  }

  @Override
  public ConductorConnection createConductor(EnterpriseEngine engine, ConductorOptions options) {
    created.add(options);
    return new ConductorConnection() {
      @Override
      public void start() {}

      @Override
      public void stop() {
        onStop.run();
      }
    };
  }

  @Override
  public SuspendBackend createSuspendBackend(EnterpriseEngine engine) {
    suspendBackendsCreated.incrementAndGet();
    return new SuspendBackend() {
      @Override
      public @Nullable Object recvSuspend(
          String workflowId,
          int stepId,
          int timeoutStepId,
          @Nullable String topic,
          Duration timeout,
          String ownerXid) {
        throw new UnsupportedOperationException("the test provider suspends nothing");
      }

      @Override
      public Duration sleepSuspend(
          String workflowId, int functionId, Duration duration, String ownerXid) {
        throw new UnsupportedOperationException("the test provider suspends nothing");
      }

      @Override
      public void transitionSuspendedWorkflows() {}
    };
  }

  @Override
  public AutoUpgradePass createAutoUpgradePass(EnterpriseEngine engine, Duration grace) {
    var upgradePass = new RecordingAutoUpgradePass(engine, grace);
    autoUpgradePasses.add(upgradePass);
    return upgradePass;
  }
}
