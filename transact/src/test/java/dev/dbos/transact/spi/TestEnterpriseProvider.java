package dev.dbos.transact.spi;

import dev.dbos.transact.conductor.ConductorConnection;
import dev.dbos.transact.conductor.ConductorOptions;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;

/**
 * Stands in for dev.dbos:transact-enterprise in this module's tests, so a launch with a Conductor
 * key, on DBOS Cloud or with suspend mode on finds a provider. Its connections connect to nothing,
 * and it records the options each was created with. Its suspend mode counts how often it is built
 * and suspends nothing.
 */
public final class TestEnterpriseProvider implements EnterpriseProvider {
  private static final List<ConductorOptions> created = new CopyOnWriteArrayList<>();
  private static final AtomicInteger suspendBackendsCreated = new AtomicInteger();

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
}
