package dev.dbos.transact.conductor;

import dev.dbos.transact.suspend.Suspend;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.jspecify.annotations.Nullable;

/**
 * Stands in for dev.dbos:transact-enterprise in this module's tests, so a launch with a Conductor
 * key, on DBOS Cloud or with suspend mode on finds a provider. Its connections connect to nothing,
 * and it records the options each was created with. Its suspend mode counts how often it is built
 * and parks nothing.
 */
public final class TestConductorProvider implements ConductorProvider {
  private static final List<ConductorOptions> created = new CopyOnWriteArrayList<>();
  private static final AtomicInteger suspendsCreated = new AtomicInteger();

  /** The options of every connection created so far, oldest first. */
  public static List<ConductorOptions> created() {
    return List.copyOf(created);
  }

  /** How many times suspend mode has been built so far. */
  public static int suspendsCreated() {
    return suspendsCreated.get();
  }

  @Override
  public ConductorConnection createConductor(ConductorEngine engine, ConductorOptions options) {
    created.add(options);
    return new ConductorConnection() {
      @Override
      public void start() {}

      @Override
      public void stop() {}
    };
  }

  @Override
  public Suspend createSuspend(ConductorEngine engine) {
    suspendsCreated.incrementAndGet();
    return new Suspend() {
      @Override
      public @Nullable Object recvSuspend(
          String workflowId,
          int stepId,
          int timeoutStepId,
          @Nullable String topic,
          Duration timeout,
          String ownerXid) {
        throw new UnsupportedOperationException("the test provider parks nothing");
      }

      @Override
      public Duration sleepSuspend(
          String workflowId, int functionId, Duration duration, String ownerXid) {
        throw new UnsupportedOperationException("the test provider parks nothing");
      }

      @Override
      public void transitionSuspendedWorkflows() {}
    };
  }
}
