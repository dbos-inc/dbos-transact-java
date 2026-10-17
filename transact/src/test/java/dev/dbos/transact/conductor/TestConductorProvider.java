package dev.dbos.transact.conductor;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Stands in for dev.dbos:transact-enterprise in this module's tests, so a launch with a Conductor
 * key or on DBOS Cloud finds a provider. It connects to nothing, and records the options each
 * connection was created with.
 */
public final class TestConductorProvider implements ConductorProvider {
  private static final List<ConductorOptions> created = new CopyOnWriteArrayList<>();

  /** The options of every connection created so far, oldest first. */
  public static List<ConductorOptions> created() {
    return List.copyOf(created);
  }

  public static void reset() {
    created.clear();
  }

  @Override
  public ConductorConnection create(
      DBOSExecutor executor, SystemDatabase systemDatabase, ConductorOptions options) {
    created.add(options);
    return new ConductorConnection() {
      @Override
      public void start() {}

      @Override
      public void stop() {}
    };
  }
}
