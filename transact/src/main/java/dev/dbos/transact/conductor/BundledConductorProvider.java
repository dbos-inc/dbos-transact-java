package dev.dbos.transact.conductor;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;

/** Provides the Conductor client that ships in this artifact. */
public final class BundledConductorProvider implements ConductorProvider {
  @Override
  public ConductorConnection create(
      DBOSExecutor executor, SystemDatabase systemDatabase, ConductorOptions options) {
    var builder = new Conductor.Builder(executor, systemDatabase, options.apiKey());
    if (options.appName() != null) {
      builder.appName(options.appName());
    }
    if (options.domain() != null) {
      builder.domain(options.domain());
    }
    return builder.build();
  }
}
