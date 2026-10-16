package dev.dbos.transact.conductor;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.execution.DBOSExecutor;

/**
 * Builds the Conductor client for a DBOS instance. DBOS Transact finds the implementation with
 * {@link java.util.ServiceLoader}; the {@code dev.dbos:transact-enterprise} artifact registers it
 * in {@code META-INF/services/dev.dbos.transact.conductor.ConductorProvider}.
 *
 * <p>DBOS requires exactly one implementation when a Conductor key is set in {@code DBOSConfig} or
 * when running in DBOS Cloud, and fails at launch without one.
 */
public interface ConductorProvider {
  /**
   * Builds a Conductor client. DBOS calls this during launch, once the system database is open, and
   * then starts the returned connection.
   */
  ConductorConnection create(
      DBOSExecutor executor, SystemDatabase systemDatabase, ConductorOptions options);
}
