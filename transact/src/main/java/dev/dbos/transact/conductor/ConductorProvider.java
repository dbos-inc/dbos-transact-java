package dev.dbos.transact.conductor;

import dev.dbos.transact.suspend.Suspend;

/**
 * Builds what {@code dev.dbos:transact-enterprise} provides to a DBOS instance: the Conductor
 * client and every other enterprise feature. It is the one entry point DBOS Transact finds with
 * {@link java.util.ServiceLoader}; the enterprise artifact registers it in {@code
 * META-INF/services/dev.dbos.transact.conductor.ConductorProvider}. It is named for Conductor, the
 * first feature to use it, and keeps that name so that DBOS still finds an enterprise artifact of
 * another version and can refuse it with a clear error.
 *
 * <p>DBOS requires exactly one implementation when a Conductor key is set in {@code DBOSConfig},
 * when running in DBOS Cloud, or when an enterprise feature such as {@code suspendMode} is turned
 * on, and fails at launch without one.
 */
public interface ConductorProvider {
  /**
   * Builds a Conductor client, without connecting. DBOS calls this during launch, once the system
   * database is open and before queues, the scheduler or lifecycle listeners start, and starts the
   * returned connection at the end of launch.
   *
   * @param engine the DBOS instance, as the Conductor client may use it
   * @param options how to connect
   * @return the connection, not yet started
   */
  ConductorConnection createConductor(ConductorEngine engine, ConductorOptions options);

  /**
   * Builds suspend mode. DBOS calls this during launch when {@code suspendMode} is set, once the
   * system database is open, and starts calling {@link Suspend#transitionSuspendedWorkflows} once
   * queues have started.
   *
   * @param engine the DBOS instance, as suspend mode may use it
   * @return suspend mode for the instance
   */
  Suspend createSuspend(ConductorEngine engine);
}
