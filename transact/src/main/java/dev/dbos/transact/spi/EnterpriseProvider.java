package dev.dbos.transact.spi;

import dev.dbos.transact.conductor.ConductorConnection;
import dev.dbos.transact.conductor.ConductorOptions;

/**
 * Builds what {@code dev.dbos:transact-enterprise} provides to a DBOS instance: the Conductor
 * client and every other enterprise feature. It is the one entry point DBOS Transact finds with
 * {@link java.util.ServiceLoader}; the enterprise artifact registers it in {@code
 * META-INF/services/dev.dbos.transact.spi.EnterpriseProvider}, and its constructor refuses a DBOS
 * Transact of another minor version. It is not application API.
 *
 * <p>The enterprise artifact implements this interface, and an enterprise build runs against any
 * later DBOS Transact build of the same minor version. So within a minor a method added here has a
 * default, for the enterprise builds that predate it; at a minor, methods change in place.
 *
 * <p>DBOS requires exactly one implementation when a Conductor key is set in {@code DBOSConfig},
 * when running in DBOS Cloud, or when an enterprise feature such as {@code suspendMode} is turned
 * on, and fails at launch without one.
 */
public interface EnterpriseProvider {
  /**
   * Builds a Conductor client, without connecting. DBOS calls this during launch, once the system
   * database is open and before queues, the scheduler or lifecycle listeners start, and starts the
   * returned connection at the end of launch.
   *
   * @param engine the DBOS instance, as the Conductor client may use it
   * @param options how to connect
   * @return the connection, not yet started
   */
  ConductorConnection createConductor(EnterpriseEngine engine, ConductorOptions options);

  /**
   * Builds suspend mode. DBOS calls this during launch when {@code suspendMode} is set, once the
   * system database is open, and starts calling {@link SuspendBackend#transitionSuspendedWorkflows}
   * once queues have started.
   *
   * @param engine the DBOS instance, as suspend mode may use it
   * @return suspend mode for the instance
   */
  SuspendBackend createSuspendBackend(EnterpriseEngine engine);
}
