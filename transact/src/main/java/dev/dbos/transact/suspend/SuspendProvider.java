package dev.dbos.transact.suspend;

/**
 * Builds suspend mode for a DBOS instance. DBOS Transact finds the implementation with {@link
 * java.util.ServiceLoader}; the {@code dev.dbos:transact-enterprise} artifact registers it in
 * {@code META-INF/services/dev.dbos.transact.suspend.SuspendProvider}.
 *
 * <p>DBOS requires exactly one implementation when {@code suspendMode} is set in {@code
 * DBOSConfig}, and fails at launch without one, or without an entitlement to suspend mode.
 */
public interface SuspendProvider {
  /**
   * Builds suspend mode. DBOS calls this during launch, once the system database is open, and
   * starts calling {@link Suspend#transitionSuspendedWorkflows} once queues have started.
   *
   * @param engine the DBOS instance, as suspend mode may use it
   * @return suspend mode for the instance
   */
  Suspend create(SuspendEngine engine);
}
