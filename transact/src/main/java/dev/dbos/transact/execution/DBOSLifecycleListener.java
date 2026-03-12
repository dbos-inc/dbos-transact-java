package dev.dbos.transact.execution;

import dev.dbos.transact.DBOS;

/**
 * For registering callbacks that hear about `DBOS.launch()` and `DBOS.shutdown()`. At this point,
 * DBOS is ready to run workflows, and no additional registrations are allowed.
 */
public interface DBOSLifecycleListener {
  /** Called from within DBOS.launch, after workflow processing is allowed */
  void dbosLaunched(DBOS dbos);

  /** Called from within DBOS.shutdown, before workflow processing is stopped */
  void dbosShutDown();
}
