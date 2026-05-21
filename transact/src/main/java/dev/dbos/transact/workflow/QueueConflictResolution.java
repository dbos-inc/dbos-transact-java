package dev.dbos.transact.workflow;

/**
 * Controls what happens when {@code registerQueue} is called for a queue that already exists in the
 * database.
 */
public enum QueueConflictResolution {
  /**
   * Overwrite the existing queue configuration unconditionally. Default for {@link
   * dev.dbos.transact.DBOSClient}.
   */
  ALWAYS_UPDATE,

  /** Leave the existing queue configuration unchanged. */
  NEVER_UPDATE,

  /**
   * Overwrite the existing queue configuration only if the running application version matches the
   * latest application version registered in the database. Default for {@link
   * dev.dbos.transact.DBOS}.
   */
  UPDATE_IF_LATEST_VERSION,
}
