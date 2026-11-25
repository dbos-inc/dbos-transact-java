package dev.dbos.transact.workflow;

/** Whether a workflow is auto-upgraded to the latest application version. */
public enum AutoUpgrade {
  /** Follows {@code DBOSConfig.autoUpgrade}. */
  DEFAULT,
  ENABLED,
  DISABLED
}
