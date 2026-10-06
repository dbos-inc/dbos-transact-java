package dev.dbos.transact.internal;

import java.sql.SQLException;

/**
 * Checkpoints a transactional step factory keeps in its own database, outside the system database.
 * Each factory registers one through {@link DBOSIntegration#registerStepCheckpointStore}, so DBOS
 * can drop a workflow's checkpoints when it discards that workflow's history.
 *
 * <p>This interface is <strong>not part of the primary public API</strong> and may change without
 * notice.
 */
@FunctionalInterface
public interface StepCheckpointStore {

  /**
   * Deletes a workflow's checkpoints from {@code fromStepId} on. Deleting checkpoints that are
   * already gone is not an error.
   *
   * @param workflowId the workflow whose checkpoints to delete
   * @param fromStepId the first step ID to delete; 0 deletes them all
   */
  void deleteCheckpoints(String workflowId, int fromStepId) throws SQLException;
}
