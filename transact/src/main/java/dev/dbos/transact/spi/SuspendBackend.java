package dev.dbos.transact.spi;

import java.time.Duration;

import org.jspecify.annotations.Nullable;

/**
 * Suspend mode for {@code recv} and {@code sleep}: a waiting recv or a sleep suspends its workflow
 * in the database as SUSPENDED instead of holding a thread and an executor slot. A send on the
 * recv's topic, the recv's timeout, or the end of the sleep makes the workflow due, a background
 * pass re-enqueues it, and the replayed call returns.
 *
 * <p>The send side belongs to DBOS Transact, which wakes a suspended receiver on every send whether
 * or not suspend mode is on, so a sender without suspend mode, such as a {@code DBOSClient}, still
 * wakes a recv another process suspended.
 */
public interface SuspendBackend {
  /**
   * A {@code recv} that suspends its workflow if it would have to wait.
   *
   * <p>Returns the recorded message when the call is a replay, and the message that is already
   * waiting, or {@code null} once the timeout has passed, when there is no need to wait. Otherwise
   * suspends the workflow and throws {@link
   * dev.dbos.transact.exceptions.DBOSWorkflowSuspendedException}.
   *
   * @param workflowId the calling workflow
   * @param stepId the step ID the recv is recorded under
   * @param timeoutStepId the step ID the recv's timeout is recorded under
   * @param topic the topic to receive on, or {@code null} for none
   * @param timeout how long to wait for a message
   * @param ownerXid the token of the execution making the call, which must still own the workflow
   * @return the message, or {@code null} if none came in time
   * @throws dev.dbos.transact.exceptions.DBOSWorkflowSuspendedException once the workflow is
   *     suspended
   */
  @Nullable Object recvSuspend(
      String workflowId,
      int stepId,
      int timeoutStepId,
      @Nullable String topic,
      Duration timeout,
      String ownerXid);

  /**
   * A {@code sleep} that suspends its workflow if the end of the sleep is far off.
   *
   * <p>Checkpoints the sleep's end time. Returns the time left to sleep in the calling thread when
   * it is short, or already past, which is what the replay of a suspended sleep finds. Otherwise
   * suspends the workflow and throws {@link
   * dev.dbos.transact.exceptions.DBOSWorkflowSuspendedException}.
   *
   * @param workflowId the calling workflow
   * @param functionId the step ID the sleep is recorded under
   * @param duration how long to sleep
   * @param ownerXid the token of the execution making the call, which must still own the workflow
   * @return how long the caller should still sleep in its thread; zero or negative for none
   * @throws dev.dbos.transact.exceptions.DBOSWorkflowSuspendedException once the workflow is
   *     suspended
   */
  Duration sleepSuspend(String workflowId, int functionId, Duration duration, String ownerXid);

  /**
   * One pass of the background job that re-enqueues this application's SUSPENDED workflows whose
   * wake time has come: the recv's timeout, the end of the sleep, or zero, which a send on the
   * awaited topic sets. Each goes back on the queue it was on, or the internal queue if it had
   * none. DBOS calls this on a fixed delay in every process with suspend mode on; the
   * implementation decides which of them does the work.
   */
  void transitionSuspendedWorkflows();
}
