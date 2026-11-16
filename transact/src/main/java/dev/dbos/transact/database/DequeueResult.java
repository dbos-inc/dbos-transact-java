package dev.dbos.transact.database;

import java.util.List;

import org.jspecify.annotations.Nullable;

/**
 * What one dequeue claimed, and why it claimed nothing when a limit left no room or a peer held the
 * claim's lock.
 *
 * @param workflowIds the workflows claimed
 * @param stopReason the reason the dequeue claimed nothing without looking for candidates, either
 *     the limit that left no room or {@link StopReason#CLAIM_LOCK}; null when it looked, whatever
 *     it found, or did not run because the queue service is paused
 */
public record DequeueResult(List<String> workflowIds, @Nullable StopReason stopReason) {

  /** A claim that found no room under {@code reason}. */
  public static DequeueResult stopped(StopReason reason) {
    return new DequeueResult(List.of(), reason);
  }

  /**
   * Why a dequeue claimed nothing: a limit it found no room under, checked in this order, or a
   * claim lock another worker held.
   */
  public enum StopReason {
    WORKER_CONCURRENCY,
    PARTITION_WORKER_CONCURRENCY,
    CLAIM_LOCK,
    RATE_LIMIT,
    PARTITION_RATE_LIMIT,
    CONCURRENCY,
    PARTITION_CONCURRENCY;

    /**
     * Whether no room is left under a limit every executor shares across the whole queue, so that
     * no other partition of it can be claimed this poll either.
     */
    public boolean isQueueWide() {
      return this == CONCURRENCY || this == RATE_LIMIT;
    }
  }
}
