package dev.dbos.transact.database;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Caps how many DB-backed polling reads may run concurrently against the system database pool.
 *
 * <p>Wait operations -- awaiting a workflow result, recv, getEvent, and reading a stream --
 * re-query on an interval. They hold a connection only for the query itself, not across the wait,
 * so a single waiter is cheap; a few thousand of them re-querying on the same interval are not.
 * Each tick they all reach for the pool at once, and the control plane (enqueue and dequeue, status
 * writes, recovery, cancellation) queues behind them. This bounds that burst so some of the pool is
 * always reachable by work that is not polling.
 *
 * <p>Only polling reads acquire a permit; control-plane work goes to the pool directly. A
 * non-positive limit disables the limiter and {@link #acquire()} becomes free.
 */
public final class PollingLimiter {

  /** A held permit, released when closed. */
  public interface Permit extends AutoCloseable {
    @Override
    void close();
  }

  private static final Permit DISABLED = () -> {};

  private final int limit;
  private final Semaphore semaphore;

  /**
   * @param limit maximum concurrent polling reads; non-positive disables the limiter
   */
  public PollingLimiter(int limit) {
    this.limit = limit;
    this.semaphore = limit > 0 ? new Semaphore(limit, true) : null;
  }

  /** The configured limit; non-positive if the limiter is disabled. */
  public int limit() {
    return limit;
  }

  /** Whether this limiter caps anything. */
  public boolean isEnabled() {
    return semaphore != null;
  }

  /** Permits currently available, or {@link Integer#MAX_VALUE} when disabled. */
  public int availablePermits() {
    return semaphore == null ? Integer.MAX_VALUE : semaphore.availablePermits();
  }

  /**
   * Acquire a permit, blocking until one is free.
   *
   * <p>Uninterruptible, but the interrupt status is preserved: a caller interrupted while queued
   * here proceeds with its read and observes the interrupt at the next blocking point in its poll
   * loop, which is where these loops already handle it. Interrupting a queued acquirer instead
   * would push that handling into every polling read for a wait of at most one query.
   */
  public Permit acquire() {
    if (semaphore == null) {
      return DISABLED;
    }
    semaphore.acquireUninterruptibly();
    var released = new AtomicBoolean(false);
    // Idempotent: a second close would hand back a permit that was never issued, raising the cap
    // for the life of the process.
    return () -> {
      if (released.compareAndSet(false, true)) {
        semaphore.release();
      }
    };
  }
}
