package dev.dbos.transact.exceptions;

/**
 * {@code DBOSDelayPastLatestInstantException} is thrown when a workflow is enqueued with a relative
 * delay that carries the current time past the largest instant epoch milliseconds can hold, so its
 * release time cannot be stored. The delay is refused rather than wrapped, which would release the
 * workflow at once.
 *
 * <p>Whether a delay does depends on the clock, so a parent workflow starting a child records this
 * refusal at its step, as it does a {@link DBOSQueueDuplicatedException}, and a replay throws it
 * again rather than deciding against a later clock.
 *
 * <p>An {@link IllegalArgumentException}, as every other refusal of a duration DBOS cannot store
 * is.
 */
public class DBOSDelayPastLatestInstantException extends IllegalArgumentException {
  // Pinned: this exception is recorded in the system database, and a replay must read it back.
  private static final long serialVersionUID = 1L;

  public DBOSDelayPastLatestInstantException() {
    super("delay must not end past the latest instant 64-bit epoch milliseconds can hold");
  }
}
