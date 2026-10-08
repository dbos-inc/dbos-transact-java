package dev.dbos.transact.database;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.TimeUnit;

/**
 * A reading of the system database's clock, carried forward by this JVM's monotonic clock.
 *
 * <p>Every executor sharing a system database shares its clock, so workflow deadlines are set and
 * compared on it. Between readings, time is measured with {@link System#nanoTime()}, which no
 * setting of this JVM's wall clock moves: a host whose clock is off waits exactly as long as one
 * whose clock is right.
 *
 * @param epochMs the database's clock when it was read, in epoch milliseconds
 * @param nanoTime {@link System#nanoTime()} when the reading arrived
 */
public record DatabaseTime(long epochMs, long nanoTime) {

  /** A reading of the column {@code column}, taken as it arrives. */
  public static DatabaseTime read(ResultSet rs, String column) throws SQLException {
    return new DatabaseTime(rs.getLong(column), System.nanoTime());
  }

  /** The database's clock now, as this reading and the time since it give it. */
  public long nowEpochMs() {
    return epochMs + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - nanoTime);
  }

  /** How long until {@code instant} on the database's clock: negative once it has passed. */
  public Duration until(Instant instant) {
    return Duration.ofMillis(instant.toEpochMilli() - nowEpochMs());
  }
}
