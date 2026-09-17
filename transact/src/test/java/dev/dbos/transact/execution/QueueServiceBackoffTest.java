package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.database.SystemDatabase;

import java.sql.SQLException;
import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * How a poll moves the queue's polling interval. The interesting cases are all arithmetic on the
 * multiplier, so they are exercised through {@link QueueService#nextBackoffFactor} rather than
 * through a live queue, whose dequeue latency is far too timing-dependent to assert on.
 */
public class QueueServiceBackoffTest {

  private static final Duration INTERVAL = Duration.ofMillis(200);

  private static final boolean CONTENDED = true;
  private static final boolean NOT_CONTENDED = false;

  private static SQLException wrapped(String sqlState) {
    // Contention reaches the listener wrapped by dbRetry, which is how it arrives in production.
    return new SQLException("contended", sqlState);
  }

  @Test
  @DisplayName("contention doubles the interval toward the 120s ceiling")
  public void contentionEscalatesToTheCeiling() {
    double factor = 1.0;
    for (int poll = 0; poll < 20; poll++) {
      factor = QueueService.nextBackoffFactor(factor, CONTENDED, INTERVAL);
    }

    assertEquals(600.0, factor, "120s over a 200ms interval");
    assertEquals(120_000L, (long) (factor * INTERVAL.toMillis()));
  }

  @Test
  @DisplayName("a genuine error leaves the cadence to decay, never escalating it")
  public void errorsDecayRatherThanBackOff() {
    // Back the queue off first, so a frozen factor is distinguishable from a decaying one.
    double factor = 1.0;
    for (int poll = 0; poll < 20; poll++) {
      factor = QueueService.nextBackoffFactor(factor, CONTENDED, INTERVAL);
    }
    double afterContention = factor;

    factor = QueueService.nextBackoffFactor(factor, NOT_CONTENDED, INTERVAL);
    assertTrue(
        factor < afterContention,
        "a genuine error must not hold the queue at the interval contention earned");
    assertEquals(afterContention * 0.9, factor);
  }

  @Test
  @DisplayName("the interval decays back to the base and stops there")
  public void decayFloorsAtTheBaseInterval() {
    double factor = 40.0;
    for (int poll = 0; poll < 100; poll++) {
      factor = QueueService.nextBackoffFactor(factor, NOT_CONTENDED, INTERVAL);
    }

    assertEquals(1.0, factor, "decay must not poll more often than the queue asked for");
  }

  @Test
  @DisplayName("both contention codes count as contention; nothing else does")
  public void classificationDecidesWhetherToBackOff() {
    // A NOWAIT claim losing the lock, and a SERIALIZABLE dequeue losing the race.
    assertTrue(SystemDatabase.isContentionError(wrapped("55P03")));
    assertTrue(SystemDatabase.isContentionError(wrapped("40001")));
    assertTrue(SystemDatabase.isContentionError(new RuntimeException(wrapped("40001"))));

    // A deadlock is class 40 too, but nothing expects it, so it is a real error here.
    assertFalse(SystemDatabase.isContentionError(wrapped("40P01")));
    assertFalse(SystemDatabase.isContentionError(new SQLException("no state")));
    assertFalse(SystemDatabase.isContentionError(new RuntimeException("boom")));
  }

  @Test
  @DisplayName("a queue polling slower than the ceiling is never backed off at all")
  public void slowQueuesAreNotSpedUp() {
    var slowInterval = Duration.ofMinutes(5);

    // The cap floors at 1.0 rather than going below it, which would poll more often on contention.
    assertEquals(1.0, QueueService.nextBackoffFactor(1.0, CONTENDED, slowInterval));
  }
}
