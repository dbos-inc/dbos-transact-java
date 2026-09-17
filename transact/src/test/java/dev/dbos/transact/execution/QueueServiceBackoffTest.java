package dev.dbos.transact.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.database.SystemDatabase;
import dev.dbos.transact.exceptions.DBOSSystemDatabaseException;

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

  private static final boolean BACK_OFF = true;
  private static final boolean DO_NOT_BACK_OFF = false;

  private static SQLException wrapped(String sqlState) {
    // Contention reaches the listener wrapped by dbRetry, which is how it arrives in production.
    return new SQLException("contended", sqlState);
  }

  @Test
  @DisplayName("contention doubles the interval toward the 120s ceiling")
  public void contentionEscalatesToTheCeiling() {
    double factor = 1.0;
    for (int poll = 0; poll < 20; poll++) {
      factor = QueueService.nextBackoffFactor(factor, BACK_OFF, INTERVAL);
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
      factor = QueueService.nextBackoffFactor(factor, BACK_OFF, INTERVAL);
    }
    double afterContention = factor;

    factor = QueueService.nextBackoffFactor(factor, DO_NOT_BACK_OFF, INTERVAL);
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
      factor = QueueService.nextBackoffFactor(factor, DO_NOT_BACK_OFF, INTERVAL);
    }

    assertEquals(1.0, factor, "decay must not poll more often than the queue asked for");
  }

  @Test
  @DisplayName("a lost row lock does not back the queue off; a conflict does")
  public void onlyAConflictAsksToBackOff() {
    // 55P03: the peer holding those rows commits in milliseconds, so the next tick is enough.
    // This is the whole of #512 -- backing off here left a queue idle for 25.5s in the report.
    assertFalse(QueueService.shouldBackOff(wrapped("55P03")));
    assertFalse(QueueService.shouldBackOff(new RuntimeException(wrapped("55P03"))));

    // 40001: a peer already committed, and under a shared budget that can keep happening.
    assertTrue(QueueService.shouldBackOff(wrapped("40001")));
    assertTrue(QueueService.shouldBackOff(new RuntimeException(wrapped("40001"))));

    // A deadlock is class 40 too, but nothing expects it, so it is a real error here.
    assertFalse(QueueService.shouldBackOff(wrapped("40P01")));
    assertFalse(QueueService.shouldBackOff(new SQLException("no state")));
    assertFalse(QueueService.shouldBackOff(new RuntimeException("boom")));
  }

  @Test
  @DisplayName("both codes still count as contention for skipping a partition")
  public void bothCodesSkipAPartition() {
    // The partition sweep skips on either, which is what Python does: a peer winning one
    // partition says nothing about the rest, whichever way it won.
    assertTrue(SystemDatabase.isContentionError(wrapped("55P03")));
    assertTrue(SystemDatabase.isContentionError(wrapped("40001")));
    assertFalse(SystemDatabase.isContentionError(wrapped("40P01")));
  }

  @Test
  @DisplayName("a queue polling slower than the ceiling is never backed off at all")
  public void slowQueuesAreNotSpedUp() {
    var slowInterval = Duration.ofMinutes(5);

    // The cap floors at 1.0 rather than going below it, which would poll more often on contention.
    assertEquals(1.0, QueueService.nextBackoffFactor(1.0, BACK_OFF, slowInterval));
  }

  @Test
  @DisplayName("raising a backed-off queue's interval reclamps the multiplier")
  public void aLongerIntervalReclampsTheMultiplier() {
    // Backed off to the 120s ceiling on a 200ms interval.
    double factor = 1.0;
    for (int poll = 0; poll < 20; poll++) {
      factor = QueueService.nextBackoffFactor(factor, BACK_OFF, INTERVAL);
    }
    assertEquals(600.0, factor);

    // An operator raises the interval to 60s. Decaying from 600 would poll every 10 hours and
    // take ~60 polls to work off, so the multiplier has to be reclamped, not merely decayed.
    var raised = Duration.ofSeconds(60);
    assertEquals(2.0, QueueService.nextBackoffFactor(factor, DO_NOT_BACK_OFF, raised));
    assertEquals(2.0, QueueService.nextBackoffFactor(factor, BACK_OFF, raised));
  }

  @Test
  @DisplayName("the decision survives dbRetry's wrapper")
  public void theDecisionSeesThroughTheSystemDatabaseWrapper() {
    // dbRetry hands failures back as DBOSSystemDatabaseException. If that ever stopped chaining,
    // every classifier below it would quietly stop matching and a lost lock would back the queue
    // off -- reintroducing #512. SystemDatabaseExceptionTest guards the wrapper; this guards the
    // decision that depends on it.
    assertFalse(QueueService.shouldBackOff(new DBOSSystemDatabaseException(wrapped("55P03"))));
    assertTrue(QueueService.shouldBackOff(new DBOSSystemDatabaseException(wrapped("40001"))));
    assertFalse(QueueService.shouldBackOff(new DBOSSystemDatabaseException(wrapped("58030"))));
  }
}
