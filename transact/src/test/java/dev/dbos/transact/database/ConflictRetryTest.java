package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The bounded replay {@link SystemDatabase#retryOnSerializationError} offers callers whose work is
 * safe to re-run. {@link SystemDatabase#dbRetry} deliberately does not replay conflicts, so every
 * caller that needs one opts in; see the classification tests in {@link SystemDatabaseTest}.
 */
public class ConflictRetryTest {

  private static SQLException sqlState(String state) {
    return new SQLException("synthetic " + state, state);
  }

  @Test
  @DisplayName("a replayable operation is retried until it wins")
  public void retryReplaysUntilItSucceeds() throws SQLException {
    var attempts = new AtomicInteger();
    var result =
        SystemDatabase.retryOnSerializationError(
            "test",
            () -> {
              if (attempts.incrementAndGet() < 3) {
                throw sqlState("40001");
              }
              return "committed";
            });

    assertEquals("committed", result);
    assertEquals(3, attempts.get());
  }

  @Test
  @DisplayName("a deadlock is replayed too, since the database already rolled it back")
  public void retryReplaysDeadlocks() throws SQLException {
    var attempts = new AtomicInteger();
    SystemDatabase.retryOnSerializationError(
        "test",
        () -> {
          if (attempts.incrementAndGet() < 2) {
            throw sqlState("40P01");
          }
          return null;
        });

    assertEquals(2, attempts.get());
  }

  @Test
  @DisplayName("anything that is not a conflict is handed straight back, unretried")
  public void retryDoesNotSwallowOtherFailures() {
    var attempts = new AtomicInteger();
    SystemDatabase.SqlSupplier<String> lostLock =
        () -> {
          attempts.incrementAndGet();
          throw sqlState("55P03");
        };

    var thrown =
        assertThrows(
            SQLException.class, () -> SystemDatabase.retryOnSerializationError("test", lostLock));

    assertEquals("55P03", thrown.getSQLState());
    assertEquals(1, attempts.get(), "a lost lock is the caller's to handle, not this helper's");
  }

  @Test
  @DisplayName("an interrupt stops the retry and leaves the flag set")
  public void retryStopsWhenInterrupted() {
    var attempts = new AtomicInteger();
    SystemDatabase.SqlSupplier<String> cancelledMidFlight =
        () -> {
          if (attempts.incrementAndGet() == 1) {
            // The work is cancelled while it runs, as shutdownNow() does to a retention worker.
            Thread.currentThread().interrupt();
          }
          throw sqlState("40001");
        };

    try {
      var thrown =
          assertThrows(
              SQLException.class,
              () -> SystemDatabase.retryOnSerializationError("test", cancelledMidFlight));

      assertEquals("40001", thrown.getSQLState());
      assertEquals(1, attempts.get(), "the work must not retry past its own cancellation");
      assertTrue(
          Thread.currentThread().isInterrupted(),
          "the flag must survive, or the caller cannot tell cancellation from exhaustion");
    } finally {
      // Clear it, so the flag cannot leak into whatever runs next on this thread.
      Thread.interrupted();
    }
  }
}
