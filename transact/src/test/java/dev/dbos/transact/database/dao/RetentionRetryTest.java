package dev.dbos.transact.database.dao;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The conflict retry retention applies to its own batches. Retention is the only work in the system
 * that deletes in batches from under live workflows, so it is the only caller that loses these
 * races routinely and can replay them safely; of the rest, only a transactional step replays a
 * conflict, and everything else lets one reach its caller. See {@code
 * SystemDatabase.isSerializationError}.
 */
public class RetentionRetryTest {

  private static SQLException sqlState(String state) {
    return new SQLException("synthetic " + state, state);
  }

  @Test
  @DisplayName("a replayable batch is retried until it wins")
  public void retryReplaysUntilItSucceeds() throws SQLException {
    var attempts = new AtomicInteger();
    var result =
        WorkflowDAO.retryOnSerializationError(
            () -> {
              if (attempts.incrementAndGet() < 3) {
                throw sqlState("40001");
              }
              return "collected";
            });

    assertEquals("collected", result);
    assertEquals(3, attempts.get());
  }

  @Test
  @DisplayName("a deadlock is replayed too, since the database already rolled it back")
  public void retryReplaysDeadlocks() throws SQLException {
    var attempts = new AtomicInteger();
    WorkflowDAO.retryOnSerializationError(
        () -> {
          if (attempts.incrementAndGet() < 2) {
            throw sqlState("40P01");
          }
          return null;
        });

    assertEquals(2, attempts.get());
  }

  @Test
  @DisplayName("an interrupt stops the retry and leaves the flag set")
  public void retryStopsWhenInterrupted() {
    var attempts = new AtomicInteger();
    try {
      var thrown =
          assertThrows(
              SQLException.class,
              () ->
                  WorkflowDAO.retryOnSerializationError(
                      () -> {
                        if (attempts.incrementAndGet() == 1) {
                          // The round is cancelled while it is working, as shutdownNow() does.
                          Thread.currentThread().interrupt();
                        }
                        throw sqlState("40001");
                      }));

      assertEquals("40001", thrown.getSQLState());
      assertEquals(1, attempts.get(), "the round must not retry past its own cancellation");
      assertTrue(
          Thread.currentThread().isInterrupted(),
          "the flag must survive, or the caller cannot tell cancellation from exhaustion");
    } finally {
      // Clear it, so the flag cannot leak into whatever runs next on this thread.
      Thread.interrupted();
    }
  }

  @Test
  @DisplayName("anything that is not a conflict is handed straight back, unretried")
  public void retryDoesNotSwallowOtherFailures() {
    var attempts = new AtomicInteger();
    var thrown =
        assertThrows(
            SQLException.class,
            () ->
                WorkflowDAO.retryOnSerializationError(
                    () -> {
                      attempts.incrementAndGet();
                      throw sqlState("55P03");
                    }));

    assertEquals("55P03", thrown.getSQLState());
    assertEquals(1, attempts.get(), "a lost lock is the caller's to handle, not retention's");
  }
}
