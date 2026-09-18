package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.exceptions.DBOSSystemDatabaseException;

import java.sql.BatchUpdateException;
import java.sql.SQLException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * How deeply the SQLSTATE predicates look for the state that carries the answer.
 *
 * <p>Two chains matter, not one. Causes carry this class's own wrapping; {@code getNextException()}
 * carries what JDBC uses for batch failures, where the driver exception holding the real SQLSTATE
 * hangs off the next-exception chain rather than the cause chain.
 */
public class ErrorClassificationTest {

  /** A batch failure shaped as JDBC specifies: the real state hangs off the next exception. */
  private static BatchUpdateException batchFailure(String sqlState) {
    var batch = new BatchUpdateException("batch entry 0 failed", null, 0, new int[] {}, null);
    batch.setNextException(new SQLException("the actual failure", sqlState));
    return batch;
  }

  @Test
  @DisplayName("a batch failure's real SQLSTATE is found on the next-exception chain")
  public void batchFailuresAreClassified() {
    assertTrue(SystemDatabase.isSerializationError(batchFailure("40001")));
    assertTrue(SystemDatabase.isContentionError(batchFailure("40001")));
    assertTrue(SystemDatabase.isLockNotAvailable(batchFailure("55P03")));
    assertFalse(SystemDatabase.isSerializationError(batchFailure("23505")));
  }

  @Test
  @DisplayName("a batch failure wrapped by dbRetry is still classified")
  public void batchFailuresSurviveWrapping() {
    // Both chains at once: DBOSSystemDatabaseException over a batch over the driver's exception.
    var wrapped = new DBOSSystemDatabaseException(batchFailure("40001"));

    assertTrue(SystemDatabase.isSerializationError(wrapped));
    assertTrue(SystemDatabase.isContentionError(wrapped));
  }

  /** A driver exception wrapped by something carrying no state of its own, as a pool wraps one. */
  private static SQLException wrapped(String innerState, String outerMessage) {
    var outer = new SQLException(outerMessage, (String) null);
    outer.initCause(new SQLException("the actual failure", innerState));
    return outer;
  }

  @Test
  @DisplayName("a wrapped SQLSTATE is trusted, as a wrapped message already was")
  public void aWrappedStateIsTrustedLikeAWrappedMessage() {
    // isConnectionFailure read getSQLState() on the top level only, then walked the cause chain
    // for message text. So a wrapped 08006 was retried when its message happened to match one of
    // five strings and handed to the caller when it did not -- trusting a wrapped message more
    // than a wrapped state. Both are now read at the same depth.
    assertTrue(SystemDatabase.isConnectionState(wrapped("08006", "could not execute")));
    assertTrue(SystemDatabase.isConnectionState(wrapped("57014", "could not execute")));
    assertTrue(SystemDatabase.isTransientState(wrapped("53300", "could not execute")));

    // The message tier still works, and still only on messages.
    assertTrue(SystemDatabase.hasConnectionMessage(wrapped("42P01", "Connection is closed")));
    assertFalse(SystemDatabase.isConnectionState(wrapped("42P01", "Connection is closed")));
  }

  @Test
  @DisplayName("the dbRetry predicates read a batch failure's real state too")
  public void retryPredicatesSeeBatchFailures() {
    // These two feed an unbounded retry loop, so their depth matters more than the classifiers'.
    assertTrue(SystemDatabase.isConnectionState(batchFailure("08006")));
    assertTrue(SystemDatabase.isTransientState(batchFailure("53300")));
    assertFalse(SystemDatabase.isTransientState(batchFailure("40001")));
    assertFalse(SystemDatabase.isConnectionState(batchFailure("40001")));
  }

  @Test
  @DisplayName("an unrelated state is not matched through either chain")
  public void unrelatedStatesDoNotMatch() {
    var batch = batchFailure("42P01");

    assertFalse(SystemDatabase.isContentionError(batch));
    assertFalse(SystemDatabase.isSerializationError(batch));
    assertFalse(SystemDatabase.isLockNotAvailable(batch));
  }

  @Test
  @DisplayName("an exception with no SQLSTATE anywhere matches nothing")
  public void noStateMatchesNothing() {
    var bare = new DBOSSystemDatabaseException(new SQLException("no state at all"));

    assertFalse(SystemDatabase.isContentionError(bare));
    assertFalse(SystemDatabase.isSerializationError(bare));
    assertFalse(SystemDatabase.isLockNotAvailable(bare));
  }

  @Test
  @DisplayName("JDBC's standard rollback type does not put a conflict back in the retry loop")
  public void aStandardRollbackTypeReachesTheCaller() {
    // SQLTransactionRollbackException is a SQLTransientException, and the dispatch used to OR the
    // type in front of the state. A driver that throws JDBC's standard type for a serialization
    // failure would have had it retried forever. PgJDBC does not, which is why nothing caught it.
    var standard = new java.sql.SQLTransactionRollbackException("conflict", "40001");

    assertEquals(SystemDatabase.Failure.CALLERS, SystemDatabase.classify(standard));
  }

  @Test
  @DisplayName("a SQLSTATE decides even when the exception type disagrees")
  public void theStateOutranksTheType() {
    // A transient *type* carrying a state this does not retry is the caller's.
    assertEquals(
        SystemDatabase.Failure.CALLERS,
        SystemDatabase.classify(new java.sql.SQLTimeoutException("gone", "42P01")));

    // A recoverable *type* carrying a conflict is likewise the caller's, not a pool reset.
    assertEquals(
        SystemDatabase.Failure.CALLERS,
        SystemDatabase.classify(new java.sql.SQLRecoverableException("conflict", "40001")));

    // And the states this does retry still decide, whatever the type.
    assertEquals(
        SystemDatabase.Failure.CONNECTION,
        SystemDatabase.classify(new SQLException("down", "08006")));
    assertEquals(
        SystemDatabase.Failure.TRANSIENT,
        SystemDatabase.classify(new SQLException("no room", "53300")));
  }

  @Test
  @DisplayName("with no SQLSTATE anywhere, the type and message decide")
  public void withoutAStateTheTypeDecides() {
    assertEquals(
        SystemDatabase.Failure.CONNECTION,
        SystemDatabase.classify(new java.sql.SQLRecoverableException("socket closed")));
    assertEquals(
        SystemDatabase.Failure.CONNECTION,
        SystemDatabase.classify(new SQLException("Connection is closed")));
    assertEquals(
        SystemDatabase.Failure.TRANSIENT,
        SystemDatabase.classify(new java.sql.SQLTransientException("try again")));
    assertEquals(
        SystemDatabase.Failure.CALLERS, SystemDatabase.classify(new SQLException("duplicate key")));
  }

  @Test
  @DisplayName("a message that reads like a connection error no longer overrides a state")
  public void aMessageDoesNotOverrideAState() {
    // This is the deliberate change in which failures evict the Hikari pool: the message tier is
    // reached only when nothing in either chain carries a state.
    var stated = new SQLException("Connection is closed", "23505");

    assertEquals(SystemDatabase.Failure.CALLERS, SystemDatabase.classify(stated));
  }
}
