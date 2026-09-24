package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.exceptions.DBOSSystemDatabaseException;

import java.sql.BatchUpdateException;
import java.sql.SQLException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What a system-database failure looks like once dbRetry has given up on it.
 *
 * <p>The classification tests here are the point: every classifier walks {@code getCause()}, so a
 * wrapper that does not chain silently stops all of them matching. {@code QueueServiceBackoffTest}
 * carries the same case for the poll loop's own decision, which is where the consequence lands -- a
 * routine lost lock race read as a genuine error backs the queue off toward the 120 s ceiling,
 * which is the bug #512 reports arriving by a different route.
 */
public class SystemDatabaseExceptionTest {

  @Test
  @DisplayName("the database's own failure is the cause, not just a private field")
  public void theFailureIsTheCause() {
    var failure = new SQLException("no such table", "42P01");

    var wrapped = new DBOSSystemDatabaseException(failure);

    assertSame(failure, wrapped.getCause());
    assertTrue(wrapped.getMessage().contains("42P01"), "the SQLSTATE belongs in the message");
    assertTrue(wrapped.getMessage().contains("no such table"));
  }

  @Test
  @DisplayName("wrapping does not hide a lost row lock from the queue listener")
  public void wrappingKeepsLockContentionVisible() {
    var wrapped = new DBOSSystemDatabaseException(new SQLException("contended", "55P03"));

    assertTrue(SystemDatabase.isLockNotAvailable(wrapped));
    assertTrue(SystemDatabase.isContentionError(wrapped));
  }

  @Test
  @DisplayName("wrapping does not hide a serialization conflict")
  public void wrappingKeepsConflictsVisible() {
    var wrapped = new DBOSSystemDatabaseException(new SQLException("conflict", "40001"));

    assertTrue(SystemDatabase.isSerializationError(wrapped));
    assertTrue(SystemDatabase.isContentionError(wrapped));
  }

  @Test
  @DisplayName("a genuine failure is still classified as one through the wrapper")
  public void wrappingDoesNotInventContention() {
    var wrapped = new DBOSSystemDatabaseException(new SQLException("disk on fire", "58030"));

    assertFalse(SystemDatabase.isContentionError(wrapped));
    assertFalse(SystemDatabase.isSerializationError(wrapped));
  }

  @Test
  @DisplayName("the deprecated accessor and the cause are the same object")
  @SuppressWarnings("removal")
  public void theAccessorAgreesWithTheCause() {
    var failure = new SQLException("boom");
    var wrapped = new DBOSSystemDatabaseException(failure);

    assertSame(wrapped.getCause(), wrapped.databaseException());
  }

  @Test
  @DisplayName("a failure carrying no SQLSTATE reports none, and says so in its message")
  public void aStatelessFailureReportsNoState() {
    var wrapped = new DBOSSystemDatabaseException(new SQLException("pool exhausted"));

    assertNull(wrapped.sqlState());
    assertEquals("System database access error: pool exhausted", wrapped.getMessage());
  }

  @Test
  @DisplayName("sqlState reports the state, wherever along the chains it is")
  public void sqlStateFindsTheState() {
    assertEquals(
        "40001", new DBOSSystemDatabaseException(new SQLException("x", "40001")).sqlState());

    // A cause carries it: the thrown exception is a wrapper with no state of its own.
    var wrapper = new SQLException("could not execute", (String) null);
    wrapper.initCause(new SQLException("the actual failure", "08006"));
    assertEquals("08006", new DBOSSystemDatabaseException(wrapper).sqlState());

    // A batch failure carries it on the next-exception chain, which getCause() never reaches.
    var batch = new BatchUpdateException("batch entry 0 failed", null, 0, new int[] {}, null);
    batch.setNextException(new SQLException("the actual failure", "40001"));
    var wrapped = new DBOSSystemDatabaseException(batch);
    assertEquals("40001", wrapped.sqlState());
    assertTrue(
        wrapped.getMessage().contains("40001"),
        "the message should name the state the caller would act on");
  }
}
