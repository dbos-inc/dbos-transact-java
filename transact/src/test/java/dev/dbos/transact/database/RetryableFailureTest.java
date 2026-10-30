package dev.dbos.transact.database;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.dbos.transact.exceptions.DBOSSystemDatabaseException;

import java.sql.SQLException;

import org.junit.jupiter.api.Test;

/**
 * Which failures of an internal step's work are its answer, to be recorded as the step's error, and
 * which only stopped the retries short of one.
 */
public class RetryableFailureTest {

  private static DBOSSystemDatabaseException wrapped(String sqlState) {
    return new DBOSSystemDatabaseException(new SQLException("failed", sqlState));
  }

  @Test
  public void aSerializationFailureOrDeadlockThatOutlastedItsRetriesIsNotAnAnswer() {
    assertTrue(SystemDatabase.isRetryableFailure(wrapped("40001")));
    assertTrue(SystemDatabase.isRetryableFailure(wrapped("40P01")));
  }

  @Test
  public void aConnectionFailureCutShortIsNotAnAnswer() {
    assertTrue(SystemDatabase.isRetryableFailure(wrapped("08006")));
    assertTrue(SystemDatabase.isRetryableFailure(wrapped("57P01")));
  }

  @Test
  public void aFailureTheRetriesWouldNotRetryIsTheAnswer() {
    assertFalse(SystemDatabase.isRetryableFailure(wrapped("XX000")));
    assertFalse(SystemDatabase.isRetryableFailure(wrapped("23505")));
    assertFalse(SystemDatabase.isRetryableFailure(new IllegalArgumentException("refused")));
  }
}
