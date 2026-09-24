package dev.dbos.transact.exceptions;

import java.sql.SQLException;

/**
 * A system database operation that DBOS will not retry any further.
 *
 * <p>This covers two rather different situations, and the SQLSTATE is what tells them apart.
 * Connectivity failures are retried first, so one arriving here means the database stayed
 * unreachable across numerous attempts; handling that is unlikely to end well, and a new execution
 * should be started once connectivity is restored. A failure that retrying could never fix -- a
 * constraint violation, a syntax error, a missing relation -- is not retried at all and arrives
 * immediately, and may well be something the caller can act on. {@link #sqlState()} is how to
 * decide which is in hand.
 *
 * <p>The failure that caused it is the {@linkplain #getCause() cause}, and is the only level of
 * wrapping: the retry loop that throws this never throws on a path it would retry, so a caller
 * inspecting the chain finds the database's own exception one level down.
 */
public class DBOSSystemDatabaseException extends RuntimeException {

  public DBOSSystemDatabaseException(SQLException e) {
    super(describe(e), e);
  }

  /**
   * The SQLSTATE of the failure, or null if it carried none.
   *
   * <p>Walks both of JDBC's chains, re-entering at every cause, because that is how the classifier
   * that decided to throw this reads them. Reporting no state for a failure it resolved by one
   * would make this accessor useless for the choice above.
   */
  public String sqlState() {
    return firstSqlState(getCause());
  }

  /** How far {@link #firstSqlState} walks before giving up; neither chain is guaranteed acyclic. */
  private static final int MAX_LINKED_EXCEPTIONS = 1000;

  /** The first SQLSTATE along either chain, or null if nothing carries one. */
  private static String firstSqlState(Throwable t) {
    int budget = MAX_LINKED_EXCEPTIONS;
    for (Throwable cause = t; cause != null && budget-- > 0; cause = cause.getCause()) {
      if (cause instanceof SQLException sqlException) {
        for (Throwable linked : sqlException) {
          if (budget-- <= 0) {
            return null;
          }
          if (linked instanceof SQLException linkedSql && linkedSql.getSQLState() != null) {
            return linkedSql.getSQLState();
          }
        }
      }
    }
    return null;
  }

  private static String describe(SQLException e) {
    String state = firstSqlState(e);
    return String.format(
        "System database access error:%s %s", state == null ? "" : " " + state, e.getMessage());
  }

  /**
   * A recent exception received from the system database connection.
   *
   * @deprecated the failure is now the {@linkplain #getCause() cause}, which is where callers and
   *     the SQLSTATE classifiers look for it. This accessor returns the same object.
   */
  @Deprecated(since = "1.1", forRemoval = true)
  public Throwable databaseException() {
    return getCause();
  }
}
