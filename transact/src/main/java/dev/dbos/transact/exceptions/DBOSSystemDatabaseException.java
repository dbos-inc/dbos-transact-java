package dev.dbos.transact.exceptions;

import java.sql.SQLException;

/**
 * This exception is thrown by DBOS when the system database cannot be reached, despite numerous
 * retries. Handling this exception is unlikely to end well. A new execution should be started once
 * system database connectivity is restored.
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
   * <p>Looks along both of JDBC's chains rather than at the wrapped exception alone, because the
   * exception holding the state is not always the one thrown. {@code executeBatch()} throws a
   * {@link java.sql.BatchUpdateException} and hangs the driver's own failure, which is the one
   * carrying the state, off {@link SQLException#getNextException()}. {@link SQLException} is {@link
   * Iterable} over both chains, so the first state found is the most specific one available.
   */
  public String sqlState() {
    return firstSqlState((SQLException) getCause());
  }

  /** The first SQLSTATE along either chain, or null if nothing carries one. */
  private static String firstSqlState(SQLException e) {
    for (Throwable linked : e) {
      if (linked instanceof SQLException sqlException && sqlException.getSQLState() != null) {
        return sqlException.getSQLState();
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
