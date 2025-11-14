package dev.dbos.transact.exceptions;

import java.sql.SQLException;

/**
 * This exception is thrown by DBOS when the system database cannot be reached, despite numerous
 * retries. Handling this exception is unlikely to end well. A new execution should be started once
 * system database connectivity is restored.
 */
public class DBOSSystemDatabaseException extends RuntimeException {
  Throwable underlyingException;

  public DBOSSystemDatabaseException(Throwable e) {
    super(
        String.format(
            "System database access error:%s %s",
            e instanceof SQLException ? " " + ((SQLException) e).getSQLState() : "",
            e.getMessage()));
    this.underlyingException = e;
  }

  /** A recent exception received from the system database connection */
  public Throwable databaseException() {
    return underlyingException;
  }
}
