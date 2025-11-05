package dev.dbos.transact.exceptions;

import java.sql.SQLException;

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

  public Throwable databaseException() {
    return underlyingException;
  }
}
