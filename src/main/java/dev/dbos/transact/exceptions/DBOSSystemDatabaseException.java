package dev.dbos.transact.exceptions;

import static dev.dbos.transact.exceptions.ErrorCode.SYSTEM_DATABASE_ACCESS_ERROR;

public class DBOSSystemDatabaseException extends DBOSException {
  Throwable underlyingException;

  public DBOSSystemDatabaseException(Throwable e) {
    super(
        SYSTEM_DATABASE_ACCESS_ERROR.getCode(),
        String.format("System database access error: %s", e.getMessage()));
    this.underlyingException = e;
  }

  public Throwable getDatabaseException() {
    return underlyingException;
  }
}
