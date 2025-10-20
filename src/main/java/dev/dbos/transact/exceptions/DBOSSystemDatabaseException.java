package dev.dbos.transact.exceptions;

public class DBOSSystemDatabaseException extends RuntimeException {
  Throwable underlyingException;

  public DBOSSystemDatabaseException(Throwable e) {
    super(String.format("System database access error: %s", e.getMessage()));
    this.underlyingException = e;
  }

  public Throwable databaseException() {
    return underlyingException;
  }
}
