package dev.dbos.transact.exceptions;

public class DBOSException extends RuntimeException {

  private final int errorCode;

  public DBOSException(int errorCode, String message) {
    super(message);
    this.errorCode = errorCode;
  }

  public DBOSException(int errorCode, String message, Throwable cause) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public int getErrorCode() {
    return errorCode;
  }

  @Override
  public String toString() {
    return String.format("DBOSException[%d]: %s", errorCode, getMessage());
  }
}
