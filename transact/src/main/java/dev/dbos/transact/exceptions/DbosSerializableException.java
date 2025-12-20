package dev.dbos.transact.exceptions;

/**
 * {@code DbosSerializableException} is used to serialize the exception when the original one is not
 * serializable.
 */
public class DbosSerializableException extends RuntimeException {
  private final String originalClassName;

  public DbosSerializableException(Throwable throwable) {
    super(throwable.getMessage());
    originalClassName = throwable.getClass().getName();
    setStackTrace(throwable.getStackTrace());
  }

  public String getOriginalClassName() {
    return originalClassName;
  }
}
