package dev.dbos.transact.spring;

/**
 * A RuntimeException wrapper used to pass non-Exception Throwables (typically Errors) through
 * ThrowingSupplier interfaces that are constrained to Exception types.
 *
 * <p>This is an internal implementation detail used by DBOSAspect to handle the mismatch between
 * AspectJ's ProceedingJoinPoint.proceed() which can throw any Throwable, and DBOS's
 * ThrowingSupplier which only accepts Exception types.
 *
 * <p>The wrapped Throwable should be extracted and rethrown to preserve original error semantics.
 */
public class WrappedThrowableException extends RuntimeException {

  /**
   * Wraps a non-Exception Throwable (typically an Error) so it can be passed through
   * Exception-constrained interfaces.
   *
   * @param wrappedThrowable the original Throwable to wrap, must not be an Exception
   */
  public WrappedThrowableException(Throwable wrappedThrowable) {
    super(
        "Wrapped non-Exception throwable: " + validate(wrappedThrowable).getClass().getSimpleName(),
        wrappedThrowable);
  }

  private static Throwable validate(Throwable t) {
    if (t instanceof Exception) {
      throw new IllegalArgumentException("Should not wrap Exception types, only Error types");
    }
    return t;
  }

  /**
   * Gets the original wrapped Throwable.
   *
   * @return the original Throwable that was wrapped
   */
  public Throwable getWrappedThrowable() {
    return getCause();
  }
}
