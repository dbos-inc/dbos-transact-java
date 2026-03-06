package dev.dbos.transact.database;

public sealed interface Result<T> {

  // Success case holds the value
  record Success<T>(T value) implements Result<T> {}

  // Failure case holds the exception
  record Failure<T>(Throwable exception) implements Result<T> {}

  // Helper methods for cleaner API
  static <T> Result<T> success(T value) {
    return new Success<>(value);
  }

  static <T> Result<T> failure(Throwable exception) {
    return new Failure<>(exception);
  }

  @SuppressWarnings("unchecked")
  public static <T, E extends Exception> T process(Result<T> result) throws E {
    if (result instanceof Result.Success<T> success) {
      return success.value();
    } else if (result instanceof Result.Failure<T> failure) {
      var t = failure.exception();
      if (failure.exception() instanceof Exception) {
        throw (E) t;
      }
      throw new RuntimeException(t.getMessage(), t);
    } else {
      throw new IllegalStateException("Unknown Result type");
    }
  }
}
