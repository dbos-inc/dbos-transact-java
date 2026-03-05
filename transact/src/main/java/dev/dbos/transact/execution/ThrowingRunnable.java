package dev.dbos.transact.execution;

@FunctionalInterface
public interface ThrowingRunnable<E extends Exception> {
  void execute() throws E;

  default ThrowingSupplier<Void, E> asSupplier() {
    return () -> {
      this.execute();
      return null;
    };
  }
}
