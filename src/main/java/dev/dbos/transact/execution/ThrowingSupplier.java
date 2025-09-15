package dev.dbos.transact.execution;

@FunctionalInterface
public interface ThrowingSupplier<T, E extends Throwable> {
  T execute() throws E;
}
