package dev.dbos.transact.execution;

@FunctionalInterface
public interface ThrowingSupplier<T, E extends Exception> {
  T execute() throws E;
}
