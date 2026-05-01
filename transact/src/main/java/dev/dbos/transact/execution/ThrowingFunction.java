package dev.dbos.transact.execution;

@FunctionalInterface
public interface ThrowingFunction<T, P, E extends Exception> {
  T execute(P p) throws E;
}
