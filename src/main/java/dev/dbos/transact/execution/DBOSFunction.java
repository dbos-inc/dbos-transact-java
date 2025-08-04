package dev.dbos.transact.execution;

@FunctionalInterface
public interface DBOSFunction<T> {
  T execute() throws Throwable;
}
