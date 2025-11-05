package dev.dbos.transact.execution;

@FunctionalInterface
public interface ThrowingRunnable<E extends Exception> {
  void execute() throws E;
}
