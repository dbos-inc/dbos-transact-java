package dev.dbos.transact.execution;

@FunctionalInterface
public interface ThrowingRunnable<E extends Throwable> {
  void execute() throws E;
}
