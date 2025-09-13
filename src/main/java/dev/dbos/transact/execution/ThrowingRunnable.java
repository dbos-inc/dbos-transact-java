package dev.dbos.transact.execution;

@FunctionalInterface
public interface ThrowingRunnable {
  void execute() throws Throwable;
}
