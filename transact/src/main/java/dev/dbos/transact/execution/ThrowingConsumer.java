package dev.dbos.transact.execution;

@FunctionalInterface
public interface ThrowingConsumer<P, E extends Exception> {
  void execute(P p) throws E;
}
