package dev.dbos.transact.execution;

@FunctionalInterface
public interface WorkflowFunction1<T1, R> {
  R run(T1 arg1);
}
