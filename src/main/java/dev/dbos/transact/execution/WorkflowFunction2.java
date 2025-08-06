package dev.dbos.transact.execution;

@FunctionalInterface
public interface WorkflowFunction2<T1, T2, R> {
  R run(T1 arg1, T2 arg2);
}
