package dev.dbos.transact.execution;

@FunctionalInterface
public interface WorkflowFunction<T> {
    T execute() throws Throwable;
}
