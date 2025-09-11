package dev.dbos.transact.execution;

@FunctionalInterface
public interface ThrowingSupplier<T> {
    T get() throws Throwable;
}