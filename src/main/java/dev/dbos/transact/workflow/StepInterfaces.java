package dev.dbos.transact.workflow;

public class StepInterfaces {
    @FunctionalInterface
    public interface ThrowingNoArg<R, E extends Exception> {
        R get() throws E;
    }

    @FunctionalInterface
    public interface ThrowingNoArgNoReturn<E extends Exception> {
        void run() throws E;
    }

    @FunctionalInterface
    public interface NonthrowingNoArg<R> {
        R get();
    }

    @FunctionalInterface
    public interface NonthrowingNoArgNoReturn {
        void get();
    }
}
