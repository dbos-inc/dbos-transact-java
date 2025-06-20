package dev.dbos.transact.execution;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;

import java.util.concurrent.Callable;

public class ContextAwareCallable<T> implements Callable<T> {
    private final Callable<T> task;
    private final DBOSContext capturedContext;

    public ContextAwareCallable(Callable<T> task) {
        this.task = task;
        this.capturedContext = DBOSContextHolder.get();
    }

    @Override
    public T call() throws Exception {
        DBOSContextHolder.set(capturedContext);
        try {
            return task.call();
        } finally {
            DBOSContextHolder.clear();
        }
    }
}
