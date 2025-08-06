package dev.dbos.transact.execution;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;

public class ContextAwareRunnable implements Runnable {
    private final Runnable task;
    private final DBOSContext capturedContext;

    public ContextAwareRunnable(DBOSContext ctx, Runnable task) {
        this.task = task;
        this.capturedContext = ctx;
    }

    @Override
    public void run() {
        DBOSContextHolder.set(capturedContext);
        try {
            task.run();
        } finally {
            DBOSContextHolder.clear();
        }
    }
}
