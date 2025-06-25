package dev.dbos.transact.execution;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class ContextAwareCallable<T> implements Callable<T> {
    private final Callable<T> task;
    private DBOSContext capturedContext;

    // having to pass in workflowId due memory visibility
    // issue TODO: make copy of context thread safe
    private volatile String workflowId ;

    Logger logger = LoggerFactory.getLogger(ContextAwareCallable.class) ;

    public ContextAwareCallable(Callable<T> task) {
        this.task = task;
        // this.capturedContext = DBOSContextHolder.get();
    }




    public void setWorkflowId(java.lang.String workflowId) {
        this.workflowId = workflowId;
    }

    @Override
    public T call() throws Exception {
        DBOSContext ctx = new DBOSContext();
        ctx.setWorkflowId(workflowId); ;

        DBOSContextHolder.set(ctx);
        System.out.println("After set(): DBOSContextHolder.get() = " + DBOSContextHolder.get().getWorkflowId() ) ;
        System.out.println("Thread ID in ContextAwareCallable.call(): " + Thread.currentThread().getId());
        System.out.println("Context object hash: " + System.identityHashCode(DBOSContextHolder.get()));

        try {
            return task.call();
        } finally {
            DBOSContextHolder.clear();
        }
    }
}
