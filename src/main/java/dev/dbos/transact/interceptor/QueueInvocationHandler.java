package dev.dbos.transact.interceptor;

import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.WorkflowFunctionWrapper;
import dev.dbos.transact.queue.Queue;

import java.lang.reflect.Proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueueInvocationHandler extends BaseInvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(QueueInvocationHandler.class);
    private final Queue queue;

    @SuppressWarnings("unchecked")
    public static <T> T createProxy(Class<T> interfaceClass, Object implementation, Queue queue,
            DBOSExecutor executor) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalArgumentException("interfaceClass must be an interface");
        }

        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                new QueueInvocationHandler(implementation, queue, executor));
    }

    public QueueInvocationHandler(Object target, Queue queue, DBOSExecutor dbosExecutor) {
        super(target, dbosExecutor);
        this.queue = queue;
    }

    protected Object submitWorkflow(String workflowName, String targetClassName,
            WorkflowFunctionWrapper wrapper, Object[] args) throws Throwable {

        dbosExecutor.enqueueWorkflow(workflowName, targetClassName, wrapper, args, queue);

        return null;
    }
}
