package dev.dbos.transact.interceptor;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.WorkflowFunctionWrapper;
import dev.dbos.transact.internal.DBOSContextHolder;

import java.lang.reflect.Proxy;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnifiedInvocationHandler extends BaseInvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(UnifiedInvocationHandler.class);

    @SuppressWarnings("unchecked")
    public static <T> T createProxy(Class<T> interfaceClass, Object implementation,
            Supplier<DBOSExecutor> executor) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalArgumentException("interfaceClass must be an interface");
        }

        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                new UnifiedInvocationHandler(implementation, executor));
    }

    protected UnifiedInvocationHandler(Object target, Supplier<DBOSExecutor> dbosExecutor) {
        super(target, dbosExecutor);
    }

    protected Object submitWorkflow(String workflowName, String targetClassName,
            WorkflowFunctionWrapper wrapper, Object[] args) throws Throwable {

        var executor = executorSupplier.get();
        if (executor == null) {
            throw new IllegalStateException();
        }

        DBOSContext ctx = DBOSContextHolder.get();

        // TODO: I think we want to remove this and force users to use submitWorkflow for wf enqueue
        if (ctx.getQueue() != null) {

            logger.debug("enqueuing workflow");

            executor.enqueueWorkflow(workflowName, targetClassName, args, ctx.getQueue());

            return null;

        } else {

            logger.debug("invoking workflow synchronously");

            return executor.syncWorkflow(workflowName,
                    targetClassName,
                    wrapper.target,
                    args,
                    wrapper.function,
                    DBOSContextHolder.get().getWorkflowId());
        }
    }
}
