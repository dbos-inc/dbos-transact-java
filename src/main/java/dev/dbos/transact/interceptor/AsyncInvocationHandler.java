package dev.dbos.transact.interceptor;

import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.WorkflowFunctionWrapper;
import java.lang.reflect.Proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncInvocationHandler extends BaseInvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(AsyncInvocationHandler.class);

    @SuppressWarnings("unchecked")
    public static <T> T createProxy(Class<T> interfaceClass, Object implementation,
            DBOSExecutor executor) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalArgumentException("interfaceClass must be an interface");
        }

        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(),
                new Class<?>[]{interfaceClass},
                new AsyncInvocationHandler(implementation, executor));
    }

    public AsyncInvocationHandler(Object target, DBOSExecutor dbosExecutor) {
        super(target, dbosExecutor);
    }

    protected Object submitWorkflow(String workflowName, String targetClassName,
            WorkflowFunctionWrapper wrapper, Object[] args) throws Throwable {

        dbosExecutor.submitWorkflow(workflowName,
                targetClassName,
                wrapper.target,
                args,
                wrapper.function);

        return null;
    }
}
