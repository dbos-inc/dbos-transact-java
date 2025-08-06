package dev.dbos.transact.interceptor;

import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.WorkflowFunctionWrapper;
import dev.dbos.transact.workflow.Workflow;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransactInvocationHandler extends BaseInvocationHandler {
    private static final Logger logger = LoggerFactory.getLogger(TransactInvocationHandler.class);

    @SuppressWarnings("unchecked")
    public static <T> T createProxy(Class<T> interfaceClass, Object implementation, DBOSExecutor executor) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalArgumentException("interfaceClass must be an interface");
        }

        // Register all @Workflow methods
        Method[] methods = implementation.getClass().getDeclaredMethods();
        for (Method method : methods) {
            Workflow wfAnnotation = method.getAnnotation(Workflow.class);
            if (wfAnnotation != null) {
                String workflowName = wfAnnotation.name().isEmpty() ? method.getName() : wfAnnotation.name();
                method.setAccessible(true); // In case it's not public

                executor.registerWorkflow(workflowName,implementation,implementation.getClass().getName(),method);
            }
        }

        T proxy = (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(),new Class<?>[]{interfaceClass},
                new TransactInvocationHandler(implementation, executor));

        return proxy;
    }

    protected TransactInvocationHandler(Object target, DBOSExecutor dbosExecutor) {
        super(target, dbosExecutor);
    }

    protected Object submitWorkflow(String workflowName, String targetClassName, WorkflowFunctionWrapper wrapper,
            Object[] args) throws Throwable {

        return dbosExecutor.syncWorkflow(workflowName,targetClassName,wrapper.target,args,wrapper.function,
                DBOSContextHolder.get().getWorkflowId());
    }
}
