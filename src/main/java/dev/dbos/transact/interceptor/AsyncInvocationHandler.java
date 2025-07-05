package dev.dbos.transact.interceptor;

import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.WorkflowFunctionWrapper;
import dev.dbos.transact.workflow.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class AsyncInvocationHandler extends BaseInvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(AsyncInvocationHandler.class);

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

                executor.registerWorkflow(workflowName, implementation, implementation.getClass().getName(), method);
            }
        }


        T proxy =  (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass },
                new AsyncInvocationHandler(implementation, executor)) ;


        return proxy;
    }

    public AsyncInvocationHandler(Object target, DBOSExecutor dbosExecutor) {
        super(target, dbosExecutor);
    }

    protected  Object submitWorkflow(String workflowName,
                                             String targetClassName,
                                             WorkflowFunctionWrapper wrapper,
                                             Object[] args
    ) throws Throwable {

        dbosExecutor.submitWorkflow(
                workflowName,
                targetClassName,
                wrapper.target,
                args,
                wrapper.function
        );

        return null ;
    }
}
