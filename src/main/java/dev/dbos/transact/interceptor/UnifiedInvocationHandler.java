package dev.dbos.transact.interceptor;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.WorkflowFunctionWrapper;
import dev.dbos.transact.workflow.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class UnifiedInvocationHandler extends BaseInvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(UnifiedInvocationHandler.class);

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

                executor.registerWorkflow(workflowName, implementation, implementation.getClass().getName(), method);
            }
        }

        T proxy =  (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass },
                new UnifiedInvocationHandler(implementation, executor)
        );

        return proxy;
    }

    protected UnifiedInvocationHandler(Object target, DBOSExecutor dbosExecutor) {
        super(target, dbosExecutor) ;
    }

    protected  Object submitWorkflow(String workflowName,
                                     String targetClassName,
                                     WorkflowFunctionWrapper wrapper,
                                     Object[] args
    ) throws Throwable {


        DBOSContext ctx = DBOSContextHolder.get() ;

        if (ctx.isAsync()) {

            logger.debug("invoking workflow asynchronously");

            dbosExecutor.submitWorkflow(
                    workflowName,
                    targetClassName,
                    wrapper.target,
                    args,
                    wrapper.function
            );

            return null ;

        } else if (ctx.getQueue() != null) {

            logger.debug("enqueuing workflow");

            dbosExecutor.enqueueWorkflow(
                    workflowName,
                    targetClassName,
                    wrapper,
                    args,
                    ctx.getQueue()
            );

            return null ;

        } else {

            logger.debug("invoking workflow synchronously");

            return dbosExecutor.syncWorkflow(
                    workflowName,
                    targetClassName,
                    wrapper.target,
                    args,
                    wrapper.function,
                    DBOSContextHolder.get().getWorkflowId()
            );
        }
    }
}
