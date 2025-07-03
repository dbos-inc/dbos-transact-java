package dev.dbos.transact.interceptor;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.WorkflowFunctionWrapper;
import dev.dbos.transact.queue.Queue;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.UUID;

public class QueueInvocationHandler implements InvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(QueueInvocationHandler.class);

    private final Object target;
    private final String targetClassName ;
    private final DBOSExecutor dbosExecutor ;
    private final Queue queue ;

    public static <T> T createProxy(Class<T> interfaceClass, Object implementation, Queue queue, DBOSExecutor executor) {
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
                new QueueInvocationHandler(implementation, queue, executor)) ;


        return proxy;
    }



    public QueueInvocationHandler(Object target, Queue queue, DBOSExecutor dbosExecutor) {
        this.target = target;
        this.targetClassName = target.getClass().getName();
        this.dbosExecutor = dbosExecutor ;
        this.queue = queue;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        Method implMethod = target.getClass().getMethod(method.getName(), method.getParameterTypes());

        if (implMethod.isAnnotationPresent(Workflow.class)) {
            return handleWorkflow(method, args, implMethod.getAnnotation(Workflow.class));

        } else if (implMethod.isAnnotationPresent(Step.class)) {
            return handleStep(method, args, implMethod.getAnnotation(Step.class));
        }

        // No special annotation, proceed normally
        return method.invoke(target, args);


    }

    protected Object handleWorkflow(Method method, Object[] args, Workflow workflow) throws Throwable {

        String workflowName = workflow.name().isEmpty() ? method.getName() : workflow.name();

        WorkflowFunctionWrapper wrapper = dbosExecutor.getWorkflow(workflowName);
        if (wrapper == null) {
            throw new IllegalStateException("Workflow not registered: " + workflowName);
        }

        DBOSContext ctx = DBOSContextHolder.get() ;
        if (ctx.isInWorkflow()) {

            // child workflow
            if (ctx.hasParent()) {
                // child called with SetWorkflowId

                dbosExecutor.enqueueWorkflow(
                        workflowName,
                        targetClassName,
                        wrapper,
                        args,
                        queue
                );
            } else {
                // child called without Set
                // create child context from the parent

                String childId = ctx.getWorkflowId() + "_" + ctx.getParentFunctionId();
                try(SetWorkflowID id = new SetWorkflowID(childId)) {
                    dbosExecutor.enqueueWorkflow(
                            workflowName,
                            targetClassName,
                            wrapper,
                            args,
                            queue
                    );
                }

            }
        } else {


            // parent
            if (ctx.getWorkflowId() == null ) {
                // parent called without SetWorkflowId
                String workflowfId = UUID.randomUUID().toString();
                try(SetWorkflowID id = new SetWorkflowID(workflowfId)) {
                    DBOSContextHolder.get().setInWorkflow(true);
                    dbosExecutor.enqueueWorkflow(
                            workflowName,
                            targetClassName,
                            wrapper,
                            args,
                            queue
                    );
                }
            } else {
                // not child called with Set just run
                DBOSContextHolder.get().setInWorkflow(true);
                dbosExecutor.enqueueWorkflow(
                        workflowName,
                        targetClassName,
                        wrapper,
                        args,
                        queue
                );
            }
        }

        return getDefaultValue(method.getReturnType()) ; // always return null or default

    }

    protected Object handleStep(Method method, Object[] args, Step step) throws Throwable {
        String msg = String.format("Before : Executing step %s %s",
                method.getName(), step.name());
        logger.info(msg);
        try {
            Object result = dbosExecutor.runStep(step.name(),
                    step.retriesAllowed(),
                    step.maxAttempts(),
                    step.backOffRate(),
                    args,
                    ()-> method.invoke(target, args)) ;
            logger.info("After: Step completed successfully");
            return result;
        } catch (Exception e) {
            logger.info("Step failed: " + e.getCause().getMessage());
            throw e.getCause();
        }
    }

    private Object getDefaultValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) return null;
        if (returnType == boolean.class) return false;
        if (returnType == char.class) return '\0';
        if (returnType == byte.class || returnType == short.class || returnType == int.class) return 0;
        if (returnType == long.class) return 0L;
        if (returnType == float.class) return 0f;
        if (returnType == double.class) return 0d;
        return null;
    }
}
