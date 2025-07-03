package dev.dbos.transact.interceptor;

import dev.dbos.transact.context.DBOSContext;
import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.context.SetWorkflowID;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.WorkflowFunctionWrapper;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Transaction;
import dev.dbos.transact.workflow.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;

public class TransactInvocationHandler implements InvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(TransactInvocationHandler.class);

    private final Object target;
    private final String targetClassName ;
    private final DBOSExecutor dbosExecutor ;

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
                new TransactInvocationHandler(implementation, executor)
        );

        return proxy;
    }

    protected TransactInvocationHandler(Object target, DBOSExecutor dbosExecutor) {
        this.target = target;
        this.targetClassName = target.getClass().getName();
        this.dbosExecutor = dbosExecutor ;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        logger.info("Interceptor called for method: " + method.getName());

        Method targetMethod = target.getClass().getMethod(method.getName(), method.getParameterTypes());

        if (targetMethod.isAnnotationPresent(Workflow.class)) {
            return handleWorkflow(method, args, targetMethod.getAnnotation(Workflow.class));
        } else if (targetMethod.isAnnotationPresent(Transaction.class)) {
            return handleTransaction(method, args, targetMethod.getAnnotation(Transaction.class));
        } else if (targetMethod.isAnnotationPresent(Step.class)) {
            return handleStep(method, args, targetMethod.getAnnotation(Step.class));
        }

        // No special annotation, proceed normally
        return method.invoke(target, args);
    }

    protected Object handleWorkflow(Method method, Object[] args, Workflow workflow) throws Throwable {

        String workflowName = workflow.name().isEmpty() ? method.getName() : workflow.name();

        String msg = String.format("Before: Starting workflow '%s' (timeout: %ds)%n",
                workflowName,
                workflow.timeout());

        logger.info(msg);

        WorkflowFunctionWrapper wrapper = dbosExecutor.getWorkflow(workflowName);
        if (wrapper == null) {
            throw new IllegalStateException("Workflow not registered: " + workflowName);
        }

        DBOSContext ctx = DBOSContextHolder.get() ;

        if (ctx.isInWorkflow()) {

            // child workflow
            if (ctx.hasParent()) {
                // child called with SetWorkflowId
                return dbosExecutor.runWorkflow(
                        workflowName,
                        targetClassName,
                        wrapper.target,
                        args,
                        wrapper.function,
                        ctx.getWorkflowId()
                );

            } else {
                // child called without Set
                // create child context from the parent
                String childId = ctx.getWorkflowId() + "_" + ctx.getParentFunctionId();
                try(SetWorkflowID id = new SetWorkflowID(childId)) {
                    return dbosExecutor.runWorkflow(
                            workflowName,
                            targetClassName,
                            wrapper.target,
                            args,
                            wrapper.function,
                            childId
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
                    return dbosExecutor.runWorkflow(
                            workflowName,
                            targetClassName,
                            wrapper.target,
                            args,
                            wrapper.function,
                            workflowfId
                    );
                }
            } else {
                // not child called with Set just run
                DBOSContextHolder.get().setInWorkflow(true);
                return dbosExecutor.runWorkflow(
                        workflowName,
                        targetClassName,
                        wrapper.target,
                        args,
                        wrapper.function,
                        ctx.getWorkflowId()
                );
            }
        }
    }

    protected Object handleTransaction(Method method, Object[] args, Transaction transaction) throws Throwable {

        String msg = String.format("Before starting transaction %s %s", method.getName(), transaction.name());
        logger.info(msg) ;

        try {
            Object result = method.invoke(target, args);
            logger.info("After : Transaction committed successfully");
            return result;
        } catch (Exception e) {

            logger.info("Transaction attempt: %s%n",
                    e.getCause().getMessage());
            throw e.getCause();

        }

    }

    protected Object handleStep(Method method, Object[] args, Step step) throws Throwable {
        String msg = String.format("Before : Executing step %s %s",
                method.getName(), step.name());
        logger.info(msg);

        Object result = dbosExecutor.runStep(step.name(),
            step.retriesAllowed(),
                step.maxAttempts(),
                step.backOffRate(),
                args,
                ()-> method.invoke(target, args)) ;
            logger.info("After: Step completed successfully");
            return result;

    }
}
