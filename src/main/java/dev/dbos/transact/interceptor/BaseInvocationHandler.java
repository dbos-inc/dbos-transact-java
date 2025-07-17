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
import java.util.UUID;

public abstract class BaseInvocationHandler implements InvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(BaseInvocationHandler.class);

    private final Object target;
    private final String targetClassName ;
    protected final DBOSExecutor dbosExecutor ;

    public BaseInvocationHandler(Object target, DBOSExecutor dbosExecutor) {
        this.target = target;
        this.targetClassName = target.getClass().getName();
        this.dbosExecutor = dbosExecutor ;
    }

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

        String msg = String.format("Before: Starting workflow '%s' (timeout: %ds)%n",
                workflowName,
                workflow.timeout());

        logger.info(msg);

        WorkflowFunctionWrapper wrapper = dbosExecutor.getWorkflow(workflowName);
        if (wrapper == null) {
            throw new IllegalStateException("Workflow not registered: " + workflowName);
        }

        DBOSContext ctx = DBOSContextHolder.get() ;

        Object result ;

        if (ctx.isInWorkflow()) {

            // child workflow
            if (ctx.hasParent()) {
                // child called with SetWorkflowId
                result = submitWorkflow(workflowName, targetClassName, wrapper, args);
            } else {
                // child called without Set
                // create child context from the parent

                String childId = ctx.getWorkflowId() + "-" + ctx.getParentFunctionId();
                try(SetWorkflowID id = new SetWorkflowID(childId)) {
                    result = submitWorkflow(workflowName, targetClassName, wrapper, args);
                }

            }
        } else {


            // parent
            if (ctx.getWorkflowId() == null ) {
                // parent called without SetWorkflowId
                String workflowfId = UUID.randomUUID().toString();
                try(SetWorkflowID id = new SetWorkflowID(workflowfId)) {
                    DBOSContextHolder.get().setInWorkflow(true);
                    result = submitWorkflow(workflowName, targetClassName, wrapper, args);
                }
            } else {
                // not child called with Set just run
                DBOSContextHolder.get().setInWorkflow(true);
                result = submitWorkflow(workflowName, targetClassName, wrapper, args);
            }
        }

        if (result != null) {
            return result ;
        } else {
            return getDefaultValue(method.getReturnType()) ;
            // always return null or default
        }
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

    protected Object getDefaultValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) return null;
        if (returnType == boolean.class) return false;
        if (returnType == char.class) return '\0';
        if (returnType == byte.class || returnType == short.class || returnType == int.class) return 0;
        if (returnType == long.class) return 0L;
        if (returnType == float.class) return 0f;
        if (returnType == double.class) return 0d;
        return null;
    }

    protected abstract Object submitWorkflow(String workflowName,
                                       String targetClassName,
                                       WorkflowFunctionWrapper wrapper,
                                       Object[] args
    ) throws Throwable ;

}
