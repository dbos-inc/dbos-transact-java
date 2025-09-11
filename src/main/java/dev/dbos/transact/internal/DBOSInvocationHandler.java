package dev.dbos.transact.internal;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.Objects;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;

public class DBOSInvocationHandler implements java.lang.reflect.InvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(DBOSInvocationHandler.class);

    private final Object target;
    private final Class<?> targetClass;
    private final String targetClassName;
    private final Supplier<DBOSExecutor> executorSupplier;

    public DBOSInvocationHandler(Object target, Supplier<DBOSExecutor> executorSupplier) {
        this.target = target;
        this.targetClass = target.getClass();
        this.targetClassName = targetClass.getName();
        this.executorSupplier = executorSupplier;
    }

    @SuppressWarnings("unchecked")
    public static <T> T createProxy(Class<T> interfaceClass, Object target, Supplier<DBOSExecutor> executorSupplier) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalArgumentException("%s is not an interface".formatted(interfaceClass.getName()));
        }

        var handler = new DBOSInvocationHandler(target, executorSupplier);
        return (T)Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class<?>[]{ interfaceClass}, handler);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        var targetMethod = targetClass.getMethod(method.getName(), method.getParameterTypes());

        Workflow wfAnnotation = targetMethod.getAnnotation(Workflow.class);
        if (wfAnnotation != null) {
            return invokeWorkflow(targetMethod, args, wfAnnotation);
        }

        Step stepAnnotation = targetMethod.getAnnotation(Step.class);
        if (stepAnnotation != null) {
            return invokeStep(targetMethod, args, stepAnnotation);
        }

        return method.invoke(target, args);
    }

    Object invokeWorkflow(Method method, Object[] args, Workflow workflow) throws Throwable {
        logger.debug("invokeWorkflow {}.{}", targetClassName, method.getName());

        var executor = Objects.requireNonNull(executorSupplier.get(), "executor supplier returned null");

        var name = workflow.name();
        name = name.isEmpty() ? method.getName() : name;

        return executor.invokeWorkflow(name, targetClassName, args, workflow.maxRecoveryAttempts());
    }

    Object invokeStep(Method method, Object[] args, Step step) throws Throwable {
        logger.debug("invokeStep {}.{}", targetClassName, method.getName());

        var executor = Objects.requireNonNull(executorSupplier.get(), "executor supplier returned null");

        var name = step.name();
        name = name.isEmpty() ? method.getName() : name;

        var interval = Duration.ofMillis(Math.round(step.intervalSeconds() * 1000));

        return executor.invokeStep(
            name, 
            targetClassName,
            () -> method.invoke(target, args), 
            step.retriesAllowed(), 
            interval, 
            step.maxAttempts(), 
            step.backOffRate());
    }

}
