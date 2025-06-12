package dev.dbos.transact.interceptor;

import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Transaction;
import dev.dbos.transact.workflow.Workflow;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

public class TransactInvocationHandler implements InvocationHandler {

    private final Object target;

    @SuppressWarnings("unchecked")
    public static <T> T createProxy(Class<T> interfaceClass, T implementation) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalArgumentException("interfaceClass must be an interface");
        }

        T proxy =  (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass },
                new TransactInvocationHandler(implementation)
        );

        return proxy;
    }

    protected TransactInvocationHandler(Object target) {
        this.target = target;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        System.out.println("Interceptor called for method: " + method.getName());

        Method targetMethod = target.getClass().getMethod(method.getName(), method.getParameterTypes());
        System.out.println("Has @Workflow: " +
                targetMethod.isAnnotationPresent(Workflow.class));

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

        System.out.printf("Before: Starting workflow '%s' (timeout: %ds)%n",
                workflow.name().isEmpty() ? method.getName() : workflow.name(),
                workflow.timeout());

        try {
            Object result = method.invoke(target, args);
            System.out.println("After: Workflow completed successfully");
            return result;
        } catch (Exception e) {
            System.out.println("After: Workflow failed: " + e.getCause().getMessage());
            throw e.getCause();
        }
    }

    protected Object handleTransaction(Method method, Object[] args, Transaction transaction) throws Throwable {

        System.out.printf("Before starting transaction %s %s", method.getName(), transaction.name());
        try {
            Object result = method.invoke(target, args);
            System.out.println("After : Transaction committed successfully");
            return result;
        } catch (Exception e) {

            System.out.printf("Transaction attempt: %s%n",
                    e.getCause().getMessage());
            throw e.getCause();

        }

    }

    protected Object handleStep(Method method, Object[] args, Step step) throws Throwable {
        System.out.printf("Before : Executing step %s %s",
                method.getName(), step.name());

        try {
            Object result = method.invoke(target, args);
            System.out.println("After: Step completed successfully");
            return result;
        } catch (Exception e) {
            System.out.println("Step failed: " + e.getCause().getMessage());
            throw e.getCause();
        }
    }
}
