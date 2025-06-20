package dev.dbos.transact.interceptor;

import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.Workflow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class AsyncInvocationHandler implements InvocationHandler {

    private static final Logger logger = LoggerFactory.getLogger(AsyncInvocationHandler.class);

    private final Object target;
    private final String targetClassName ;
    private final DBOSExecutor dbosExecutor ;

    public static <T> T createProxy(Class<T> interfaceClass, Object implementation, DBOSExecutor executor) {
        if (!interfaceClass.isInterface()) {
            throw new IllegalArgumentException("interfaceClass must be an interface");
        }

        T proxy =  (T) Proxy.newProxyInstance(
                interfaceClass.getClassLoader(),
                new Class<?>[] { interfaceClass },
                new AsyncInvocationHandler(implementation, executor)) ;


        return proxy;
    }


    public AsyncInvocationHandler(Object target, DBOSExecutor dbosExecutor) {
        this.target = target;
        this.targetClassName = target.getClass().getName();
        this.dbosExecutor = dbosExecutor ;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {

        Method targetMethod = findMethodWithSameSignature(target.getClass(), method);

        Workflow wfAnnotation = targetMethod.getAnnotation(Workflow.class) ;

        if (wfAnnotation != null) {

            String workflowName = wfAnnotation.name().isEmpty() ? targetMethod.getName() : wfAnnotation.name();

            String msg = String.format("Before: Starting workflow '%s' (timeout: %ds)%n",
                    workflowName,
                    wfAnnotation.timeout());

            logger.info(msg);

            return dbosExecutor.submitWorkflow(
                    workflowName,
                    targetClassName,
                    method.getName(),
                    args,
                    () -> (Object) targetMethod.invoke(target, args)
            );


        } else {
            throw new RuntimeException("workflow annotation expected on target method");
        }

    }

    private Method findMethodWithSameSignature(Class<?> clazz, Method interfaceMethod) throws NoSuchMethodException {
        for (Method m : clazz.getMethods()) {
            if (m.getName().equals(interfaceMethod.getName()) &&
                    Arrays.equals(m.getParameterTypes(), interfaceMethod.getParameterTypes())) {
                return m;
            }
        }
        throw new NoSuchMethodException("Matching method not found");
    }
}
