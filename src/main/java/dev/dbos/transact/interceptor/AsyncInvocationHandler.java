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
        
        Method implMethod = target.getClass().getMethod(method.getName(), method.getParameterTypes());

        Workflow wfAnnotation = implMethod.getAnnotation(Workflow.class) ;

        if (wfAnnotation != null) {

            String workflowName = wfAnnotation.name().isEmpty() ? implMethod.getName() : wfAnnotation.name();

            String msg = String.format("Before: Starting workflow '%s' (timeout: %ds)%n",
                    workflowName,
                    wfAnnotation.timeout());

            logger.info(msg);

            dbosExecutor.submitWorkflow(
                    workflowName,
                    targetClassName,
                    method.getName(),
                    args,
                    () -> (Object) method.invoke(target, args)
            );

            return getDefaultValue(method.getReturnType()) ; // always return null or default

        } else {
            throw new RuntimeException("workflow annotation expected on target method");
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
