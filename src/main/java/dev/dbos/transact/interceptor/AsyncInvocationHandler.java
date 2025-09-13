package dev.dbos.transact.interceptor;

import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.execution.WorkflowFunctionWrapper;

import java.lang.reflect.Proxy;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AsyncInvocationHandler extends BaseInvocationHandler {

  private static final Logger logger = LoggerFactory.getLogger(AsyncInvocationHandler.class);

  @SuppressWarnings("unchecked")
  public static <T> T createProxy(
      Class<T> interfaceClass, Object implementation, Supplier<DBOSExecutor> executor) {
    if (!interfaceClass.isInterface()) {
      throw new IllegalArgumentException("interfaceClass must be an interface");
    }

    return (T)
        Proxy.newProxyInstance(
            interfaceClass.getClassLoader(),
            new Class<?>[] {interfaceClass},
            new AsyncInvocationHandler(implementation, executor));
  }

  public AsyncInvocationHandler(Object target, Supplier<DBOSExecutor> executor) {
    super(target, executor);
  }

  protected Object submitWorkflow(
      String workflowName, String targetClassName, WorkflowFunctionWrapper wrapper, Object[] args)
      throws Throwable {
    logger.debug("submitWorkflow");

    var executor = executorSupplier.get();
    if (executor == null) {
      throw new IllegalStateException();
    }

    executor.submitWorkflow(workflowName, targetClassName, wrapper.target, args, wrapper.function);

    return null;
  }
}
