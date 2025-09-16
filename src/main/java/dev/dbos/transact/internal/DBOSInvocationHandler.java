package dev.dbos.transact.internal;

import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOSInvocationHandler implements InvocationHandler {

  private static final Logger logger = LoggerFactory.getLogger(DBOSInvocationHandler.class);

  private final Object target;
  protected final Supplier<DBOSExecutor> executorSupplier;

  public DBOSInvocationHandler(Object target, Supplier<DBOSExecutor> executorSupplier) {
    this.target = target;
    this.executorSupplier = executorSupplier;
  }

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
            new DBOSInvocationHandler(implementation, executor));
  }

  public Object invoke(Object proxy, Method method, Object[] args) throws Exception {

    Method implMethod = target.getClass().getMethod(method.getName(), method.getParameterTypes());

    var wfTag = implMethod.getAnnotation(Workflow.class);
    if (wfTag != null) {
      return handleWorkflow(implMethod, args, wfTag);
    }

    var stepTag = implMethod.getAnnotation(Step.class);
    if (stepTag != null) {
      return handleStep(implMethod, args, stepTag);
    }

    return method.invoke(target, args);
  }

  protected Object handleWorkflow(Method method, Object[] args, Workflow workflow)
      throws Exception {
    var executor = executorSupplier.get();
    if (executor == null) {
      throw new IllegalStateException();
    }

    var handle = executor.invokeWorkflow(workflow.name(), method, args);
    return handle.getResult();
  }

  protected Object handleStep(Method method, Object[] args, Step step) throws Exception {
    var executor = executorSupplier.get();
    if (executor == null) {
      throw new IllegalStateException();
    }

    logger.info("Before : Executing step {} {}", method.getName(), step.name());
    try {
      Object result =
          executor.runStepInternal(
              step.name(),
              step.retriesAllowed(),
              step.maxAttempts(),
              step.intervalSeconds(),
              step.backOffRate(),
              () -> method.invoke(target, args));
      logger.info("After: Step completed successfully");
      return result;
    } catch (Exception e) {
      logger.error("Step failed", e);
      throw e;
    }
  }
}
