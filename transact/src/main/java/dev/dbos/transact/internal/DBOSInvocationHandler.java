package dev.dbos.transact.internal;

import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBOSInvocationHandler implements InvocationHandler {
  private static final Logger logger = LoggerFactory.getLogger(DBOSInvocationHandler.class);

  private final Object target;
  private final String instanceName;
  protected final Supplier<DBOSExecutor> executorSupplier;

  public DBOSInvocationHandler(
      Object target, String instanceName, Supplier<DBOSExecutor> executorSupplier) {
    this.target = target;
    this.instanceName = instanceName;
    this.executorSupplier = executorSupplier;
  }

  @SuppressWarnings("unchecked")
  public static <T> T createProxy(
      Class<T> interfaceClass,
      Object implementation,
      String instanceName,
      Supplier<DBOSExecutor> executor) {
    if (!interfaceClass.isInterface()) {
      throw new IllegalArgumentException("interfaceClass must be an interface");
    }

    return (T)
        Proxy.newProxyInstance(
            interfaceClass.getClassLoader(),
            new Class<?>[] {interfaceClass},
            new DBOSInvocationHandler(implementation, instanceName, executor));
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Exception {

    var implMethod = target.getClass().getMethod(method.getName(), method.getParameterTypes());
    implMethod.setAccessible(true);

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
    var executor = Objects.requireNonNull(executorSupplier.get(), "executorSupplier returned null");
    WorkflowClassName classNameAnnotation =
        target.getClass().getAnnotation(WorkflowClassName.class);
    String className =
        (classNameAnnotation != null && !classNameAnnotation.value().isEmpty())
            ? classNameAnnotation.value()
            : target.getClass().getName();
    var workflowName = workflow.name().isEmpty() ? method.getName() : workflow.name();

    return executor.dispatchProxiedWorkflow(
        workflowName, className, instanceName, args, method.getReturnType());
  }

  protected Object handleStep(Method method, Object[] args, Step step) throws Exception {
    var executor = Objects.requireNonNull(executorSupplier.get(), "executorSupplier returned null");

    var name = step.name().isEmpty() ? method.getName() : step.name();
    logger.debug("Executing step {}", name);
    return executor.runStepInternal(
        name,
        step.retriesAllowed(),
        step.maxAttempts(),
        step.intervalSeconds(),
        step.backOffRate(),
        null,
        () -> method.invoke(target, args));
  }
}
