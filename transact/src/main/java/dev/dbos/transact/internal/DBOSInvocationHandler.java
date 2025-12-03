package dev.dbos.transact.internal;

import dev.dbos.transact.context.DBOSContextHolder;
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

  public static final ThreadLocal<StartWorkflowHook> hookHolder = new ThreadLocal<>();
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

  public Object invoke(Object proxy, Method method, Object[] args) throws Exception {

    var hook = hookHolder.get();
    var implMethod = target.getClass().getMethod(method.getName(), method.getParameterTypes());

    var wfTag = implMethod.getAnnotation(Workflow.class);
    if (wfTag != null) {
      return handleWorkflow(implMethod, args, wfTag, hook);
    }

    if (hook != null) {
      throw new RuntimeException(
          "Only @Workflow functions may be called from the startWorkflow lambda");
    }

    var stepTag = implMethod.getAnnotation(Step.class);
    if (stepTag != null) {
      return handleStep(implMethod, args, stepTag);
    }

    return method.invoke(target, args);
  }

  protected Object handleWorkflow(
      Method method, Object[] args, Workflow workflow, StartWorkflowHook hook) throws Exception {
    var clsName = target.getClass().getName();
    var wfName = workflow.name().isEmpty() ? method.getName() : workflow.name();
    var invocation = new Invocation(clsName, instanceName, wfName, args);
    if (hook != null) {
      hook.invoke(invocation);
      return null;
    }

    var executor = executorSupplier.get();
    if (executor == null) {
      throw new IllegalStateException("executorSupplier returned null");
    }

    var handle = executor.invokeWorkflow(invocation);
    // This is not really a getResult call - it is part of invocation which will be
    // written
    // as its own step entry.
    var ctx = DBOSContextHolder.get();
    try {
      DBOSContextHolder.clear();
      return handle.getResult();
    } finally {
      DBOSContextHolder.set(ctx);
    }
  }

  protected Object handleStep(Method method, Object[] args, Step step) throws Exception {
    var executor = executorSupplier.get();
    if (executor == null) {
      throw new IllegalStateException("executorSupplier returned null");
    }

    var name = step.name().isEmpty() ? method.getName() : step.name();
    logger.debug("Before : Executing step {}", name);
    try {
      Object result =
          executor.runStepInternal(
              name,
              step.retriesAllowed(),
              step.maxAttempts(),
              step.intervalSeconds(),
              step.backOffRate(),
              null,
              () -> method.invoke(target, args));
      logger.debug("After: Step completed successfully");
      return result;
    } catch (Exception e) {
      logger.error("Step failed", e);
      throw e;
    }
  }
}
