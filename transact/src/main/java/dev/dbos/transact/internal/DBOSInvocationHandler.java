package dev.dbos.transact.internal;

import dev.dbos.transact.context.DBOSContextHolder;
import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;

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

    var implMethod = target.getClass().getMethod(method.getName(), method.getParameterTypes());
    implMethod.setAccessible(true);
    var hook = hookHolder.get();

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

  static Object defaultReturn(Method method) {
    var type = method.getReturnType();

    if (type.isPrimitive()) {
      if (type == void.class) return null;
      if (type == boolean.class) return false;
      if (type == byte.class) return (byte) 0;
      if (type == short.class) return (short) 0;
      if (type == int.class) return 0;
      if (type == long.class) return 0L;
      if (type == float.class) return 0f;
      if (type == double.class) return 0d;
      if (type == char.class) return '\0';
    }

    return null;
  }

  protected Object handleWorkflow(
      Method method, Object[] args, Workflow workflow, StartWorkflowHook hook) throws Exception {
    WorkflowClassName classNameAnnotation =
        target.getClass().getAnnotation(WorkflowClassName.class);
    String className =
        (classNameAnnotation != null && !classNameAnnotation.value().isEmpty())
            ? classNameAnnotation.value()
            : target.getClass().getName();
    var workflowName = workflow.name().isEmpty() ? method.getName() : workflow.name();
    if (hook != null) {
      var invocation = new Invocation(className, instanceName, workflowName, args);
      hook.invoke(invocation);
      return defaultReturn(method);
    }

    var executor = executorSupplier.get();
    if (executor == null) {
      throw new IllegalStateException("executorSupplier returned null");
    }

    var handle = executor.invokeWorkflow(className, instanceName, workflowName, args);

    // This is not really a getResult call - it is part of invocation which will be written
    //  as its own step entry.
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
