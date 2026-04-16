package dev.dbos.transact.internal;

import dev.dbos.transact.execution.DBOSExecutor;
import dev.dbos.transact.workflow.Step;
import dev.dbos.transact.workflow.StepOptions;
import dev.dbos.transact.workflow.Workflow;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;
import java.util.function.Supplier;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

public class DBOSInvocationHandler implements InvocationHandler {
  private final Object target;
  private final String instanceName;
  protected final Supplier<DBOSExecutor> executorSupplier;

  public DBOSInvocationHandler(
      @NonNull Object target,
      @Nullable String instanceName,
      @NonNull Supplier<DBOSExecutor> executorSupplier) {
    this.target = Objects.requireNonNull(target, "target must not be null");
    this.instanceName = instanceName;
    this.executorSupplier =
        Objects.requireNonNull(executorSupplier, "executorSupplier must not be null");
  }

  @SuppressWarnings("unchecked")
  public static <T> T createProxy(
      @NonNull Class<T> interfaceClass,
      @NonNull Object implementation,
      @Nullable String instanceName,
      @NonNull Supplier<DBOSExecutor> executor) {

    if (!Objects.requireNonNull(interfaceClass, "interfaceClass must not be null").isInterface()) {
      throw new IllegalArgumentException("interfaceClass must be an interface");
    }

    return (T)
        Proxy.newProxyInstance(
            interfaceClass.getClassLoader(),
            new Class<?>[] {interfaceClass},
            new DBOSInvocationHandler(implementation, instanceName, executor));
  }

  private DBOSExecutor executor() {
    return Objects.requireNonNull(
        executorSupplier.get(), "executorSupplier returned a null executor");
  }

  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Exception {
    var implMethod = target.getClass().getMethod(method.getName(), method.getParameterTypes());
    implMethod.setAccessible(true);

    var wfTag = implMethod.getAnnotation(Workflow.class);
    if (wfTag != null) {
      return executor().runWorkflow(target, instanceName, implMethod, args, wfTag);
    }

    var stepTag = implMethod.getAnnotation(Step.class);
    if (stepTag != null) {
      var options = StepOptions.create(stepTag, method);
      return executor().runStep(() -> implMethod.invoke(target, args), options, null);
    }

    return method.invoke(target, args);
  }
}
