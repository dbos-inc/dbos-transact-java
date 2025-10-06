package dev.dbos.transact.execution;

import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

public record RegisteredWorkflow(
    String name,
    String className,
    String instanceName,
    Object target,
    WorkflowFunctionReflect function,
    int maxRecoveryAttempts) {

  public RegisteredWorkflow {
    Objects.requireNonNull(name);
    Objects.requireNonNull(target);
    Objects.requireNonNull(className);
    Objects.requireNonNull(instanceName);
    Objects.requireNonNull(function);
  }

  public RegisteredWorkflow(
      String name,
      Object target,
      String instanceName,
      WorkflowFunctionReflect function,
      int maxRecoveryAttempts) {
    this(
        name,
        Objects.requireNonNull(target).getClass().getName(),
        instanceName,
        target,
        function,
        maxRecoveryAttempts);
  }

  public <T, E extends Exception> T invoke(Object[] args) throws E {
    try {
      return (T) function.invoke(target, args);
    } catch (Exception e) {
      while (e instanceof InvocationTargetException) {
        var ite = (InvocationTargetException) e;
        var target = ite.getTargetException();
        if (target instanceof Exception) {
          e = (Exception) target;
        } else {
          throw new RuntimeException(e);
        }
      }
      throw (E) e;
    }
  }
}
