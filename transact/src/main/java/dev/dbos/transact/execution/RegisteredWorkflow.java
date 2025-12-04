package dev.dbos.transact.execution;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

public record RegisteredWorkflow(
    String name,
    String className,
    String instanceName,
    Object target,
    Method workflowMethod,
    int maxRecoveryAttempts) {

  public RegisteredWorkflow {
    Objects.requireNonNull(name, "workflow name must not be null");
    Objects.requireNonNull(className, "workflow class name must not be null");
    instanceName = Objects.requireNonNullElse(instanceName, "");
    Objects.requireNonNull(target, "workflow target object must not be null");
    Objects.requireNonNull(workflowMethod, "workflow method must not be null");
  }

  public RegisteredWorkflow(
      String name,
      Object target,
      String instanceName,
      Method workflowMethod,
      int maxRecoveryAttempts) {
    this(
        name,
        Objects.requireNonNull(target, "workflow target object must not be null")
            .getClass()
            .getName(),
        instanceName,
        target,
        workflowMethod,
        maxRecoveryAttempts);
  }

  public static String fullyQualifiedName(
      String className, String instanceName, String workflowName) {
    return String.format("%s/%s/%s", className, instanceName, workflowName);
  }

  public String fullyQualifiedName() {
    return fullyQualifiedName(className, instanceName, name);
  }

  @SuppressWarnings("unchecked")
  public <T, E extends Exception> T invoke(Object[] args) throws E {
    try {
      return (T) workflowMethod.invoke(target, args);
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
