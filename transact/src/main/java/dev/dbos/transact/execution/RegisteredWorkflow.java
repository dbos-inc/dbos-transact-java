package dev.dbos.transact.execution;

import dev.dbos.transact.workflow.SerializationStrategy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

import org.jspecify.annotations.NonNull;

public record RegisteredWorkflow(
    String name,
    String className,
    String instanceName,
    Object target,
    Method workflowMethod,
    int maxRecoveryAttempts,
    SerializationStrategy serializationStrategy) {

  public RegisteredWorkflow {
    Objects.requireNonNull(name, "workflow name must not be null");
    Objects.requireNonNull(className, "workflow class name must not be null");
    instanceName = Objects.requireNonNullElse(instanceName, "");
    Objects.requireNonNull(target, "workflow target object must not be null");
    Objects.requireNonNull(workflowMethod, "workflow method must not be null");
  }

  public static String fullyQualifiedName(@NonNull String className, @NonNull String workflowName) {
    return fullyQualifiedName(className, "", workflowName);
  }

  public static String fullyQualifiedName(
      @NonNull String className, @NonNull String instanceName, @NonNull String workflowName) {
    return String.format(
        "%s/%s/%s",
        Objects.requireNonNull(className, "className cannot be null"),
        Objects.requireNonNull(instanceName, "instanceName cannot be null"),
        Objects.requireNonNull(workflowName, "workflowName cannot be null"));
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
