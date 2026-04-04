package dev.dbos.transact.execution;

import dev.dbos.transact.workflow.SerializationStrategy;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Objects;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

public record RegisteredWorkflow(
    String workflowName,
    String className,
    String instanceName,
    Object target,
    Method workflowMethod,
    int maxRecoveryAttempts,
    SerializationStrategy serializationStrategy) {

  public RegisteredWorkflow {
    Objects.requireNonNull(workflowName, "workflow name must not be null");
    Objects.requireNonNull(className, "workflow class name must not be null");
    instanceName = Objects.requireNonNullElse(instanceName, "");
    Objects.requireNonNull(target, "workflow target object must not be null");
    Objects.requireNonNull(workflowMethod, "workflow method must not be null");
  }

  public static String fullyQualifiedName(@NonNull String workflowName, @NonNull String className) {
    return fullyQualifiedName(workflowName, className, "");
  }

  public static String fullyQualifiedName(
      @NonNull String workflowName, @NonNull String className, @Nullable String instanceName) {
    return String.format(
        "%s/%s/%s",
        Objects.requireNonNull(workflowName, "workflowName cannot be null"),
        Objects.requireNonNull(className, "className cannot be null"),
        Objects.requireNonNullElse(instanceName, ""));
  }

  public String fullyQualifiedName() {
    return fullyQualifiedName(workflowName, className, instanceName);
  }

  @SuppressWarnings("unchecked")
  public <T, E extends Exception> T invoke(Object[] args) throws E {
    try {
      return (T) workflowMethod.invoke(target, args);
    } catch (Exception e) {
      while (e instanceof InvocationTargetException ite) {
        var targetException = ite.getTargetException();
        if (targetException instanceof Exception exception) {
          e = exception;
        } else {
          // Preserve existing behavior: reflective invocation can surface a non-Exception
          // Throwable (for example, an Error), which cannot be rethrown as E here.
          throw new RuntimeException(targetException);
        }
      }
      throw (E) e;
    }
  }
}
