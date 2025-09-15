package dev.dbos.transact.execution;

import java.util.Objects;

public record RegisteredWorkflow(
    Object target, String className, WorkflowFunctionReflect function, int maxRecoveryAttempts) {
  public RegisteredWorkflow {
    Objects.requireNonNull(target);
    Objects.requireNonNull(className);
    Objects.requireNonNull(function);
  }

  public RegisteredWorkflow(
      Object target, WorkflowFunctionReflect function, int maxRecoveryAttempts) {
    this(
        target, Objects.requireNonNull(target).getClass().getName(), function, maxRecoveryAttempts);
  }

  @SuppressWarnings("unchecked")
  public <T> T invoke(Object[] args) throws Exception {
    return (T) function.invoke(target, args);
  }
}
