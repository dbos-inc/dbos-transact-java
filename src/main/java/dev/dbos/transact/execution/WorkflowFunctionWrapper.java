package dev.dbos.transact.execution;

public class WorkflowFunctionWrapper {
  public final Object target;
  public final String targetClassName;
  public final WorkflowFunction function;

  public WorkflowFunctionWrapper(Object target, String targetClassName, WorkflowFunction function) {
    this.target = target;
    this.targetClassName = targetClassName;
    this.function = function;
  }

  public Object invoke(Object[] args) throws Exception {
    return function.invoke(target, args);
  }
}
