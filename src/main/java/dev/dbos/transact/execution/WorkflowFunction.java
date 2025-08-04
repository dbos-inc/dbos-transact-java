package dev.dbos.transact.execution;

@FunctionalInterface
public interface WorkflowFunction {
  Object invoke(Object target, Object[] args) throws Exception;
}
