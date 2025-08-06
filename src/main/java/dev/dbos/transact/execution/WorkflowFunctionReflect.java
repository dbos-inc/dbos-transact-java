package dev.dbos.transact.execution;

@FunctionalInterface
public interface WorkflowFunctionReflect {
    Object invoke(Object target, Object[] args) throws Exception;
}
