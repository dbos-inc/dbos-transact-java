package dev.dbos.transact.execution;

public class WorkflowFunctionWrapper {
    public final Object target;
    public final WorkflowFunction function;

    public WorkflowFunctionWrapper(Object target, WorkflowFunction function) {
        this.target = target;
        this.function = function;
    }

    public Object invoke(Object[] args) throws Exception {
        return function.invoke(target, args);
    }
}