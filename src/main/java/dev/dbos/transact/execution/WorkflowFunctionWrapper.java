package dev.dbos.transact.execution;

public class WorkflowFunctionWrapper {
    public final Object target;
    public final String targetClassName;
    public final WorkflowFunctionReflect function;

    public WorkflowFunctionWrapper(Object target, String targetClassName,
            WorkflowFunctionReflect function) {
        this.target = target;
        this.targetClassName = targetClassName;
        this.function = function;
    }

    @SuppressWarnings("unchecked")
    public <T> ThrowingSupplier<T> getSupplier(Object[] args) {
        return () -> (T)function.invoke(target, args);
    }
}
