package dev.dbos.transact.execution;

import java.util.Objects;
import java.util.OptionalInt;

public class RegisteredWorkflow {
    public final Object target;
    public final String targetClassName;
    public final WorkflowFunctionReflect function;
    public final OptionalInt maxRecoveryAttempts;

    public RegisteredWorkflow(Object target, WorkflowFunctionReflect function, int maxRecoveryAttempts) {
        this(target, function, maxRecoveryAttempts > 0 ? OptionalInt.of(maxRecoveryAttempts) : OptionalInt.empty());
    }

    public RegisteredWorkflow(Object target, WorkflowFunctionReflect function, OptionalInt maxRecoveryAttempts) {
        this.target = Objects.requireNonNull(target);
        this.targetClassName = this.target.getClass().getName();
        this.function = Objects.requireNonNull(function);
        this.maxRecoveryAttempts = maxRecoveryAttempts;
    }

    @SuppressWarnings("unchecked")
    public <T> T invoke(Object[] args) throws Exception {
        return (T) function.invoke(target, args);
    }
}
