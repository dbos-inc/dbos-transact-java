package dev.dbos.transact.execution;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class WorkflowFunctionKey {
    private final Class<?> clazz;
    private final String methodName;
    private final List<Class<?>> parameterTypes;

    public WorkflowFunctionKey(Class<?> clazz, Method method) {
        this.clazz = clazz;
        this.methodName = method.getName();
        this.parameterTypes = Arrays.asList(method.getParameterTypes());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof WorkflowFunctionKey)) return false;
        WorkflowFunctionKey that = (WorkflowFunctionKey) o;
        return clazz.equals(that.clazz) &&
                methodName.equals(that.methodName) &&
                parameterTypes.equals(that.parameterTypes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clazz, methodName, parameterTypes);
    }

    @Override
    public String toString() {
        return clazz.getName() + "#" + methodName + parameterTypes;
    }
}

