package dev.dbos.transact.execution;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

public class WorkflowRegistry {
    private static final ConcurrentHashMap<String, WorkflowFunctionWrapper> registry = new ConcurrentHashMap<>();

    public  void register(String workflowName, Object target, Method method) {
        WorkflowFunction function = (t, args) -> method.invoke(t, args);
        registry.put(workflowName, new WorkflowFunctionWrapper(target, function));
    }

    public WorkflowFunctionWrapper get(String workflowName) {
        return registry.get(workflowName);
    }
}
