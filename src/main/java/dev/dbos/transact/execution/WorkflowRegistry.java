package dev.dbos.transact.execution;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

public class WorkflowRegistry {
  private final ConcurrentHashMap<String, WorkflowFunctionWrapper> registry =
      new ConcurrentHashMap<>();

  public void register(String workflowName, Object target, String targetClassName, Method method) {
    if (registry.containsKey(workflowName)) {
      throw new IllegalStateException("Workflow already registered with name: " + workflowName);
    }
    WorkflowFunctionReflect function = (t, args) -> method.invoke(t, args);
    registry.put(workflowName, new WorkflowFunctionWrapper(target, targetClassName, function));
  }

  public WorkflowFunctionWrapper get(String workflowName) {
    return registry.get(workflowName);
  }
}
