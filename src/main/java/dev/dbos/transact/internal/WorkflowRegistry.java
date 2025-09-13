package dev.dbos.transact.internal;

import dev.dbos.transact.execution.WorkflowFunctionReflect;
import dev.dbos.transact.execution.WorkflowFunctionWrapper;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WorkflowRegistry {
  private final ConcurrentHashMap<String, WorkflowFunctionWrapper> registry =
      new ConcurrentHashMap<>();

  public void register(String workflowName, Object target, String targetClassName, Method method) {

    WorkflowFunctionReflect function = (t, args) -> method.invoke(t, args);
    var previous =
        registry.putIfAbsent(
            workflowName, new WorkflowFunctionWrapper(target, targetClassName, function));

    if (previous != null) {
      throw new IllegalStateException("Workflow already registered with name: " + workflowName);
    }
  }

  public void clear() {
    registry.clear();
  }

  public WorkflowFunctionWrapper get(String workflowName) {
    return registry.get(workflowName);
  }

  public Map<String, WorkflowFunctionWrapper> getSnapshot() {
    return Map.copyOf(registry);
  }
}
