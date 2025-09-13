package dev.dbos.transact.internal;

import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.execution.WorkflowFunctionReflect;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WorkflowRegistry {
  private final ConcurrentHashMap<String, RegisteredWorkflow> registry = new ConcurrentHashMap<>();

  public void register(String workflowName, Object target, Method method, int maxRecoveryAttempts) {

    WorkflowFunctionReflect function = (t, args) -> method.invoke(t, args);
    var previous =
        registry.putIfAbsent(
            workflowName, new RegisteredWorkflow(target, function, maxRecoveryAttempts));

    if (previous != null) {
      throw new IllegalStateException("Workflow already registered with name: " + workflowName);
    }
  }

  public void clear() {
    registry.clear();
  }

  public RegisteredWorkflow get(String workflowName) {
    return registry.get(workflowName);
  }

  public Map<String, RegisteredWorkflow> getSnapshot() {
    return Map.copyOf(registry);
  }
}
