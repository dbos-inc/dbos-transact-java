package dev.dbos.transact.internal;

import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.execution.WorkflowFunctionReflect;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WorkflowRegistry {
  private final ConcurrentHashMap<String, RegisteredWorkflow> registry = new ConcurrentHashMap<>();

  public static String getFullyQualifiedWFName(String cls, String wf) {
    return String.format("%s/%s", cls, wf);
  }

  public void register(
      String className,
      String workflowName,
      Object target,
      Method method,
      int maxRecoveryAttempts) {

    WorkflowFunctionReflect function = (t, args) -> method.invoke(t, args);
    var fqName = getFullyQualifiedWFName(className, workflowName);
    var previous =
        registry.putIfAbsent(
            fqName, new RegisteredWorkflow(workflowName, target, function, maxRecoveryAttempts));

    if (previous != null) {
      throw new IllegalStateException("Workflow already registered with name: " + fqName);
    }
  }

  public void clear() {
    registry.clear();
  }

  public Map<String, RegisteredWorkflow> getSnapshot() {
    return Map.copyOf(registry);
  }
}
