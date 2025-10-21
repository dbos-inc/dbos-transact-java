package dev.dbos.transact.internal;

import dev.dbos.transact.execution.RegisteredWorkflow;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WorkflowRegistry {
  private final ConcurrentHashMap<String, RegisteredWorkflow> registry = new ConcurrentHashMap<>();

  public void register(
      String className,
      String workflowName,
      Object target,
      String instanceName,
      Method method,
      int maxRecoveryAttempts) {

    var fqName = RegisteredWorkflow.fullyQualifiedWFName(className, instanceName, workflowName);
    var previous =
        registry.putIfAbsent(
            fqName,
            new RegisteredWorkflow(
                workflowName, target, instanceName, method, maxRecoveryAttempts));

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
