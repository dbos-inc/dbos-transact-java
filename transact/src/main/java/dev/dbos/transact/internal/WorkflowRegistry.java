package dev.dbos.transact.internal;

import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.execution.RegisteredWorkflowInstance;
import dev.dbos.transact.execution.SchedulerService;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WorkflowRegistry {
  private final ConcurrentHashMap<String, RegisteredWorkflowInstance> wfInstRegistry =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, RegisteredWorkflow> wfRegistry =
      new ConcurrentHashMap<>();

  public void register(Class<?> ifc, Object target, String className, String instanceName) {
    var fqName = RegisteredWorkflowInstance.fullyQualifiedInstName(className, instanceName);
    var regClass = new RegisteredWorkflowInstance(className, instanceName, ifc, target);
    var previous = wfInstRegistry.putIfAbsent(fqName, regClass);
    if (previous != null) {
      throw new IllegalStateException("Workflow class already registered with name: " + fqName);
    }
  }

  public void register(
      String className,
      String workflowName,
      Object target,
      String instanceName,
      Method method,
      int maxRecoveryAttempts) {

    var fqName = RegisteredWorkflow.fullyQualifiedName(className, instanceName, workflowName);
    var regWorkflow =
        new RegisteredWorkflow(
            workflowName, className, instanceName, target, method, maxRecoveryAttempts);
    SchedulerService.validateScheduledWorkflow(regWorkflow);

    var previous = wfRegistry.putIfAbsent(fqName, regWorkflow);
    if (previous != null) {
      throw new IllegalStateException("Workflow already registered with name: " + fqName);
    }
  }

  public void clear() {
    wfInstRegistry.clear();
    wfRegistry.clear();
  }

  public Map<String, RegisteredWorkflow> getWorkflowSnapshot() {
    return Map.copyOf(wfRegistry);
  }

  public Map<String, RegisteredWorkflowInstance> getInstanceSnapshot() {
    return Map.copyOf(wfInstRegistry);
  }
}
