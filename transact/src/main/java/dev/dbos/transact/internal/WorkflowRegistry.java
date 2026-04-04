package dev.dbos.transact.internal;

import dev.dbos.transact.DBOS;
import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.execution.RegisteredWorkflowInstance;
import dev.dbos.transact.execution.SchedulerService;
import dev.dbos.transact.workflow.Workflow;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.jetbrains.annotations.Nullable;
import org.jspecify.annotations.NonNull;

public class WorkflowRegistry {
  private final ConcurrentHashMap<String, RegisteredWorkflowInstance> wfInstRegistry =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, RegisteredWorkflow> wfRegistry =
      new ConcurrentHashMap<>();

  public void registerInstance(@Nullable String instanceName, @NonNull Object target) {
    instanceName = Objects.requireNonNullElse(instanceName, "");
    var className = DBOS.getWorkflowClassName(target);
    var fqName = RegisteredWorkflowInstance.fullyQualifiedInstName(className, instanceName);
    var regClass = new RegisteredWorkflowInstance(className, instanceName, target);
    var previous = wfInstRegistry.putIfAbsent(fqName, regClass);
    if (previous != null) {
      throw new IllegalStateException("Workflow class already registered with name: " + fqName);
    }
  }

  public void registerWorkflow(
      @NonNull Workflow wfTag,
      @NonNull Object target,
      @NonNull Method method,
      @Nullable String instanceName) {

    var workflowName = DBOS.getWorkflowName(wfTag, method);
    var className = DBOS.getWorkflowClassName(target);
    var fqName = RegisteredWorkflow.fullyQualifiedName(workflowName, className, instanceName);
    var regWorkflow =
        new RegisteredWorkflow(
            workflowName,
            className,
            instanceName,
            target,
            method,
            wfTag.maxRecoveryAttempts(),
            wfTag.serializationStrategy());
    SchedulerService.validateAnnotatedWorkflowSchedule(regWorkflow);

    var previous = wfRegistry.putIfAbsent(fqName, regWorkflow);
    if (previous != null) {
      throw new IllegalStateException("Workflow already registered with name: " + fqName);
    }
  }

  public Map<String, RegisteredWorkflow> getWorkflowSnapshot() {
    return Map.copyOf(wfRegistry);
  }

  public Map<String, RegisteredWorkflowInstance> getInstanceSnapshot() {
    return Map.copyOf(wfInstRegistry);
  }
}
