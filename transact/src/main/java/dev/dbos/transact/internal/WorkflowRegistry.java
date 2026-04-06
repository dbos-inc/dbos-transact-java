package dev.dbos.transact.internal;

import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.execution.RegisteredWorkflowInstance;
import dev.dbos.transact.execution.SchedulerService;
import dev.dbos.transact.workflow.Workflow;
import dev.dbos.transact.workflow.WorkflowClassName;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

public class WorkflowRegistry {

  public static @NonNull String getWorkflowClassName(@NonNull Object target) {
    var klass = Objects.requireNonNull(target, "target can not be null").getClass();
    var wfClassTag = klass.getAnnotation(WorkflowClassName.class);
    return (wfClassTag == null || wfClassTag.value().isEmpty())
        ? klass.getName()
        : wfClassTag.value();
  }

  public static @NonNull String getWorkflowName(@NonNull Workflow wfTag, @NonNull Method method) {
    return Objects.requireNonNull(wfTag, "wfTag can not be null").name().isEmpty()
        ? Objects.requireNonNull(method, "method can not be null").getName()
        : wfTag.name();
  }

  private final ConcurrentHashMap<String, RegisteredWorkflowInstance> wfInstRegistry =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, RegisteredWorkflow> wfRegistry =
      new ConcurrentHashMap<>();

  public void registerInstance(@Nullable String instanceName, @NonNull Object target) {
    var className = getWorkflowClassName(target);
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

    var workflowName = getWorkflowName(wfTag, method);
    var className = getWorkflowClassName(target);
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
