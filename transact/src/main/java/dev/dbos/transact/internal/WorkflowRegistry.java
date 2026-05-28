package dev.dbos.transact.internal;

import dev.dbos.transact.execution.RegisteredWorkflow;
import dev.dbos.transact.execution.RegisteredWorkflowInstance;
import dev.dbos.transact.workflow.SerializationStrategy;
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
  private final ConcurrentHashMap<String, RegisteredWorkflowInstance> internalWfInstRegistry =
      new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, RegisteredWorkflow> internalWfRegistry =
      new ConcurrentHashMap<>();

  public void registerInstance(@Nullable String instanceName, @NonNull Object target) {
    registerInstance(instanceName, target, wfInstRegistry);
  }

  public void registerInternalInstance(@NonNull Object target) {
    registerInstance(null, target, internalWfInstRegistry);
  }

  private static void registerInstance(
      @Nullable String instanceName,
      @NonNull Object target,
      ConcurrentHashMap<String, RegisteredWorkflowInstance> registry) {
    var className = getWorkflowClassName(target);
    var fqName = RegisteredWorkflowInstance.fullyQualifiedInstName(className, instanceName);
    var regClass = new RegisteredWorkflowInstance(className, instanceName, target);
    var previous = registry.putIfAbsent(fqName, regClass);
    if (previous != null) {
      throw new IllegalStateException("Workflow class already registered with name: " + fqName);
    }
  }

  public RegisteredWorkflow registerWorkflow(
      @NonNull String workflowName,
      @NonNull String className,
      @Nullable String instanceName,
      @NonNull Object target,
      @NonNull Method method,
      @Nullable Integer maxRecoveryAttempts,
      @Nullable SerializationStrategy serializationStrategy) {
    return registerWorkflow(
        workflowName,
        className,
        instanceName,
        target,
        method,
        maxRecoveryAttempts,
        serializationStrategy,
        wfRegistry);
  }

  public RegisteredWorkflow registerInternalWorkflow(
      @NonNull String workflowName,
      @NonNull String className,
      @NonNull Object target,
      @NonNull Method method) {
    return registerWorkflow(
        workflowName, className, null, target, method, null, null, internalWfRegistry);
  }

  private static RegisteredWorkflow registerWorkflow(
      @NonNull String workflowName,
      @NonNull String className,
      @Nullable String instanceName,
      @NonNull Object target,
      @NonNull Method method,
      @Nullable Integer maxRecoveryAttempts,
      @Nullable SerializationStrategy serializationStrategy,
      ConcurrentHashMap<String, RegisteredWorkflow> registry) {
    var fqName = RegisteredWorkflow.fullyQualifiedName(workflowName, className, instanceName);

    var regWorkflow =
        new RegisteredWorkflow(
            workflowName,
            className,
            instanceName,
            target,
            method,
            Objects.requireNonNullElse(maxRecoveryAttempts, -1),
            Objects.requireNonNullElse(serializationStrategy, SerializationStrategy.DEFAULT));

    var previous = registry.putIfAbsent(fqName, regWorkflow);
    if (previous != null) {
      throw new IllegalStateException("Workflow already registered with name: " + fqName);
    }
    return regWorkflow;
  }

  public Map<String, RegisteredWorkflow> getWorkflowSnapshot() {
    return Map.copyOf(wfRegistry);
  }

  public Map<String, RegisteredWorkflowInstance> getInstanceSnapshot() {
    return Map.copyOf(wfInstRegistry);
  }

  public Map<String, RegisteredWorkflow> getInternalWorkflowSnapshot() {
    return Map.copyOf(internalWfRegistry);
  }

  public Map<String, RegisteredWorkflowInstance> getInternalInstanceSnapshot() {
    return Map.copyOf(internalWfInstRegistry);
  }
}
