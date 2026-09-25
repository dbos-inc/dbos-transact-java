package dev.dbos.transact.exceptions;

import dev.dbos.transact.execution.RegisteredWorkflow;

import java.util.Objects;

import org.jspecify.annotations.Nullable;

/**
 * {@code DBOSWorkflowFunctionNotFoundException} indicates that invocation of a workflow function
 * was attempted using the registered name of the workflow function, but the name does not exist.
 * DBOSWorkflowFunctionNotFoundException usually indicates a programmer error, such as removing or
 * renaming a workflow function between runs, or using the DBOS client to call a function that does
 * not exist.
 */
public class DBOSWorkflowFunctionNotFoundException extends RuntimeException {
  private final String workflowId;
  private final String workflowName;

  /**
   * @param id the ID of the workflow that could not be run, or {@code null} when the lookup failed
   *     before any workflow was created
   * @param name the workflow function name that does not exist in the registry
   */
  public DBOSWorkflowFunctionNotFoundException(String id, String name) {
    super(
        id == null
            ? String.format("Workflow function %s does not exist.", name)
            : String.format("Workflow function %s does not exist for workflow id %s.", name, id));
    this.workflowName = name;
    this.workflowId = id;
  }

  /**
   * @param id the ID of the workflow that could not be run, or {@code null} when the lookup failed
   *     before any workflow was created
   * @param workflowName the name of the workflow function that is not registered
   * @param className the class it was looked up on
   * @param instanceName the instance it was looked up on
   */
  public DBOSWorkflowFunctionNotFoundException(
      @Nullable String id,
      @Nullable String workflowName,
      @Nullable String className,
      @Nullable String instanceName) {
    this(
        id,
        RegisteredWorkflow.fullyQualifiedName(
            Objects.requireNonNullElse(workflowName, ""),
            Objects.requireNonNullElse(className, ""),
            Objects.requireNonNullElse(instanceName, "")));
  }

  /** The workflow function name that does not exist in the registry */
  public String workflowName() {
    return workflowName;
  }

  /**
   * The ID of the workflow attempted with an unregistered function name, or {@code null} when the
   * lookup failed before any workflow was created
   */
  public String workflowId() {
    return workflowId;
  }
}
