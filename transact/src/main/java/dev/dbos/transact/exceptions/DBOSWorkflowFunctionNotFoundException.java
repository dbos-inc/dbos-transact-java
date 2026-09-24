package dev.dbos.transact.exceptions;

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

  public DBOSWorkflowFunctionNotFoundException(String id, String name) {
    super(String.format("Workflow function %s does not exist for workflow id %s.", name, id));
    this.workflowName = name;
    this.workflowId = id;
  }

  /**
   * @param id the ID of the workflow that could not be run
   * @param name the workflow function name that does not exist in the registry
   * @param reason why no registered function can match, appended to the message
   */
  public DBOSWorkflowFunctionNotFoundException(String id, String name, String reason) {
    super(
        String.format(
            "Workflow function %s does not exist for workflow id %s: %s.", name, id, reason));
    this.workflowName = name;
    this.workflowId = id;
  }

  /** The workflow function name that does not exist in the registry */
  public String workflowName() {
    return workflowName;
  }

  /** The ID of the workflow attempted with an unregistered function name */
  public String workflowId() {
    return workflowId;
  }
}
